# ─────────────────────────────────────────────────────────────────────────────
# Trading Bot — One-Shot AWS Setup (PowerShell / Windows-native)
# Run this ONCE to provision everything on the AWS free tier.
#
# Prerequisites:
#   - AWS CLI v2 installed + configured (`aws configure`)
#   - Python 3.11+ installed and on PATH (pip available)
#
# Usage (from a PowerShell prompt in this folder):
#   powershell -ExecutionPolicy Bypass -File .\setup.ps1
# ─────────────────────────────────────────────────────────────────────────────

# Don't use "Stop" — AWS CLI writes informational messages to stderr on benign
# not-found responses (e.g. DescribeTable when the table doesn't exist yet),
# and "Stop" turns those into terminating RemoteExceptions. We use "Continue"
# and check $LASTEXITCODE explicitly for operations that must succeed.
$ErrorActionPreference = "Continue"

function Assert-LastExit($msg) {
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: $msg (exit code $LASTEXITCODE)" -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

$REGION           = "us-east-1"
$BOT_NAME         = "trading-bot"
$TABLE_NAME       = "trading-bot-trades"
$LAMBDA_NAME      = "trading-bot-runner"
$ROLE_NAME        = "trading-bot-lambda-role"
$SCHEDULE_NAME    = "trading-bot-15min"

$ACCOUNT_ID       = (aws sts get-caller-identity --query Account --output text)
if (-not $ACCOUNT_ID) { throw "Could not read AWS account id. Run 'aws configure' first." }
$DASHBOARD_BUCKET = "$BOT_NAME-dashboard-$ACCOUNT_ID"

Write-Host ""
Write-Host "Setting up AI Trading Bot on AWS Free Tier" -ForegroundColor Cyan
Write-Host "----------------------------------------------"
Write-Host "Region:  $REGION"
Write-Host "Account: $ACCOUNT_ID"
Write-Host "Bot:     $LAMBDA_NAME"
Write-Host "Table:   $TABLE_NAME"
Write-Host ""

# ── 1. Prompt for secrets (skip if already saved) ───────────────────────────
function Test-SsmParam($name) {
    aws ssm get-parameter --region $REGION --name $name --with-decryption --query "Parameter.Value" --output text *> $null
    return ($LASTEXITCODE -eq 0)
}

$haveAlpacaKey    = Test-SsmParam "/trading-bot/alpaca-key"
$haveAlpacaSecret = Test-SsmParam "/trading-bot/alpaca-secret"
$haveAnthropic    = Test-SsmParam "/trading-bot/anthropic-key"

# Always grab Alpaca key/secret (dashboard needs them spliced in even on re-runs)
if ($haveAlpacaKey -and $haveAlpacaSecret -and $haveAnthropic) {
    Write-Host "SSM parameters already present." -ForegroundColor DarkGray
    $reuse = Read-Host "Reuse existing keys? (Y/n)"
    if ($reuse -eq "" -or $reuse -match '^[Yy]') {
        $ALPACA_KEY    = aws ssm get-parameter --region $REGION --name "/trading-bot/alpaca-key"    --with-decryption --query "Parameter.Value" --output text
        $ALPACA_SECRET = aws ssm get-parameter --region $REGION --name "/trading-bot/alpaca-secret" --with-decryption --query "Parameter.Value" --output text
        $ANTHROPIC_KEY = aws ssm get-parameter --region $REGION --name "/trading-bot/anthropic-key" --with-decryption --query "Parameter.Value" --output text
    } else {
        $haveAlpacaKey = $false; $haveAlpacaSecret = $false; $haveAnthropic = $false
    }
}

if (-not ($haveAlpacaKey -and $haveAlpacaSecret -and $haveAnthropic)) {
    Write-Host "Enter your credentials (stored securely in SSM Parameter Store)" -ForegroundColor Yellow
    $ALPACA_KEY     = Read-Host "  Alpaca API Key"
    $ALPACA_SECRET  = Read-Host "  Alpaca API Secret"
    $ANTHROPIC_KEY  = Read-Host "  Anthropic API Key"

    Write-Host ""
    Write-Host "Saving secrets to SSM Parameter Store..."
    aws ssm put-parameter --region $REGION --name "/trading-bot/alpaca-key"    --value "$ALPACA_KEY"    --type SecureString --overwrite | Out-Null
    aws ssm put-parameter --region $REGION --name "/trading-bot/alpaca-secret" --value "$ALPACA_SECRET" --type SecureString --overwrite | Out-Null
    aws ssm put-parameter --region $REGION --name "/trading-bot/anthropic-key" --value "$ANTHROPIC_KEY" --type SecureString --overwrite | Out-Null
    Assert-LastExit "Failed to save SSM parameters"
    Write-Host "   Secrets saved." -ForegroundColor Green
}

# ── 2. DynamoDB table ────────────────────────────────────────────────────────
Write-Host ""
Write-Host "Creating DynamoDB table..."
aws dynamodb describe-table --region $REGION --table-name $TABLE_NAME --no-cli-pager *> $null
if ($LASTEXITCODE -ne 0) {
    aws dynamodb create-table `
        --region $REGION `
        --table-name $TABLE_NAME `
        --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S `
        --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE `
        --billing-mode PAY_PER_REQUEST `
        --no-cli-pager | Out-Null
    Assert-LastExit "Failed to create DynamoDB table"
    Write-Host "   Created." -ForegroundColor Green
} else {
    Write-Host "   Table already exists, skipping." -ForegroundColor DarkGray
}

# ── 3. IAM role for Lambda ───────────────────────────────────────────────────
Write-Host ""
Write-Host "Creating IAM role..."
$TRUST = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
$trustFile = Join-Path $env:TEMP "trading-bot-trust.json"
$TRUST | Out-File -Encoding ascii -FilePath $trustFile

$ROLE_ARN = aws iam create-role --role-name $ROLE_NAME --assume-role-policy-document "file://$trustFile" --query "Role.Arn" --output text 2>$null
if (-not $ROLE_ARN -or $LASTEXITCODE -ne 0) {
    $ROLE_ARN = aws iam get-role --role-name $ROLE_NAME --query "Role.Arn" --output text
    Assert-LastExit "Could not create or fetch IAM role '$ROLE_NAME'"
}

aws iam attach-role-policy --role-name $ROLE_NAME --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole | Out-Null

$inlinePolicy = @"
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect":"Allow","Action":["dynamodb:PutItem","dynamodb:Query","dynamodb:Scan"],"Resource":"arn:aws:dynamodb:*:*:table/trading-bot-trades"},
    {"Effect":"Allow","Action":["ssm:GetParameter","ssm:PutParameter"],"Resource":"arn:aws:ssm:*:*:parameter/trading-bot/*"},
    {"Effect":"Allow","Action":["s3:PutObject","s3:GetObject"],"Resource":"arn:aws:s3:::$DASHBOARD_BUCKET/*"}
  ]
}
"@
$policyFile = Join-Path $env:TEMP "trading-bot-policy.json"
$inlinePolicy | Out-File -Encoding ascii -FilePath $policyFile
aws iam put-role-policy --role-name $ROLE_NAME --policy-name trading-bot-policy --policy-document "file://$policyFile" | Out-Null

Write-Host "   Role ARN: $ROLE_ARN" -ForegroundColor Green
Write-Host "   Waiting 10s for IAM propagation..."
Start-Sleep -Seconds 10

# ── 4. Package and deploy Lambda ─────────────────────────────────────────────
Write-Host ""
Write-Host "Packaging Lambda function..."

$lambdaDir = Join-Path $PSScriptRoot "lambda"
$packageDir = Join-Path $lambdaDir "package"
$zipPath = Join-Path $lambdaDir "bot.zip"

if (Test-Path $packageDir) { Remove-Item -Recurse -Force $packageDir }
if (Test-Path $zipPath)    { Remove-Item -Force $zipPath }

New-Item -ItemType Directory -Path $packageDir | Out-Null

# Install deps directly into the package dir.
# CRITICAL: use Linux manylinux wheels, NOT Windows wheels — Lambda runs on Linux x86_64
# and packages like pydantic_core ship platform-specific C extensions.
& python -m pip install `
    -r (Join-Path $lambdaDir "requirements.txt") `
    --target $packageDir `
    --platform manylinux2014_x86_64 `
    --implementation cp `
    --python-version 3.12 `
    --only-binary=:all: `
    --upgrade `
    --quiet
Assert-LastExit "pip install failed (could not resolve Linux wheels for a dependency)"
Copy-Item (Join-Path $lambdaDir "bot.py") $packageDir

# Zip the contents of the package dir (not the dir itself)
Compress-Archive -Path (Join-Path $packageDir "*") -DestinationPath $zipPath -Force
Remove-Item -Recurse -Force $packageDir

Write-Host "Deploying Lambda..."
aws lambda get-function --region $REGION --function-name $LAMBDA_NAME --no-cli-pager *> $null
if ($LASTEXITCODE -ne 0) {
    aws lambda create-function `
        --region $REGION `
        --function-name $LAMBDA_NAME `
        --runtime python3.12 `
        --role $ROLE_ARN `
        --handler bot.handler `
        --zip-file "fileb://$zipPath" `
        --timeout 120 `
        --memory-size 256 `
        --environment "Variables={DYNAMO_TABLE=$TABLE_NAME}" `
        --no-cli-pager | Out-Null
    Assert-LastExit "Failed to create Lambda function"
} else {
    aws lambda update-function-code --region $REGION --function-name $LAMBDA_NAME --zip-file "fileb://$zipPath" --no-cli-pager | Out-Null
    Assert-LastExit "Failed to update Lambda code"
    # update-function-configuration can race with a just-created function; retry once
    aws lambda update-function-configuration --region $REGION --function-name $LAMBDA_NAME --environment "Variables={DYNAMO_TABLE=$TABLE_NAME}" --no-cli-pager 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Start-Sleep -Seconds 5
        aws lambda update-function-configuration --region $REGION --function-name $LAMBDA_NAME --environment "Variables={DYNAMO_TABLE=$TABLE_NAME}" --no-cli-pager | Out-Null
        Assert-LastExit "Failed to update Lambda configuration"
    }
}
Write-Host "   Lambda deployed." -ForegroundColor Green

# ── 5. Create Function URL (for dashboard) ───────────────────────────────────
Write-Host ""
Write-Host "Creating Lambda Function URL for dashboard..."
$functionUrl = aws lambda get-function-url-config --region $REGION --function-name $LAMBDA_NAME --query "FunctionUrl" --output text 2>$null
if (-not $functionUrl -or $LASTEXITCODE -ne 0) {
    $corsFile = Join-Path $env:TEMP "trading-bot-cors.json"
    '{"AllowOrigins":["*"],"AllowMethods":["GET"],"AllowHeaders":["Content-Type"]}' | Out-File -Encoding ascii -FilePath $corsFile
    $functionUrl = aws lambda create-function-url-config `
        --region $REGION `
        --function-name $LAMBDA_NAME `
        --auth-type NONE `
        --cors "file://$corsFile" `
        --query "FunctionUrl" --output text
    Assert-LastExit "Failed to create Lambda Function URL"
    aws lambda add-permission `
        --region $REGION `
        --function-name $LAMBDA_NAME `
        --statement-id function-url-public `
        --action lambda:InvokeFunctionUrl `
        --principal "*" `
        --function-url-auth-type NONE 2>$null | Out-Null
}
Write-Host "   Function URL: $functionUrl" -ForegroundColor Green

# ── 6. EventBridge schedule (every 15 min, Mon–Fri, 9:30–16:00 ET) ───────────
Write-Host ""
Write-Host "Setting up EventBridge schedules..."
# DST-tolerant schedule: covers 13:00-20:45 UTC, which spans both EDT
# (9:00am-4:45pm EDT) and EST (8:00am-3:45pm EST). market_is_open() in bot.py
# gates against pre-market/after-hours ticks so over-firing is free.
aws events put-rule --region $REGION --name $SCHEDULE_NAME `
    --schedule-expression "cron(0/15 13-20 ? * MON-FRI *)" --state ENABLED --no-cli-pager | Out-Null
aws events put-rule --region $REGION --name "$SCHEDULE_NAME-open" `
    --schedule-expression "cron(30 13 ? * MON-FRI *)" --state ENABLED --no-cli-pager | Out-Null

$LAMBDA_ARN = "arn:aws:lambda:$REGION`:$ACCOUNT_ID`:function:$LAMBDA_NAME"

aws lambda add-permission --function-name $LAMBDA_NAME `
    --statement-id eventbridge-15min --action lambda:InvokeFunction `
    --principal events.amazonaws.com `
    --source-arn "arn:aws:events:$REGION`:$ACCOUNT_ID`:rule/$SCHEDULE_NAME" --no-cli-pager *> $null

aws lambda add-permission --function-name $LAMBDA_NAME `
    --statement-id eventbridge-open --action lambda:InvokeFunction `
    --principal events.amazonaws.com `
    --source-arn "arn:aws:events:$REGION`:$ACCOUNT_ID`:rule/$SCHEDULE_NAME-open" --no-cli-pager *> $null

aws events put-targets --region $REGION --rule $SCHEDULE_NAME --targets "Id=bot,Arn=$LAMBDA_ARN" --no-cli-pager | Out-Null
aws events put-targets --region $REGION --rule "$SCHEDULE_NAME-open" --targets "Id=bot-open,Arn=$LAMBDA_ARN" --no-cli-pager | Out-Null

Write-Host "   Schedules active (every 15 min, Mon-Fri market hours)." -ForegroundColor Green

# ── 7. S3 dashboard bucket ───────────────────────────────────────────────────
Write-Host ""
Write-Host "Creating dashboard S3 bucket..."
aws s3 mb "s3://$DASHBOARD_BUCKET" --region $REGION *> $null

# Disable block public access so the static site can serve content
aws s3api put-public-access-block --bucket $DASHBOARD_BUCKET `
    --public-access-block-configuration "BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=false,RestrictPublicBuckets=false" | Out-Null

aws s3 website "s3://$DASHBOARD_BUCKET" --index-document index.html

$bucketPolicy = @"
{"Version":"2012-10-17","Statement":[{"Sid":"PublicRead","Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::$DASHBOARD_BUCKET/*"}]}
"@
$policyPath = Join-Path $env:TEMP "bucket-policy.json"
$bucketPolicy | Out-File -Encoding ascii -FilePath $policyPath
aws s3api put-bucket-policy --bucket $DASHBOARD_BUCKET --policy "file://$policyPath" | Out-Null

# ── 8. Splice live config into dashboard and upload ──────────────────────────
Write-Host ""
Write-Host "Injecting live config into dashboard/index.html..."
$dashPath = Join-Path $PSScriptRoot "dashboard\index.html"
$dash = Get-Content $dashPath -Raw
$dash = $dash -replace 'YOUR_ALPACA_API_KEY', $ALPACA_KEY
$dash = $dash -replace 'YOUR_ALPACA_SECRET',  $ALPACA_SECRET
$dash = $dash -replace 'YOUR_LAMBDA_FUNCTION_URL', $functionUrl.TrimEnd('/')
$tempDash = Join-Path $env:TEMP "tradebot-index.html"
# Write without BOM so browsers and S3 content-type cooperate cleanly.
try {
    $dash | Out-File -Encoding utf8NoBOM -FilePath $tempDash
} catch {
    [System.IO.File]::WriteAllText($tempDash, $dash, (New-Object System.Text.UTF8Encoding $false))
}
aws s3 cp $tempDash "s3://$DASHBOARD_BUCKET/index.html" --content-type "text/html; charset=utf-8" | Out-Null

$DASHBOARD_URL = "http://$DASHBOARD_BUCKET.s3-website-$REGION.amazonaws.com"

Write-Host "   Dashboard live." -ForegroundColor Green
Write-Host ""
Write-Host "=============================================="
Write-Host "Trading Bot is LIVE on AWS Free Tier" -ForegroundColor Cyan
Write-Host "=============================================="
Write-Host ""
Write-Host "  Dashboard:     $DASHBOARD_URL"
Write-Host "  Function URL:  $functionUrl"
Write-Host "  Lambda:        $LAMBDA_NAME"
Write-Host "  Schedule:      Every 15 min, Mon-Fri 9:30-4pm ET"
Write-Host "  Trades table:  $TABLE_NAME"
Write-Host ""
Write-Host "  Test manually:"
Write-Host "    aws lambda invoke --function-name $LAMBDA_NAME --region $REGION out.json; Get-Content out.json"
Write-Host ""
Write-Host "  Tail logs:"
Write-Host "    aws logs tail /aws/lambda/$LAMBDA_NAME --follow --region $REGION"
Write-Host ""
