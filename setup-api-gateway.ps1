# One-time setup: put an HTTP API Gateway in front of trading-bot-runner.
#
# Why: the Lambda Function URL kept returning Forbidden even with AuthType=NONE
# and a public-access permission attached, which we couldn't pin to an obvious
# cause (account is not in an org, no SCP). API Gateway is a different routing
# service and avoids that block.
#
# What this does:
#   1. Creates an HTTP API named trading-bot-api targeting the Lambda
#   2. Updates it with CORS so the dashboard can fetch from a browser
#   3. Grants API Gateway permission to invoke the Lambda
#   4. Splices the new URL into dashboard/index.html and uploads to S3
#   5. Tests the new URL with Invoke-RestMethod
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File .\setup-api-gateway.ps1

$ErrorActionPreference = "Continue"

$REGION      = "us-east-1"
$BOT_NAME    = "trading-bot"
$LAMBDA_NAME = "$BOT_NAME-runner"
$API_NAME    = "$BOT_NAME-api"

# ---- 1. Resolve Lambda ARN -------------------------------------------------
$LAMBDA_ARN = (aws lambda get-function --function-name $LAMBDA_NAME --region $REGION --query "Configuration.FunctionArn" --output text 2>$null).Trim()
if (-not $LAMBDA_ARN) { Write-Host "Cannot read Lambda ARN. Is $LAMBDA_NAME deployed?" -ForegroundColor Red; exit 1 }
Write-Host "Lambda: $LAMBDA_ARN"

$ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text 2>$null).Trim()
if (-not $ACCOUNT_ID) { Write-Host "Could not read AWS account id" -ForegroundColor Red; exit 1 }

# ---- 2. Delete any pre-existing API with the same name (idempotent) -------
$EXISTING = aws apigatewayv2 get-apis --region $REGION --query "Items[?Name=='$API_NAME'].ApiId" --output text 2>$null
if ($EXISTING) {
    $EXISTING = $EXISTING.Trim()
    if ($EXISTING) {
        Write-Host "Deleting pre-existing API: $EXISTING"
        aws apigatewayv2 delete-api --api-id $EXISTING --region $REGION 2>$null | Out-Null
    }
}

# ---- 3. Create HTTP API with Lambda quick-target --------------------------
Write-Host "Creating HTTP API ($API_NAME)..."
$API_ID = (aws apigatewayv2 create-api `
    --name $API_NAME `
    --protocol-type HTTP `
    --target $LAMBDA_ARN `
    --region $REGION `
    --query "ApiId" --output text).Trim()

if (-not $API_ID) { Write-Host "Failed to create API" -ForegroundColor Red; exit 1 }
Write-Host "API created: $API_ID"

# ---- 4. Apply CORS so the dashboard can fetch from a browser --------------
# Using a JSON file avoids PS shorthand-syntax pitfalls with array values.
$corsJson = '{"AllowOrigins":["*"],"AllowMethods":["GET","OPTIONS"],"AllowHeaders":["content-type"]}'
$corsFile = Join-Path $env:TEMP "apigw-cors.json"
[System.IO.File]::WriteAllText($corsFile, $corsJson, (New-Object System.Text.UTF8Encoding $false))

Write-Host "Applying CORS..."
aws apigatewayv2 update-api `
    --api-id $API_ID `
    --cors-configuration "file://$corsFile" `
    --region $REGION | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Host "  CORS update failed (continuing)" -ForegroundColor Yellow }

# ---- 5. Grant API Gateway permission to invoke the Lambda -----------------
$STMT_ID  = "apigw-invoke"
aws lambda remove-permission --function-name $LAMBDA_NAME --statement-id $STMT_ID --region $REGION 2>$null | Out-Null

$SOURCE_ARN = "arn:aws:execute-api:${REGION}:${ACCOUNT_ID}:${API_ID}/*/*"
Write-Host "Granting API Gateway invoke permission..."
aws lambda add-permission `
    --function-name $LAMBDA_NAME `
    --statement-id  $STMT_ID `
    --action        "lambda:InvokeFunction" `
    --principal     "apigateway.amazonaws.com" `
    --source-arn    $SOURCE_ARN `
    --region        $REGION | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Host "  add-permission failed" -ForegroundColor Red; exit 1 }

$API_URL = "https://$API_ID.execute-api.$REGION.amazonaws.com"
Write-Host "API URL: $API_URL"

# ---- 6. Splice URL into dashboard and upload to S3 -------------------------
$DASHBOARD_BUCKET = "$BOT_NAME-dashboard-$ACCOUNT_ID"
$ALPACA_KEY    = aws ssm get-parameter --name "/$BOT_NAME/alpaca-key"    --with-decryption --region $REGION --query Parameter.Value --output text 2>$null
$ALPACA_SECRET = aws ssm get-parameter --name "/$BOT_NAME/alpaca-secret" --with-decryption --region $REGION --query Parameter.Value --output text 2>$null

if (-not $ALPACA_KEY -or -not $ALPACA_SECRET) {
    Write-Host "Could not read Alpaca keys from SSM. Skipping dashboard splice." -ForegroundColor Yellow
} else {
    Write-Host "Splicing dashboard..."
    $dashPath = Join-Path $PSScriptRoot "dashboard\index.html"
    $dash = Get-Content $dashPath -Raw
    $dash = $dash -replace 'YOUR_ALPACA_API_KEY',      $ALPACA_KEY
    $dash = $dash -replace 'YOUR_ALPACA_SECRET',       $ALPACA_SECRET
    $dash = $dash -replace 'YOUR_LAMBDA_FUNCTION_URL', $API_URL
    $tempDash = Join-Path $env:TEMP "tradebot-index.html"
    [System.IO.File]::WriteAllText($tempDash, $dash, (New-Object System.Text.UTF8Encoding $false))

    Write-Host "Uploading dashboard to S3..."
    aws s3 cp $tempDash "s3://$DASHBOARD_BUCKET/index.html" --content-type "text/html; charset=utf-8" --region $REGION | Out-Null
    if ($LASTEXITCODE -ne 0) { Write-Host "S3 upload failed" -ForegroundColor Red; exit 1 }
}

# ---- 7. Smoke test the new URL --------------------------------------------
Write-Host ""
Write-Host "Smoke test: fetching $API_URL/?action=notes ..."
try {
    $probe = Invoke-RestMethod -Uri "$API_URL/?action=notes" -ErrorAction Stop -TimeoutSec 15
    Write-Host "  Got response (typeof: $($probe.GetType().Name))" -ForegroundColor Green
    if ($probe -is [array] -and $probe.Count -gt 0) {
        Write-Host "  First note timestamp: $($probe[0].timestamp)"
    }
} catch {
    Write-Host "  Probe failed: $($_.Exception.Message)" -ForegroundColor Yellow
    Write-Host "  This can happen for ~30s right after creation while AWS propagates." -ForegroundColor Yellow
    Write-Host "  Try again in a minute: Invoke-RestMethod -Uri '$API_URL/?action=notes'"
}

# ---- 8. Summary ------------------------------------------------------------
$DASHBOARD_URL = "http://$DASHBOARD_BUCKET.s3-website-$REGION.amazonaws.com"
Write-Host ""
Write-Host "==============================================" -ForegroundColor Green
Write-Host "API Gateway setup complete." -ForegroundColor Green
Write-Host "  API URL    : $API_URL"
Write-Host "  Dashboard  : $DASHBOARD_URL"
Write-Host "==============================================" -ForegroundColor Green
