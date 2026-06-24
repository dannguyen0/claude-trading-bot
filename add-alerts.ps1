# One-shot SNS email-alert provisioner for trading-bot-runner
# Usage: powershell -ExecutionPolicy Bypass -File .\add-alerts.ps1
#
# After this runs, AWS will send a confirmation email to the address below.
# You MUST click the confirm link in that email before alerts start flowing.

$ErrorActionPreference = "Continue"

$REGION        = "us-east-1"
$BOT_NAME      = "trading-bot"
$LAMBDA_NAME   = "$BOT_NAME-runner"
$ROLE_NAME     = "$BOT_NAME-lambda-role"
$TOPIC_NAME    = "$BOT_NAME-alerts"
$ALERT_EMAIL   = "danmn4@uci.edu"

# ---- 1. Create (or look up existing) SNS topic -----------------------------
Write-Host "Creating SNS topic..."
$TOPIC_ARN = (aws sns create-topic --name $TOPIC_NAME --region $REGION --query TopicArn --output text 2>$null).Trim()
if (-not $TOPIC_ARN) {
    Write-Host "Could not create SNS topic" -ForegroundColor Red
    exit 1
}
Write-Host "  Topic: $TOPIC_ARN"

# ---- 2. Subscribe the email (idempotent-ish) -------------------------------
# If the same address is already subscribed, AWS returns a PendingConfirmation
# or the existing SubscriptionArn - either way harmless.
Write-Host "Subscribing $ALERT_EMAIL ..."
aws sns subscribe `
    --topic-arn $TOPIC_ARN `
    --protocol email `
    --notification-endpoint $ALERT_EMAIL `
    --region $REGION | Out-Null
Write-Host "  Check your inbox for an AWS confirmation email and click the link." -ForegroundColor Yellow

# ---- 3. Grant the Lambda role sns:Publish on this topic --------------------
Write-Host "Granting Lambda role publish permission..."
$policyDoc = @"
{
  "Version":"2012-10-17",
  "Statement":[{"Effect":"Allow","Action":"sns:Publish","Resource":"$TOPIC_ARN"}]
}
"@
$policyPath = Join-Path $env:TEMP "sns-publish-policy.json"
$policyDoc | Out-File -Encoding ascii -FilePath $policyPath

aws iam put-role-policy `
    --role-name $ROLE_NAME `
    --policy-name "$BOT_NAME-sns-publish" `
    --policy-document "file://$policyPath" | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "IAM put-role-policy failed" -ForegroundColor Red
    exit 1
}

# ---- 4. Merge ALERT_TOPIC_ARN into the Lambda env vars ---------------------
# Read existing env vars first so we don't clobber DYNAMO_TABLE.
Write-Host "Updating Lambda env vars..."
$existingJson = aws lambda get-function-configuration --function-name $LAMBDA_NAME --region $REGION --query "Environment.Variables" --output json 2>$null
if (-not $existingJson -or $existingJson -eq "null") { $existingJson = "{}" }
$vars = $existingJson | ConvertFrom-Json
# Convert to hashtable, set/override ALERT_TOPIC_ARN, serialise for the CLI
$newVars = @{}
$vars.PSObject.Properties | ForEach-Object { $newVars[$_.Name] = $_.Value }
$newVars["ALERT_TOPIC_ARN"] = $TOPIC_ARN

# Build the "Variables={KEY=val,KEY=val}" shell argument the aws CLI wants
$kvpairs = ($newVars.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join ","
$envArg  = "Variables={$kvpairs}"

aws lambda update-function-configuration `
    --function-name $LAMBDA_NAME `
    --region $REGION `
    --environment $envArg | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Lambda env var update failed" -ForegroundColor Red
    exit 1
}

# IAM changes can take a few seconds to propagate to Lambda
Start-Sleep -Seconds 3

Write-Host ""
Write-Host "==============================================" -ForegroundColor Green
Write-Host "Email alerts wired up." -ForegroundColor Green
Write-Host "  Topic      : $TOPIC_ARN"
Write-Host "  Subscriber : $ALERT_EMAIL (confirm via email link)"
Write-Host "==============================================" -ForegroundColor Green
