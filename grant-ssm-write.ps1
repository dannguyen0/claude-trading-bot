# One-off: re-apply the Lambda role's inline policy with ssm:PutParameter added.
# Needed so run_refresh_earnings() can write the earnings calendar back to SSM.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File .\grant-ssm-write.ps1

$ErrorActionPreference = "Continue"
$ROLE_NAME       = "trading-bot-lambda-role"
$DASHBOARD_BUCKET = "trading-bot-dashboard-975193805321"

$inlinePolicy = @"
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect":"Allow","Action":["dynamodb:PutItem","dynamodb:Query","dynamodb:Scan","dynamodb:UpdateItem","dynamodb:DeleteItem"],"Resource":"arn:aws:dynamodb:*:*:table/trading-bot-trades"},
    {"Effect":"Allow","Action":["ssm:GetParameter","ssm:PutParameter"],"Resource":"arn:aws:ssm:*:*:parameter/trading-bot/*"},
    {"Effect":"Allow","Action":["s3:PutObject","s3:GetObject"],"Resource":"arn:aws:s3:::$DASHBOARD_BUCKET/*"},
    {"Effect":"Allow","Action":["sns:Publish"],"Resource":"*"}
  ]
}
"@

$policyFile = Join-Path $env:TEMP "trading-bot-policy.json"
[System.IO.File]::WriteAllText($policyFile, $inlinePolicy, (New-Object System.Text.UTF8Encoding $false))

Write-Host "Updating inline policy on role $ROLE_NAME..."
aws iam put-role-policy --role-name $ROLE_NAME --policy-name trading-bot-policy --policy-document "file://$policyFile"
if ($LASTEXITCODE -ne 0) { Write-Host "Policy update failed" -ForegroundColor Red; exit 1 }

Write-Host ""
Write-Host "Done. Wait ~10 seconds for IAM propagation before re-invoking the Lambda." -ForegroundColor Green
