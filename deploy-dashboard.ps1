# Quick dashboard-only redeploy - run after editing dashboard\index.html
# Usage: powershell -ExecutionPolicy Bypass -File .\deploy-dashboard.ps1
#
# This mirrors the splice-and-upload logic in setup.ps1 so we can push
# dashboard changes without rerunning the full provisioner.

$ErrorActionPreference = "Continue"
$REGION           = "us-east-1"
$BOT_NAME         = "trading-bot"
$LAMBDA_NAME      = "$BOT_NAME-runner"

# Resolve account id and bucket name
$ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text 2>$null).Trim()
if (-not $ACCOUNT_ID) { Write-Host "Could not read AWS account id" -ForegroundColor Red; exit 1 }
$DASHBOARD_BUCKET = "$BOT_NAME-dashboard-$ACCOUNT_ID"

# Pull the live Alpaca keys and API Gateway URL from AWS so the dashboard gets
# the same config setup-api-gateway.ps1 originally spliced in.
# (The old Function URL path returned 403 even with public access, so we route
#  through API Gateway now and look up its URL by API name.)
$API_NAME = "$BOT_NAME-api"
Write-Host "Fetching live config from AWS..."
$ALPACA_KEY      = aws ssm get-parameter --name "/$BOT_NAME/alpaca-key"            --with-decryption --region $REGION --query Parameter.Value --output text 2>$null
$ALPACA_SECRET   = aws ssm get-parameter --name "/$BOT_NAME/alpaca-secret"         --with-decryption --region $REGION --query Parameter.Value --output text 2>$null
$ALPACA_KEY_2    = aws ssm get-parameter --name "/$BOT_NAME/strategy2/alpaca-key"    --with-decryption --region $REGION --query Parameter.Value --output text 2>$null
$ALPACA_SECRET_2 = aws ssm get-parameter --name "/$BOT_NAME/strategy2/alpaca-secret" --with-decryption --region $REGION --query Parameter.Value --output text 2>$null
$ALPACA_KEY_3    = aws ssm get-parameter --name "/$BOT_NAME/strategy3/alpaca-key"    --with-decryption --region $REGION --query Parameter.Value --output text 2>$null
$ALPACA_SECRET_3 = aws ssm get-parameter --name "/$BOT_NAME/strategy3/alpaca-secret" --with-decryption --region $REGION --query Parameter.Value --output text 2>$null
$API_ID          = (aws apigatewayv2 get-apis --region $REGION --query "Items[?Name=='$API_NAME'].ApiId" --output text 2>$null)
if ($API_ID) { $API_ID = $API_ID.Trim() }

if (-not $ALPACA_KEY -or -not $ALPACA_SECRET -or -not $ALPACA_KEY_2 -or -not $ALPACA_SECRET_2 -or -not $ALPACA_KEY_3 -or -not $ALPACA_SECRET_3 -or -not $API_ID) {
    Write-Host "Missing live config. Is setup-api-gateway.ps1 deployed and have you stored all 3 strategy keys?" -ForegroundColor Red
    Write-Host "  ALPACA_KEY found:      $([bool]$ALPACA_KEY)"
    Write-Host "  ALPACA_SECRET found:   $([bool]$ALPACA_SECRET)"
    Write-Host "  ALPACA_KEY_2 found:    $([bool]$ALPACA_KEY_2)"
    Write-Host "  ALPACA_SECRET_2 found: $([bool]$ALPACA_SECRET_2)"
    Write-Host "  ALPACA_KEY_3 found:    $([bool]$ALPACA_KEY_3)"
    Write-Host "  ALPACA_SECRET_3 found: $([bool]$ALPACA_SECRET_3)"
    Write-Host "  API_ID found:          '$API_ID'"
    exit 1
}
$API_URL = "https://$API_ID.execute-api.$REGION.amazonaws.com"
Write-Host "  API URL: $API_URL"

Write-Host "Splicing dashboard..."
$dashPath = Join-Path $PSScriptRoot "dashboard\index.html"
$dash = Get-Content $dashPath -Raw
# Order matters: replace suffixed placeholders before unsuffixed, otherwise
# YOUR_ALPACA_API_KEY matches YOUR_ALPACA_API_KEY_2/_3 prefix first and corrupts.
$dash = $dash -replace 'YOUR_ALPACA_API_KEY_3',    $ALPACA_KEY_3
$dash = $dash -replace 'YOUR_ALPACA_SECRET_3',     $ALPACA_SECRET_3
$dash = $dash -replace 'YOUR_ALPACA_API_KEY_2',    $ALPACA_KEY_2
$dash = $dash -replace 'YOUR_ALPACA_SECRET_2',     $ALPACA_SECRET_2
$dash = $dash -replace 'YOUR_ALPACA_API_KEY',      $ALPACA_KEY
$dash = $dash -replace 'YOUR_ALPACA_SECRET',       $ALPACA_SECRET
$dash = $dash -replace 'YOUR_LAMBDA_FUNCTION_URL', $API_URL
$tempDash = Join-Path $env:TEMP "tradebot-index.html"
# Use UTF8NoBOM to avoid mojibake in browsers that don't strip the BOM.
# PowerShell 5.1 doesn't support UTF8NoBOM, so fall back to a BOM-stripping write.
try {
    $dash | Out-File -Encoding utf8NoBOM -FilePath $tempDash
} catch {
    [System.IO.File]::WriteAllText($tempDash, $dash, (New-Object System.Text.UTF8Encoding $false))
}

Write-Host "Uploading to S3..."
aws s3 cp $tempDash "s3://$DASHBOARD_BUCKET/index.html" --content-type "text/html; charset=utf-8" --region $REGION | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Host "S3 upload failed" -ForegroundColor Red; exit 1 }

$DASHBOARD_URL = "http://$DASHBOARD_BUCKET.s3-website-$REGION.amazonaws.com"
Write-Host "Done. Dashboard: $DASHBOARD_URL" -ForegroundColor Green
