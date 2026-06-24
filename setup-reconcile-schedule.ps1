# EventBridge rule for the daily DDB <-> Alpaca reconciliation.
# Fires once per weekday at 8:00 ET (pre-market, before any sweeps run).
# Iterates all three strategies, archives any DDB open spread that Alpaca
# doesn't see as a live position.
#
# Usage: powershell -ExecutionPolicy Bypass -File .\setup-reconcile-schedule.ps1

$ErrorActionPreference = "Continue"
$REGION      = "us-east-1"
$BOT_NAME    = "trading-bot"
$LAMBDA_NAME = "$BOT_NAME-runner"
$RULE_NAME   = "$BOT_NAME-reconcile"

# Set to $false after the Nov DST flip each year
$DST_EDT = $true
$RECONCILE_CRON = if ($DST_EDT) {
    "cron(0 12 ? * MON-FRI *)"   # 08:00 ET in EDT (UTC-4)
} else {
    "cron(0 13 ? * MON-FRI *)"   # 08:00 ET in EST (UTC-5)
}

$LAMBDA_ARN = (aws lambda get-function --function-name $LAMBDA_NAME --region $REGION --query "Configuration.FunctionArn" --output text 2>$null).Trim()
if (-not $LAMBDA_ARN) { Write-Host "Cannot read Lambda ARN. Is $LAMBDA_NAME deployed?" -ForegroundColor Red; exit 1 }

Write-Host "Creating rule $RULE_NAME ($RECONCILE_CRON)"
aws events put-rule --name $RULE_NAME --schedule-expression $RECONCILE_CRON --state ENABLED --region $REGION | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Host "put-rule failed" -ForegroundColor Red; exit 1 }

# Lambda invoke permission (idempotent)
$stmtId = "${RULE_NAME}-invoke"
aws lambda remove-permission --function-name $LAMBDA_NAME --statement-id $stmtId --region $REGION 2>$null | Out-Null

$RULE_ARN = (aws events describe-rule --name $RULE_NAME --region $REGION --query Arn --output text 2>$null).Trim()
aws lambda add-permission `
    --function-name $LAMBDA_NAME `
    --statement-id  $stmtId `
    --action        "lambda:InvokeFunction" `
    --principal     "events.amazonaws.com" `
    --source-arn    $RULE_ARN `
    --region        $REGION | Out-Null

# Target (mode payload via file:// to dodge PowerShell quote stripping)
$payloadEscaped = '{"mode":"reconcile"}' -replace '"', '\"'
$targetsInline  = '[{"Id":"1","Arn":"' + $LAMBDA_ARN + '","Input":"' + $payloadEscaped + '"}]'
$targetPath = Join-Path $env:TEMP "eb-target-reconcile.json"
[System.IO.File]::WriteAllText($targetPath, $targetsInline, (New-Object System.Text.UTF8Encoding $false))

aws events put-targets --rule $RULE_NAME --targets "file://$targetPath" --region $REGION | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Host "put-targets failed" -ForegroundColor Red; exit 1 }

Write-Host ""
Write-Host "==============================================" -ForegroundColor Green
Write-Host "Reconciliation schedule activated." -ForegroundColor Green
Write-Host "  $RULE_NAME : $RECONCILE_CRON  (08:00 ET / 5:00 AM PT)"
Write-Host "==============================================" -ForegroundColor Green
Write-Host ""
Write-Host "DST note: re-run with `$DST_EDT = `$false after the Nov flip."
