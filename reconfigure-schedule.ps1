# Reconfigure EventBridge for the credit-spread bot.
#
# Deletes the old trading-bot-15min rule and creates three new rules:
#   trading-bot-morning : 08:45 ET (5:45 AM PT)  mode=morning
#   trading-bot-midday  : 10:30 ET (7:30 AM PT)  mode=midday
#   trading-bot-eod     : 15:00 ET (12:00 PM PT) mode=eod
#
# Times below are UTC crons under EDT (Mar-Nov).  When DST flips to EST
# (on or about Nov 1 each year) each UTC hour shifts by +1.  Re-run this
# script with $DST_EDT = $false after the flip.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File .\reconfigure-schedule.ps1

$ErrorActionPreference = "Continue"

$REGION      = "us-east-1"
$BOT_NAME    = "trading-bot"
$LAMBDA_NAME = "$BOT_NAME-runner"
$OLD_RULE    = "$BOT_NAME-15min"

# Set to $false on or after the Nov DST flip each year.
$DST_EDT = $true

if ($DST_EDT) {
    # EDT = UTC-4
    $MORNING_CRON = "cron(45 12 ? * MON-FRI *)"   # 08:45 ET
    $MIDDAY_CRON  = "cron(30 14 ? * MON-FRI *)"   # 10:30 ET
    $EOD_CRON     = "cron(0 19 ? * MON-FRI *)"    # 15:00 ET
} else {
    # EST = UTC-5
    $MORNING_CRON = "cron(45 13 ? * MON-FRI *)"
    $MIDDAY_CRON  = "cron(30 15 ? * MON-FRI *)"
    $EOD_CRON     = "cron(0 20 ? * MON-FRI *)"
}

$LAMBDA_ARN = (aws lambda get-function --function-name $LAMBDA_NAME --region $REGION --query "Configuration.FunctionArn" --output text 2>$null).Trim()
if (-not $LAMBDA_ARN) { Write-Host "Cannot read Lambda ARN. Is $LAMBDA_NAME deployed?" -ForegroundColor Red; exit 1 }
Write-Host "Lambda: $LAMBDA_ARN"

# ---- 1. Remove old 15-min rule ---------------------------------------------
Write-Host "Removing old rule: $OLD_RULE"
aws events remove-targets --rule $OLD_RULE --ids "1" --region $REGION 2>$null | Out-Null
aws events delete-rule    --name $OLD_RULE --region $REGION 2>$null | Out-Null

# Remove any Lambda permission that referenced the old rule
aws lambda remove-permission --function-name $LAMBDA_NAME --statement-id "${OLD_RULE}-invoke" --region $REGION 2>$null | Out-Null

# ---- 2. Helper: create/replace a rule ---------------------------------------
# Tracks failures so the summary at the end is honest instead of always "success".
$script:FailedSteps = @()

function Upsert-Rule {
    param(
        [string]$Name,
        [string]$CronExpr,
        [string]$ModeJson   # e.g. '{"mode":"morning"}'
    )
    Write-Host "Creating rule $Name ($CronExpr)"
    aws events put-rule `
        --name $Name `
        --schedule-expression $CronExpr `
        --state ENABLED `
        --region $REGION | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  put-rule failed for $Name" -ForegroundColor Red
        $script:FailedSteps += "put-rule($Name)"
        return
    }

    # Grant the rule permission to invoke the Lambda (idempotent: remove then add)
    $stmtId = "${Name}-invoke"
    aws lambda remove-permission --function-name $LAMBDA_NAME --statement-id $stmtId --region $REGION 2>$null | Out-Null

    $RULE_ARN = (aws events describe-rule --name $Name --region $REGION --query Arn --output text 2>$null).Trim()
    aws lambda add-permission `
        --function-name $LAMBDA_NAME `
        --statement-id  $stmtId `
        --action        "lambda:InvokeFunction" `
        --principal     "events.amazonaws.com" `
        --source-arn    $RULE_ARN `
        --region        $REGION | Out-Null

    # Build the Targets JSON by hand so nothing PowerShell can mangle it
    # (PS 5.1's ConvertTo-Json unwraps single-element arrays when piped, and
    # its default indentation sometimes trips AWS CLI's JSON parser).
    # Input is itself JSON, so we need to escape the inner double quotes.
    $modeEscaped  = $ModeJson -replace '"', '\"'
    $targetsInline = '[{"Id":"1","Arn":"' + $LAMBDA_ARN + '","Input":"' + $modeEscaped + '"}]'

    # Write to a file and use file:// — this avoids any shell-quoting ambiguity.
    $targetPath = Join-Path $env:TEMP "eb-target-$Name.json"
    [System.IO.File]::WriteAllText($targetPath, $targetsInline, (New-Object System.Text.UTF8Encoding $false))

    aws events put-targets --rule $Name --targets "file://$targetPath" --region $REGION | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  put-targets failed for $Name" -ForegroundColor Red
        $script:FailedSteps += "put-targets($Name)"
    }
}

# ---- 3. Create the three mode rules ----------------------------------------
Upsert-Rule -Name "$BOT_NAME-morning" -CronExpr $MORNING_CRON -ModeJson '{"mode":"morning"}'
Upsert-Rule -Name "$BOT_NAME-midday"  -CronExpr $MIDDAY_CRON  -ModeJson '{"mode":"midday"}'
Upsert-Rule -Name "$BOT_NAME-eod"     -CronExpr $EOD_CRON     -ModeJson '{"mode":"eod"}'

Write-Host ""
if ($script:FailedSteps.Count -eq 0) {
    Write-Host "==============================================" -ForegroundColor Green
    Write-Host "EventBridge reconfigured." -ForegroundColor Green
    Write-Host "  morning : $MORNING_CRON  (08:45 ET / 5:45 AM PT)"
    Write-Host "  midday  : $MIDDAY_CRON  (10:30 ET / 7:30 AM PT)"
    Write-Host "  eod     : $EOD_CRON  (15:00 ET / 12:00 PM PT)"
    Write-Host "==============================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "DST note: re-run with `$DST_EDT = `$false after the Nov DST flip each year."
} else {
    Write-Host "==============================================" -ForegroundColor Red
    Write-Host "One or more steps failed:" -ForegroundColor Red
    $script:FailedSteps | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    Write-Host "==============================================" -ForegroundColor Red
    exit 1
}
