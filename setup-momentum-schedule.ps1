# EventBridge schedule for the momentum (Strategy 2) bot + weekly earnings refresh.
#
# Creates these rules (all targeting trading-bot-runner):
#   trading-bot-mom-premarket : 09:00 ET MON-FRI  payload {"strategy":"momentum","mode":"premarket"}
#   trading-bot-mom-postopen  : 09:45 ET MON-FRI  payload {"strategy":"momentum","mode":"postopen"}
#   trading-bot-mom-midday    : 12:00 ET MON-FRI  payload {"strategy":"momentum","mode":"midday"}
#   trading-bot-mom-preclose  : 15:00 ET MON-FRI  payload {"strategy":"momentum","mode":"preclose"}
#   trading-bot-refresh-earnings : Sunday 00:00 ET payload {"mode":"refresh_earnings"}
#
# The momentum rules run in parallel with the existing credit-spread rules.
# Each Lambda invocation is independent - same code, different payload.
#
# Times below are UTC crons under EDT (Mar-Nov).  When DST flips to EST
# (on or about Nov 1 each year) each UTC hour shifts by +1.  Re-run this
# script with $DST_EDT = $false after the flip.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File .\setup-momentum-schedule.ps1

$ErrorActionPreference = "Continue"

$REGION      = "us-east-1"
$BOT_NAME    = "trading-bot"
$LAMBDA_NAME = "$BOT_NAME-runner"

# Set to $false on or after the Nov DST flip each year.
$DST_EDT = $true

if ($DST_EDT) {
    # EDT = UTC-4
    $REGIME_CRON    = "cron(30 12 ? * MON-FRI *)"  # 08:30 ET (Phase B regime gate)
    $PREMARKET_CRON = "cron(0 13 ? * MON-FRI *)"   # 09:00 ET
    $POSTOPEN_CRON  = "cron(45 13 ? * MON-FRI *)"  # 09:45 ET
    $ORB_CRON       = "cron(30 14 ? * MON-FRI *)"  # 10:30 ET (Phase D ORB scan)
    $MIDDAY_CRON    = "cron(0 16 ? * MON-FRI *)"   # 12:00 ET
    $AFTERNOON_CRON = "cron(30 17 ? * MON-FRI *)"  # 13:30 ET (Phase E afternoon momentum)
    $PRECLOSE_CRON  = "cron(0 19 ? * MON-FRI *)"   # 15:00 ET
    $EXITSWEEP_CRON = "cron(0/15 14-19 ? * MON-FRI *)"   # every 15 min, 10:00-15:45 ET
    $UNIVERSE_CRON  = "cron(0 3 ? * SUN *)"        # Sun 23:00 ET Sat (8 PM PT)
    $EARNINGS_CRON  = "cron(0 4 ? * SUN *)"        # Sun 00:00 ET
} else {
    # EST = UTC-5
    $REGIME_CRON    = "cron(30 13 ? * MON-FRI *)"
    $PREMARKET_CRON = "cron(0 14 ? * MON-FRI *)"
    $POSTOPEN_CRON  = "cron(45 14 ? * MON-FRI *)"
    $ORB_CRON       = "cron(30 15 ? * MON-FRI *)"
    $MIDDAY_CRON    = "cron(0 17 ? * MON-FRI *)"
    $AFTERNOON_CRON = "cron(30 18 ? * MON-FRI *)"
    $PRECLOSE_CRON  = "cron(0 20 ? * MON-FRI *)"
    $EXITSWEEP_CRON = "cron(0/15 15-20 ? * MON-FRI *)"
    $UNIVERSE_CRON  = "cron(0 4 ? * SUN *)"
    $EARNINGS_CRON  = "cron(0 5 ? * SUN *)"
}

$LAMBDA_ARN = (aws lambda get-function --function-name $LAMBDA_NAME --region $REGION --query "Configuration.FunctionArn" --output text 2>$null).Trim()
if (-not $LAMBDA_ARN) { Write-Host "Cannot read Lambda ARN. Is $LAMBDA_NAME deployed?" -ForegroundColor Red; exit 1 }
Write-Host "Lambda: $LAMBDA_ARN"

$script:FailedSteps = @()

function Upsert-Rule {
    param(
        [string]$Name,
        [string]$CronExpr,
        [string]$PayloadJson   # e.g. '{"strategy":"momentum","mode":"premarket"}'
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

    # Build Targets JSON by hand (PS 5.1 ConvertTo-Json unwraps single-element arrays)
    $payloadEscaped = $PayloadJson -replace '"', '\"'
    $targetsInline  = '[{"Id":"1","Arn":"' + $LAMBDA_ARN + '","Input":"' + $payloadEscaped + '"}]'

    $targetPath = Join-Path $env:TEMP "eb-target-$Name.json"
    [System.IO.File]::WriteAllText($targetPath, $targetsInline, (New-Object System.Text.UTF8Encoding $false))

    aws events put-targets --rule $Name --targets "file://$targetPath" --region $REGION | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  put-targets failed for $Name" -ForegroundColor Red
        $script:FailedSteps += "put-targets($Name)"
    }
}

# ---- Create the rules ------------------------------------------------------
Upsert-Rule -Name "$BOT_NAME-mom-regime"      -CronExpr $REGIME_CRON    -PayloadJson '{"mode":"regime_check"}'
Upsert-Rule -Name "$BOT_NAME-mom-premarket"   -CronExpr $PREMARKET_CRON -PayloadJson '{"strategy":"momentum","mode":"premarket"}'
Upsert-Rule -Name "$BOT_NAME-mom-postopen"    -CronExpr $POSTOPEN_CRON  -PayloadJson '{"strategy":"momentum","mode":"postopen"}'
Upsert-Rule -Name "$BOT_NAME-mom-orb"         -CronExpr $ORB_CRON       -PayloadJson '{"strategy":"momentum","mode":"orb"}'
Upsert-Rule -Name "$BOT_NAME-mom-midday"      -CronExpr $MIDDAY_CRON    -PayloadJson '{"strategy":"momentum","mode":"midday"}'
Upsert-Rule -Name "$BOT_NAME-mom-afternoon"   -CronExpr $AFTERNOON_CRON -PayloadJson '{"strategy":"momentum","mode":"afternoon"}'
Upsert-Rule -Name "$BOT_NAME-mom-preclose"    -CronExpr $PRECLOSE_CRON  -PayloadJson '{"strategy":"momentum","mode":"preclose"}'
Upsert-Rule -Name "$BOT_NAME-mom-exit-sweep"  -CronExpr $EXITSWEEP_CRON -PayloadJson '{"strategy":"momentum","mode":"exit_sweep"}'
Upsert-Rule -Name "$BOT_NAME-refresh-universe" -CronExpr $UNIVERSE_CRON -PayloadJson '{"mode":"refresh_universe"}'
Upsert-Rule -Name "$BOT_NAME-refresh-earnings" -CronExpr $EARNINGS_CRON -PayloadJson '{"mode":"refresh_earnings"}'

Write-Host ""
if ($script:FailedSteps.Count -eq 0) {
    Write-Host "==============================================" -ForegroundColor Green
    Write-Host "Momentum schedule + earnings refresh activated." -ForegroundColor Green
    Write-Host "  regime      : $REGIME_CRON   (08:30 ET / 5:30 AM PT)"
    Write-Host "  premarket   : $PREMARKET_CRON  (09:00 ET / 6:00 AM PT)"
    Write-Host "  postopen    : $POSTOPEN_CRON  (09:45 ET / 6:45 AM PT)"
    Write-Host "  orb         : $ORB_CRON   (10:30 ET / 7:30 AM PT)"
    Write-Host "  midday      : $MIDDAY_CRON   (12:00 ET / 9:00 AM PT)"
    Write-Host "  afternoon   : $AFTERNOON_CRON  (13:30 ET / 10:30 AM PT)"
    Write-Host "  preclose    : $PRECLOSE_CRON   (15:00 ET / 12:00 PM PT)"
    Write-Host "  exit_sweep  : $EXITSWEEP_CRON  (every 15 min, 10:00-15:45 ET)"
    Write-Host "  universe    : $UNIVERSE_CRON   (Sat 23:00 ET / Sat 8:00 PM PT)"
    Write-Host "  earnings    : $EARNINGS_CRON   (Sun 00:00 ET / Sat 9:00 PM PT - uses fresh universe)"
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
