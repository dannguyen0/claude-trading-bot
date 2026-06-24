# EventBridge schedule for Weeklies strategy (naked options).
#
# Entry attempts : every 15 min during market hours (free - no Claude calls)
# Re-scans       : 7:30 AM and 10:00 AM PT (Claude re-scores universe with live data)
# Exit sweeps    : every 10 min during market hours (free)
# Premarket scan : 6:00 AM PT (Claude scores universe pre-open)
#
# All times PT / ET (EDT summer: PT=UTC-7, ET=UTC-4)
#
# DST toggle: set $DST_EDT = $false after November clock change.
# Usage: powershell -ExecutionPolicy Bypass -File .\setup-naked-schedule.ps1

$ErrorActionPreference = "Continue"
$REGION      = "us-east-1"
$BOT_NAME    = "trading-bot"
$LAMBDA_NAME = "$BOT_NAME-runner"
$DST_EDT     = $true   # true = summer (EDT/PDT)

if ($DST_EDT) {
    # PDT = UTC-7 / EDT = UTC-4
    # Market hours: 6:30 AM - 1:00 PM PT = 9:30 AM - 4:00 PM ET = 13:30 - 20:00 UTC
    $PREMARKET_CRON = "cron(0 13 ? * MON-FRI *)"          # 6:00 AM PT / 9:00 AM ET
    $RESCAN1_CRON   = "cron(30 14 ? * MON-FRI *)"         # 7:30 AM PT / 10:30 AM ET
    $RESCAN2_CRON   = "cron(0 17 ? * MON-FRI *)"          # 10:00 AM PT / 1:00 PM ET
    # Every 15 min from 6:30 AM PT (13:30 UTC) through 12:45 PM PT (19:45 UTC)
    $ENTRY_CRON     = "cron(15/15 13-19 ? * MON-FRI *)"   # :15,:30,:45,:00 each hour 13-19 UTC
    # Every 10 min from 6:00 AM PT (13:00 UTC) through 12:50 PM PT (19:50 UTC)
    $EXITSWEEP_CRON = "cron(*/10 13-19 ? * MON-FRI *)"
} else {
    # PST = UTC-8 / EST = UTC-5
    $PREMARKET_CRON = "cron(0 14 ? * MON-FRI *)"
    $RESCAN1_CRON   = "cron(30 15 ? * MON-FRI *)"
    $RESCAN2_CRON   = "cron(0 18 ? * MON-FRI *)"
    $ENTRY_CRON     = "cron(15/15 14-20 ? * MON-FRI *)"
    $EXITSWEEP_CRON = "cron(*/10 14-20 ? * MON-FRI *)"
}

$LAMBDA_ARN = (aws lambda get-function --function-name $LAMBDA_NAME --region $REGION --query "Configuration.FunctionArn" --output text 2>$null).Trim()
if (-not $LAMBDA_ARN) { Write-Host "Cannot read Lambda ARN." -ForegroundColor Red; exit 1 }
Write-Host "Lambda: $LAMBDA_ARN"

$script:FailedSteps = @()

function Upsert-Rule {
    param([string]$Name, [string]$CronExpr, [string]$PayloadJson)
    Write-Host "  $Name  ($CronExpr)"
    aws events put-rule --name $Name --schedule-expression $CronExpr --state ENABLED --region $REGION | Out-Null
    if ($LASTEXITCODE -ne 0) { $script:FailedSteps += "put-rule($Name)"; return }
    $stmtId = "${Name}-invoke"
    aws lambda remove-permission --function-name $LAMBDA_NAME --statement-id $stmtId --region $REGION 2>$null | Out-Null
    $RULE_ARN = (aws events describe-rule --name $Name --region $REGION --query Arn --output text 2>$null).Trim()
    aws lambda add-permission --function-name $LAMBDA_NAME --statement-id $stmtId --action "lambda:InvokeFunction" --principal "events.amazonaws.com" --source-arn $RULE_ARN --region $REGION | Out-Null
    $payloadEscaped = $PayloadJson -replace '"', '\"'
    $targetsInline  = '[{"Id":"1","Arn":"' + $LAMBDA_ARN + '","Input":"' + $payloadEscaped + '"}]'
    $targetPath = Join-Path $env:TEMP "eb-target-$Name.json"
    [System.IO.File]::WriteAllText($targetPath, $targetsInline, (New-Object System.Text.UTF8Encoding $false))
    aws events put-targets --rule $Name --targets "file://$targetPath" --region $REGION | Out-Null
    if ($LASTEXITCODE -ne 0) { $script:FailedSteps += "put-targets($Name)" }
}

function Delete-Rule {
    param([string]$Name)
    $exists = aws events describe-rule --name $Name --region $REGION --query "Name" --output text 2>$null
    if ($exists -and $exists -ne "None") {
        Write-Host "  Removing old rule: $Name"
        aws events remove-targets --rule $Name --ids "1" --region $REGION | Out-Null
        aws lambda remove-permission --function-name $LAMBDA_NAME --statement-id "${Name}-invoke" --region $REGION 2>$null | Out-Null
        aws events delete-rule --name $Name --region $REGION | Out-Null
    }
}

# Remove old individual entry rules replaced by the single entry sweep
Write-Host ""
Write-Host "Removing old individual entry rules..."
Delete-Rule "$BOT_NAME-naked-entry-open"
Delete-Rule "$BOT_NAME-naked-entry-1"
Delete-Rule "$BOT_NAME-naked-entry-2"
Delete-Rule "$BOT_NAME-naked-entry-3"
Delete-Rule "$BOT_NAME-naked-entry-4"
Delete-Rule "$BOT_NAME-naked-entry-5"
Delete-Rule "$BOT_NAME-naked-entry-6"
Delete-Rule "$BOT_NAME-naked-entry-7"
Delete-Rule "$BOT_NAME-naked-entry-8"
Delete-Rule "$BOT_NAME-naked-preclose"

Write-Host ""
Write-Host "Creating new rules..."
Upsert-Rule "$BOT_NAME-naked-premarket"   $PREMARKET_CRON  '{"strategy":"naked","mode":"premarket"}'
Upsert-Rule "$BOT_NAME-naked-rescan-1"    $RESCAN1_CRON    '{"strategy":"naked","mode":"premarket"}'
Upsert-Rule "$BOT_NAME-naked-rescan-2"    $RESCAN2_CRON    '{"strategy":"naked","mode":"premarket"}'
Upsert-Rule "$BOT_NAME-naked-entry"       $ENTRY_CRON      '{"strategy":"naked","mode":"entry"}'
Upsert-Rule "$BOT_NAME-naked-exit-sweep"  $EXITSWEEP_CRON  '{"strategy":"naked","mode":"exit_sweep"}'

Write-Host ""
if ($script:FailedSteps.Count -eq 0) {
    Write-Host "==============================================" -ForegroundColor Green
    Write-Host "Weeklies schedule live." -ForegroundColor Green
    Write-Host ""
    Write-Host "  PT TIME        ET TIME       WHAT" -ForegroundColor Cyan
    Write-Host "  6:00 AM        9:00 AM       Premarket scan (Claude)"
    Write-Host "  6:30 AM        9:30 AM  \    "
    Write-Host "  6:45 AM        9:45 AM   |   Entry attempts every 15 min"
    Write-Host "  7:00 AM       10:00 AM   |   (~26 windows per day, free)"
    Write-Host "  7:15 AM       10:15 AM   |"
    Write-Host "  7:30 AM       10:30 AM  /    RE-SCAN #1 (Claude re-scores)"
    Write-Host "  7:45 AM       10:45 AM  \"
    Write-Host "  ... every 15 min ..."
    Write-Host " 10:00 AM        1:00 PM       RE-SCAN #2 (Claude re-scores)"
    Write-Host "  ... every 15 min ..."
    Write-Host " 12:45 PM        3:45 PM  /    Last entry attempt"
    Write-Host "  Every 10 min              Exit sweep (stops + profit targets)"
    Write-Host ""
    Write-Host "  Max positions : 4   Risk cap : `$4,000"
    Write-Host "  Claude calls  : ~5/day   Cost : ~`$0.90/month"
    Write-Host "==============================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "DST note: re-run with DST_EDT = `$false after the Nov clock change."
} else {
    Write-Host "==============================================" -ForegroundColor Red
    $script:FailedSteps | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    Write-Host "==============================================" -ForegroundColor Red
    exit 1
}
