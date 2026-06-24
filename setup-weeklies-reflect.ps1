# setup-weeklies-reflect.ps1
# Creates two reflection rules for the Weeklies self-learning loop:
#   1. Daily EOD (3:45 PM ET weekdays) - reflects on every trading day
#   2. Sunday morning (9:00 AM ET) - weekly deep look
# Toggle $DST_EDT = $true (summer) / $false (winter) before running.

$ErrorActionPreference = "Stop"
$REGION      = "us-east-1"
$LAMBDA_NAME = "trading-bot-runner"
$LAMBDA_ARN  = "arn:aws:lambda:$REGION`:975193805321:function:$LAMBDA_NAME"
$PAYLOAD     = '{"strategy":"naked","mode":"weeklies_reflect"}'
$DST_EDT     = $true    # true = EDT (UTC-4), false = EST (UTC-5)

# Cron times:
# Daily 3:45 PM ET weekdays:  EDT=19:45 UTC, EST=20:45 UTC
# Sunday  9:00 AM ET:          EDT=13:00 UTC, EST=14:00 UTC
if ($DST_EDT) {
    $CRON_EOD  = "cron(45 19 ? * MON-FRI *)"
    $CRON_SUN  = "cron(0 13 ? * SUN *)"
} else {
    $CRON_EOD  = "cron(45 20 ? * MON-FRI *)"
    $CRON_SUN  = "cron(0 14 ? * SUN *)"
}

Write-Host "DST mode: $(if ($DST_EDT) { 'EDT (UTC-4)' } else { 'EST (UTC-5)' })"

# Helper: create/update one rule + target + permission
function Set-ReflectRule {
    param($RuleName, $Cron, $StatementId)

    Write-Host "Creating rule: $RuleName ($Cron)"
    aws events put-rule `
        --name $RuleName `
        --schedule-expression $Cron `
        --state ENABLED `
        --region $REGION `
        --no-cli-pager | Out-Null

    $TMP = "$env:TEMP\$RuleName-target.json"
    $TARGETS = '[{"Id":"1","Arn":"' + $LAMBDA_ARN + '","Input":"' + $PAYLOAD.Replace('"','\"') + '"}]'
    [System.IO.File]::WriteAllText($TMP, $TARGETS)

    aws events put-targets `
        --rule $RuleName `
        --targets "file://$TMP" `
        --region $REGION `
        --no-cli-pager | Out-Null

    aws lambda add-permission `
        --function-name $LAMBDA_NAME `
        --statement-id $StatementId `
        --action "lambda:InvokeFunction" `
        --principal "events.amazonaws.com" `
        --source-arn "arn:aws:events:$REGION`:975193805321:rule/$RuleName" `
        --region $REGION `
        --no-cli-pager 2>&1 | Out-Null
}

Set-ReflectRule -RuleName "trading-bot-weeklies-reflect-eod" -Cron $CRON_EOD -StatementId "allow-weeklies-reflect-eod"
Set-ReflectRule -RuleName "trading-bot-weeklies-reflect-sun" -Cron $CRON_SUN -StatementId "allow-weeklies-reflect-sun"

Write-Host "Done. Reflection fires daily 3:45 PM ET (weekdays) + Sundays 9:00 AM ET." -ForegroundColor Green

# Initialize the SSM strategy config if not already present
Write-Host "Initializing Weeklies strategy config in SSM..."
$CONFIG = '{"version":"v01","delta_min":0.30,"delta_max":0.55,"score_threshold":6.0,"score_high":8.0,"dte_max":28,"dte_min":0,"stop_floor_pct":0.60,"max_positions":2,"debit_high":1000.0,"debit_mid":500.0,"universe_size":30,"ba_max_pct":0.10}'
$CONFIG_TMP = "$env:TEMP\weeklies-config.json"
[System.IO.File]::WriteAllText($CONFIG_TMP, $CONFIG)

aws ssm put-parameter `
    --name "/trading-bot/weeklies-strategy-config" `
    --value "file://$CONFIG_TMP" `
    --type String `
    --overwrite `
    --region $REGION `
    --no-cli-pager | Out-Null

Write-Host "SSM config at /trading-bot/weeklies-strategy-config" -ForegroundColor Green
