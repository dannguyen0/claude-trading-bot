# setup-qqq-schedule.ps1
# Creates the single every-5-min EventBridge rule for the QQQ strategy.
# Toggle $DST_EDT = $true (summer) / $false (winter) before running.
# Run once to create; re-run after DST change to update the cron expression.

$ErrorActionPreference = "Stop"
$REGION      = "us-east-1"
$LAMBDA_NAME = "trading-bot-runner"
$RULE_NAME   = "trading-bot-qqq-tick"
$DST_EDT     = $true    # true = EDT (UTC-4), false = EST (UTC-5)

# Every 5 min, weekdays.
# EDT: 9:45-15:30 ET = 13:45-19:30 UTC -> start offset 45, hours 13-19
# EST: 9:45-15:30 ET = 14:45-20:30 UTC -> start offset 45, hours 14-20
if ($DST_EDT) {
    $CRON = "cron(45/5 13,14,15,16,17,18,19 ? * MON-FRI *)"
} else {
    $CRON = "cron(45/5 14,15,16,17,18,19,20 ? * MON-FRI *)"
}

$LAMBDA_ARN = "arn:aws:lambda:$REGION`:975193805321:function:$LAMBDA_NAME"
$PAYLOAD    = '{"strategy":"momentum","mode":"qqq_tick"}'

Write-Host "Creating rule: $RULE_NAME"
Write-Host "  Cron: $CRON"
Write-Host "  DST mode: $(if ($DST_EDT) { 'EDT (UTC-4)' } else { 'EST (UTC-5)' })"

# Create/update the rule
aws events put-rule `
    --name $RULE_NAME `
    --schedule-expression $CRON `
    --state ENABLED `
    --region $REGION `
    --no-cli-pager | Out-Null

# Write payload to temp file (avoids PowerShell JSON quoting issues)
$TMP = "$env:TEMP\qqq-target.json"
$TARGETS = '[{"Id":"1","Arn":"' + $LAMBDA_ARN + '","Input":"' + $PAYLOAD.Replace('"','\"') + '"}]'
[System.IO.File]::WriteAllText($TMP, $TARGETS)

aws events put-targets `
    --rule $RULE_NAME `
    --targets "file://$TMP" `
    --region $REGION `
    --no-cli-pager | Out-Null

# Grant EventBridge permission to invoke Lambda
aws lambda add-permission `
    --function-name $LAMBDA_NAME `
    --statement-id "allow-qqq-tick" `
    --action "lambda:InvokeFunction" `
    --principal "events.amazonaws.com" `
    --source-arn "arn:aws:events:$REGION`:975193805321:rule/$RULE_NAME" `
    --region $REGION `
    --no-cli-pager 2>&1 | Out-Null

Write-Host "Done. QQQ tick fires every 5 min weekdays 9:45-15:30 ET." -ForegroundColor Green
