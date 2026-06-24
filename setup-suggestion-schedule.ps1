# setup-suggestion-schedule.ps1
# Wires up the Daily Options Suggestion system:
#   - Stores Telegram credentials in SSM
#   - Creates placeholder SSM state params
#   - Creates 4 EventBridge rules:
#       09:45 ET  morning scan     (after open, spreads tight)
#       12:00 ET  P&L threshold check
#       12:30 ET  midday recap     (always-send, shows how play moved)
#       15:00 ET  P&L threshold check
#
# Reuses your existing Conservative Alpaca account for market data reads.
# No new Alpaca account needed. No orders are ever placed.
#
# RUN ONCE after setting up your Telegram bot.
# Re-run at any time to update credentials or recreate rules.

param(
    [string]$TelegramToken  = "",
    [string]$TelegramChatId = ""
)

$Region    = "us-east-1"
$LambdaArn = "arn:aws:lambda:us-east-1:975193805321:function:trading-bot-runner"

# DST flag: set to $false after November daylight-saving-time ends
$DST_EDT = $true

# Cron times in UTC (EDT = UTC-4, EST = UTC-5)
# 09:45 ET -> 13:45 UTC (EDT) / 14:45 UTC (EST)
$MorningHrUTC  = if ($DST_EDT) { "13" } else { "14" }
$MorningMinUTC = "45"

# 12:00 ET -> 16:00 UTC (EDT) / 17:00 UTC (EST)
$MidnoonUTC    = if ($DST_EDT) { "16" } else { "17" }

# 12:30 ET -> 16:30 UTC (EDT) / 17:30 UTC (EST)
$RecapHrUTC    = if ($DST_EDT) { "16" } else { "17" }
$RecapMinUTC   = "30"

# 15:00 ET -> 19:00 UTC (EDT) / 20:00 UTC (EST)
$PrecloseUTC   = if ($DST_EDT) { "19" } else { "20" }

Write-Host "=== Daily Suggestion Setup ===" -ForegroundColor Cyan
Write-Host "Uses existing Conservative Alpaca account for market data (read-only)."
Write-Host ""

# ---- Telegram credentials ---------------------------------------------------
if (-not $TelegramToken) {
    $TelegramToken = Read-Host "Enter Telegram bot token"
}
if (-not $TelegramChatId) {
    $TelegramChatId = Read-Host "Enter Telegram chat ID"
}

Write-Host ""
Write-Host "Storing credentials in SSM..." -ForegroundColor Cyan

aws ssm put-parameter `
    --name "/trading-bot/telegram-bot-token" `
    --value $TelegramToken `
    --type "SecureString" `
    --overwrite `
    --region $Region
Write-Host "  /trading-bot/telegram-bot-token stored"

aws ssm put-parameter `
    --name "/trading-bot/telegram-chat-id" `
    --value $TelegramChatId `
    --type "String" `
    --overwrite `
    --region $Region
Write-Host "  /trading-bot/telegram-chat-id stored"

# ---- Placeholder state params -----------------------------------------------
aws ssm put-parameter `
    --name "/trading-bot/daily-suggestion" `
    --value "{}" `
    --type "String" `
    --overwrite `
    --region $Region
Write-Host "  /trading-bot/daily-suggestion placeholder stored"

aws ssm put-parameter `
    --name "/trading-bot/daily-suggestion-alerts" `
    --value "{}" `
    --type "String" `
    --overwrite `
    --region $Region
Write-Host "  /trading-bot/daily-suggestion-alerts placeholder stored"

Write-Host ""
Write-Host "Creating EventBridge rules..." -ForegroundColor Cyan

# ---- Rule 1: Morning suggestion scan (09:45 ET weekdays) --------------------
# Runs 15 min after open so options spreads are tight and price discovery is real.
aws events put-rule `
    --name "trading-bot-suggestion-morning" `
    --schedule-expression "cron($MorningMinUTC $MorningHrUTC ? * MON-FRI *)" `
    --state ENABLED `
    --region $Region
Write-Host "  Rule: trading-bot-suggestion-morning (09:45 ET weekdays)"

$t1 = '[{"Id":"suggestion-morning","Arn":"' + $LambdaArn + '","Input":"{\"mode\":\"daily_suggestion\",\"strategy\":\"suggestion\"}"}]'
[System.IO.File]::WriteAllText("$env:TEMP\sg_morning.json", $t1)
aws events put-targets --rule "trading-bot-suggestion-morning" --targets "file://$env:TEMP/sg_morning.json" --region $Region

# ---- Rule 2: Midday P&L check (12:00 ET weekdays) ---------------------------
aws events put-rule `
    --name "trading-bot-suggestion-midday" `
    --schedule-expression "cron(0 $MidnoonUTC ? * MON-FRI *)" `
    --state ENABLED `
    --region $Region
Write-Host "  Rule: trading-bot-suggestion-midday (12:00 ET weekdays)"

$t2 = '[{"Id":"suggestion-midday","Arn":"' + $LambdaArn + '","Input":"{\"mode\":\"suggestion_pnl_check\",\"strategy\":\"suggestion\"}"}]'
[System.IO.File]::WriteAllText("$env:TEMP\sg_midday.json", $t2)
aws events put-targets --rule "trading-bot-suggestion-midday" --targets "file://$env:TEMP/sg_midday.json" --region $Region

# ---- Rule 3: Midday recap (12:30 ET weekdays) --------------------------------
# Always-send: shows underlying move, option P&L, thesis status regardless of thresholds.
aws events put-rule `
    --name "trading-bot-suggestion-recap" `
    --schedule-expression "cron($RecapMinUTC $RecapHrUTC ? * MON-FRI *)" `
    --state ENABLED `
    --region $Region
Write-Host "  Rule: trading-bot-suggestion-recap (12:30 ET weekdays)"

$t3 = '[{"Id":"suggestion-recap","Arn":"' + $LambdaArn + '","Input":"{\"mode\":\"suggestion_recap\",\"strategy\":\"suggestion\"}"}]'
[System.IO.File]::WriteAllText("$env:TEMP\sg_recap.json", $t3)
aws events put-targets --rule "trading-bot-suggestion-recap" --targets "file://$env:TEMP/sg_recap.json" --region $Region

# ---- Rule 4: Pre-close P&L check (15:00 ET weekdays) ------------------------
aws events put-rule `
    --name "trading-bot-suggestion-preclose" `
    --schedule-expression "cron(0 $PrecloseUTC ? * MON-FRI *)" `
    --state ENABLED `
    --region $Region
Write-Host "  Rule: trading-bot-suggestion-preclose (15:00 ET weekdays)"

$t4 = '[{"Id":"suggestion-preclose","Arn":"' + $LambdaArn + '","Input":"{\"mode\":\"suggestion_pnl_check\",\"strategy\":\"suggestion\"}"}]'
[System.IO.File]::WriteAllText("$env:TEMP\sg_preclose.json", $t4)
aws events put-targets --rule "trading-bot-suggestion-preclose" --targets "file://$env:TEMP/sg_preclose.json" --region $Region

# ---- Lambda invoke permissions ----------------------------------------------
Write-Host ""
Write-Host "Granting EventBridge invoke permission on Lambda..." -ForegroundColor Cyan

foreach ($pair in @(
    @("AllowSuggestionMorning",  "trading-bot-suggestion-morning"),
    @("AllowSuggestionMidday",   "trading-bot-suggestion-midday"),
    @("AllowSuggestionRecap",    "trading-bot-suggestion-recap"),
    @("AllowSuggestionPreclose", "trading-bot-suggestion-preclose")
)) {
    $sid = $pair[0]; $rule = $pair[1]
    $result = aws lambda add-permission `
        --function-name trading-bot-runner `
        --statement-id $sid `
        --action "lambda:InvokeFunction" `
        --principal events.amazonaws.com `
        --source-arn "arn:aws:events:us-east-1:975193805321:rule/$rule" `
        --region $Region 2>&1
    if ($result -match "ResourceConflictException") {
        Write-Host "  (permission already exists: $sid)"
    } else {
        Write-Host "  Permission granted: $sid"
    }
}

Write-Host ""
Write-Host "=== Setup complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Schedule (ET, weekdays):" -ForegroundColor White
Write-Host "  09:45  Morning scan  -> scans movers after open, picks best play, sends Telegram"
Write-Host "  12:00  P&L check     -> alerts only if +10%, +20%, or -15% threshold crossed"
Write-Host "  12:30  Midday recap  -> always sends: underlying move, option P&L, thesis status"
Write-Host "  15:00  P&L check     -> alerts only if +10%, +20%, or -15% threshold crossed"
Write-Host ""
Write-Host "Pacific Time equivalent:"
Write-Host "  06:45 AM  Morning scan"
Write-Host "  09:00 AM  P&L check"
Write-Host "  09:30 AM  Midday recap"
Write-Host "  12:00 PM  P&L check"
