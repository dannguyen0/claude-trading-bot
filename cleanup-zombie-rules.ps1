# One-off: delete the two zombie EventBridge rules from the original 15-min setup.
#
# Why: trading-bot-15min was firing every 15 minutes from 9 AM to 4 PM ET,
# which caused mode=midday to run ~28x/day instead of once. trading-bot-15min-open
# is a duplicate of trading-bot-midday (both fire 10:30 ET). Both should have
# been killed by reconfigure-schedule.ps1 but were missed.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File .\cleanup-zombie-rules.ps1

$ErrorActionPreference = "Continue"
$REGION = "us-east-1"
$LAMBDA_NAME = "trading-bot-runner"

$zombies = @("trading-bot-15min", "trading-bot-15min-open")

foreach ($rule in $zombies) {
    Write-Host "--- Removing rule: $rule ---"

    # Targets must be removed before the rule itself
    $targets = aws events list-targets-by-rule --rule $rule --region $REGION --query "Targets[].Id" --output text 2>$null
    if ($targets) {
        $targetIds = $targets -split "\s+" | Where-Object { $_ }
        Write-Host "  removing targets: $($targetIds -join ', ')"
        aws events remove-targets --rule $rule --ids $targetIds --region $REGION 2>$null | Out-Null
    } else {
        Write-Host "  no targets attached"
    }

    aws events delete-rule --name $rule --region $REGION 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  rule deleted" -ForegroundColor Green
    } else {
        Write-Host "  delete-rule failed (may already be gone)" -ForegroundColor Yellow
    }

    # Clean up any stale Lambda permissions referencing this rule
    foreach ($stmtCandidate in @("${rule}-invoke", "eventbridge-15min", "eventbridge-open")) {
        aws lambda remove-permission --function-name $LAMBDA_NAME --statement-id $stmtCandidate --region $REGION 2>$null | Out-Null
    }
}

Write-Host ""
Write-Host "Verification - remaining rules:"
aws events list-rules --name-prefix trading-bot --region $REGION --query "Rules[].{Name:Name,Schedule:ScheduleExpression}" --output table
