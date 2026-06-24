# One-off cleanup: delete today's STRAT3 error rows from Recent Activity that
# came from the NVDA phantom-position retry loop.
#
# Usage: powershell -ExecutionPolicy Bypass -File .\cleanup-naked-spam.ps1

$ErrorActionPreference = "Stop"
$REGION = "us-east-1"
$TABLE  = "trading-bot-trades"
$today  = (Get-Date).ToUniversalTime().ToString("yyyyMMdd")
$pk     = "STRAT3#TRADE#$today"

Write-Host "Querying $pk for error rows..."
$expr = '{":pk":{"S":"' + $pk + '"},":kind":{"S":"error"}}'
[System.IO.File]::WriteAllText("$env:TEMP\cleanup-q.json", $expr)

$resp = aws dynamodb query `
    --table-name $TABLE `
    --key-condition-expression "pk = :pk" `
    --filter-expression "kind = :kind" `
    --expression-attribute-values "file://$env:TEMP/cleanup-q.json" `
    --region $REGION | ConvertFrom-Json

if (-not $resp.Items -or $resp.Items.Count -eq 0) {
    Write-Host "No error rows found for today. Nothing to delete." -ForegroundColor Yellow
    exit 0
}

Write-Host "Found $($resp.Items.Count) error rows. Deleting..." -ForegroundColor Cyan

$deleted = 0
foreach ($item in $resp.Items) {
    $sk = $item.sk.S
    $key = '{"pk":{"S":"' + $pk + '"},"sk":{"S":"' + $sk + '"}}'
    [System.IO.File]::WriteAllText("$env:TEMP\cleanup-k.json", $key)
    aws dynamodb delete-item `
        --table-name $TABLE `
        --key "file://$env:TEMP/cleanup-k.json" `
        --region $REGION | Out-Null
    if ($LASTEXITCODE -eq 0) { $deleted++ }
}

Write-Host "Deleted $deleted of $($resp.Items.Count) error rows." -ForegroundColor Green
