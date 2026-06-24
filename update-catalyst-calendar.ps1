# Upload catalyst-calendar.json to AWS SSM. Run this any time you edit the
# JSON file - typically Sunday night when adding next week's macro events.
#
# Usage:
#   1. Edit catalyst-calendar.json - add/remove FOMC, CPI, NFP, FDA, etc.
#   2. powershell -ExecutionPolicy Bypass -File .\update-catalyst-calendar.ps1
#
# Why a script and not an inline aws cli call: PowerShell's argument parsing
# strips inner double-quotes from JSON strings on the way to the AWS CLI,
# corrupting the value. Reading from a file via "file://" bypasses that.

$ErrorActionPreference = "Stop"

$jsonPath = Join-Path $PSScriptRoot "catalyst-calendar.json"
if (-not (Test-Path $jsonPath)) {
    Write-Host "catalyst-calendar.json not found at $jsonPath" -ForegroundColor Red
    exit 1
}

# Validate JSON before uploading
try {
    $content = Get-Content $jsonPath -Raw
    $parsed  = $content | ConvertFrom-Json
} catch {
    Write-Host "catalyst-calendar.json is not valid JSON: $_" -ForegroundColor Red
    exit 1
}

if (-not $parsed -or $parsed.Count -eq 0) {
    Write-Host "WARNING: catalyst-calendar.json is empty - bot will see no macro events." -ForegroundColor Yellow
} else {
    Write-Host "Found $($parsed.Count) catalyst entries:" -ForegroundColor Green
    foreach ($e in $parsed) {
        Write-Host "  $($e.date)  $($e.type)  $($e.ticker)  - $($e.description)"
    }
}

Write-Host ""
Write-Host "Uploading to SSM..."
aws ssm put-parameter `
    --name "/trading-bot/catalyst-calendar" `
    --type String `
    --overwrite `
    --value "file://$jsonPath" `
    --region us-east-1 | Out-Null

if ($LASTEXITCODE -eq 0) {
    Write-Host "Done." -ForegroundColor Green
} else {
    Write-Host "put-parameter failed (exit $LASTEXITCODE)" -ForegroundColor Red
    exit 1
}
