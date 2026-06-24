# Deploy dashboard to GitHub Pages.
# Run after any dashboard change.
#
# Usage: powershell -ExecutionPolicy Bypass -File .\deploy-github-pages.ps1

$ErrorActionPreference = "Continue"

# Copy latest dashboard to docs/
if (-not (Test-Path "docs")) { New-Item -ItemType Directory -Path "docs" | Out-Null }
Copy-Item "dashboard\index.html" "docs\index.html" -Force
Write-Host "Copied dashboard/index.html -> docs/index.html" -ForegroundColor Cyan

git add docs\index.html
$status = git status --short docs\index.html
if (-not $status) {
    Write-Host "No changes to dashboard - nothing to deploy." -ForegroundColor Yellow
    exit 0
}

git commit -m "Update dashboard"
git push origin main

$ghUser = (gh api user --jq .login 2>$null).Trim()
$repoName = (git remote get-url origin 2>$null) -replace ".*github\.com[:/].*?/(.+?)(?:\.git)?$", '$1'

Write-Host ""
Write-Host "==========================================" -ForegroundColor Green
Write-Host "Dashboard deployed!" -ForegroundColor Green
Write-Host "  URL: https://$ghUser.github.io/$repoName/" -ForegroundColor Cyan
Write-Host "  (takes ~30 seconds to refresh)" -ForegroundColor Yellow
Write-Host "==========================================" -ForegroundColor Green
