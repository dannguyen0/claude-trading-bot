# One-time setup: initialize git repo, create GitHub repo, enable Pages.
# Run ONCE from the project root.
#
# Prerequisites:
#   1. GitHub CLI installed: https://cli.github.com
#   2. Logged in: gh auth login
#
# Usage: powershell -ExecutionPolicy Bypass -File .\setup-github.ps1

$ErrorActionPreference = "Stop"

$REPO_NAME  = "claude-trading-bot"
$REPO_DESC  = "AWS Lambda options trading bot with Claude AI"
$VISIBILITY = "private"   # change to "public" if you want it public

# Verify gh CLI is available
if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    Write-Host "GitHub CLI not found. Install from https://cli.github.com then run: gh auth login" -ForegroundColor Red
    exit 1
}

$loginCheck = gh auth status 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Not logged in to GitHub. Run: gh auth login" -ForegroundColor Red
    exit 1
}

# Initialize git if not already done
if (-not (Test-Path ".git")) {
    Write-Host "Initializing git..." -ForegroundColor Cyan
    git init
    git branch -M main
}

# Create .gitignore so we don't commit the Lambda package or zip
if (-not (Test-Path ".gitignore")) {
    @"
lambda/package/
lambda/bot.zip
__pycache__/
*.pyc
*.pyo
.env
out.json
"@ | Set-Content ".gitignore" -Encoding utf8
    Write-Host "Created .gitignore" -ForegroundColor Cyan
}

# Stage everything for first commit
git add -A
$status = git status --short
if ($status) {
    git commit -m "Initial commit: trading bot + dashboard"
} else {
    Write-Host "Nothing to commit." -ForegroundColor Yellow
}

# Create GitHub repo (will error if already exists - that's fine)
Write-Host ""
Write-Host "Creating GitHub repo: $REPO_NAME..." -ForegroundColor Cyan
gh repo create $REPO_NAME --description $REPO_DESC --$VISIBILITY --source=. --remote=origin --push
if ($LASTEXITCODE -ne 0) {
    # Might already exist; try to add remote and push
    Write-Host "Repo may already exist - attempting to push..." -ForegroundColor Yellow
    git remote remove origin 2>$null
    $ghUser = (gh api user --jq .login).Trim()
    git remote add origin "https://github.com/$ghUser/$REPO_NAME.git"
    git push -u origin main
}

# Enable GitHub Pages from docs/ folder on main branch
Write-Host ""
Write-Host "Enabling GitHub Pages (docs/ folder)..." -ForegroundColor Cyan
$ghUser = (gh api user --jq .login).Trim()
gh api repos/$ghUser/$REPO_NAME/pages --method POST --field source[branch]=main --field source[path]=/docs 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Pages may already be enabled (that's OK)." -ForegroundColor Yellow
}

# Copy dashboard to docs/ and push
Write-Host ""
Write-Host "Deploying dashboard to docs/..." -ForegroundColor Cyan
if (-not (Test-Path "docs")) { New-Item -ItemType Directory -Path "docs" | Out-Null }
Copy-Item "dashboard\index.html" "docs\index.html" -Force

git add docs\index.html
git commit -m "Deploy dashboard to GitHub Pages" 2>$null
git push origin main

$ghUser = (gh api user --jq .login).Trim()
$pagesUrl = "https://$ghUser.github.io/$REPO_NAME/"

Write-Host ""
Write-Host "==========================================" -ForegroundColor Green
Write-Host "GitHub setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "  Repo   : https://github.com/$ghUser/$REPO_NAME" -ForegroundColor Cyan
Write-Host "  Pages  : $pagesUrl" -ForegroundColor Cyan
Write-Host ""
Write-Host "Pages takes 1-2 minutes to go live on first deploy." -ForegroundColor Yellow
Write-Host "After that, use deploy-github-pages.ps1 to push dashboard updates." -ForegroundColor Yellow
Write-Host "==========================================" -ForegroundColor Green
