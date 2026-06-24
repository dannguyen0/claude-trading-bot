# Quick redeploy - run after editing lambda\bot.py
# Usage: powershell -ExecutionPolicy Bypass -File .\deploy.ps1

$ErrorActionPreference = "Stop"
$REGION      = "us-east-1"
$LAMBDA_NAME = "trading-bot-runner"

$lambdaDir  = Join-Path $PSScriptRoot "lambda"
$packageDir = Join-Path $lambdaDir "package"
$zipPath    = Join-Path $lambdaDir "bot.zip"

Write-Host "Packaging..."
if (Test-Path $packageDir) { Remove-Item -Recurse -Force $packageDir }
if (Test-Path $zipPath)    { Remove-Item -Force $zipPath }
New-Item -ItemType Directory -Path $packageDir | Out-Null

# Force Linux manylinux wheels. Lambda runtime is Linux x86_64, not Windows,
# so platform-specific C extensions (like pydantic_core) must be Linux builds.
& python -m pip install `
    -r (Join-Path $lambdaDir "requirements.txt") `
    --target $packageDir `
    --platform manylinux2014_x86_64 `
    --implementation cp `
    --python-version 3.12 `
    --only-binary=:all: `
    --upgrade `
    --quiet
if ($LASTEXITCODE -ne 0) { Write-Host "pip install failed" -ForegroundColor Red; exit 1 }
Copy-Item (Join-Path $lambdaDir "bot.py") $packageDir

# Use Python's zipfile instead of Compress-Archive - PS 5.1 has permissions issues with some pip packages
& python -c "
import zipfile, os, sys
pkg = sys.argv[1]
out = sys.argv[2]
with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED) as z:
    for root, dirs, files in os.walk(pkg):
        for f in files:
            fp = os.path.join(root, f)
            z.write(fp, os.path.relpath(fp, pkg))
print('Zipped OK')
" $packageDir $zipPath
if ($LASTEXITCODE -ne 0) { Write-Host "zip failed" -ForegroundColor Red; exit 1 }
Remove-Item -Recurse -Force $packageDir

Write-Host "Deploying to Lambda..."
aws lambda update-function-code --region $REGION --function-name $LAMBDA_NAME --zip-file "fileb://$zipPath" --no-cli-pager | Out-Null

Write-Host "Done - changes live in about 10 seconds." -ForegroundColor Green
