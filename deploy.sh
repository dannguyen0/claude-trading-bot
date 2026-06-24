#!/bin/bash
# Quick redeploy — run this whenever you edit bot.py
# Usage: ./deploy.sh

set -e
REGION="us-east-1"
LAMBDA_NAME="trading-bot-runner"

echo "📦 Packaging..."
cd lambda
pip install -r requirements.txt -t package/ -q
cp bot.py package/
cd package && zip -r ../bot.zip . -q && cd ..
rm -rf package
cd ..

echo "🚀 Deploying to Lambda..."
aws lambda update-function-code \
  --region $REGION \
  --function-name $LAMBDA_NAME \
  --zip-file fileb://lambda/bot.zip \
  --no-cli-pager

echo "✅ Done — changes live in ~10 seconds"
