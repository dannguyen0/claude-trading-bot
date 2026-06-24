#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Trading Bot — One-Shot AWS Setup
# Run this ONCE to provision everything on the free tier
#
# Prerequisites:
#   - AWS CLI installed + configured (aws configure)
#   - Python 3.11+ installed locally
#
# Usage:
#   chmod +x setup.sh && ./setup.sh
# ─────────────────────────────────────────────────────────────────────────────

set -e  # exit on any error

REGION="us-east-1"
BOT_NAME="trading-bot"
TABLE_NAME="trading-bot-trades"
LAMBDA_NAME="trading-bot-runner"
ROLE_NAME="trading-bot-lambda-role"
SCHEDULE_NAME="trading-bot-15min"
DASHBOARD_BUCKET="${BOT_NAME}-dashboard-$(aws sts get-caller-identity --query Account --output text)"

echo ""
echo "🤖 Setting up AI Trading Bot on AWS Free Tier"
echo "──────────────────────────────────────────────"
echo "Region:  $REGION"
echo "Bot:     $LAMBDA_NAME"
echo "Table:   $TABLE_NAME"
echo ""

# ── 1. Prompt for secrets ─────────────────────────────────────────────────────
echo "📋 Enter your credentials (stored securely in SSM Parameter Store)"
echo ""

read -p "  Alpaca API Key:    " ALPACA_KEY
read -p "  Alpaca API Secret: " ALPACA_SECRET
read -p "  Anthropic API Key: " ANTHROPIC_KEY

echo ""
echo "💾 Saving secrets to SSM Parameter Store..."
aws ssm put-parameter --region $REGION --name "/trading-bot/alpaca-key"     --value "$ALPACA_KEY"     --type SecureString --overwrite
aws ssm put-parameter --region $REGION --name "/trading-bot/alpaca-secret"  --value "$ALPACA_SECRET"  --type SecureString --overwrite
aws ssm put-parameter --region $REGION --name "/trading-bot/anthropic-key"  --value "$ANTHROPIC_KEY"  --type SecureString --overwrite
echo "   ✅ Secrets saved"

# ── 2. DynamoDB table ─────────────────────────────────────────────────────────
echo ""
echo "🗄️  Creating DynamoDB table..."
aws dynamodb create-table \
  --region $REGION \
  --table-name $TABLE_NAME \
  --attribute-definitions \
    AttributeName=pk,AttributeType=S \
    AttributeName=sk,AttributeType=S \
  --key-schema \
    AttributeName=pk,KeyType=HASH \
    AttributeName=sk,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --no-cli-pager 2>/dev/null || echo "   Table already exists, skipping"
echo "   ✅ DynamoDB ready"

# ── 3. IAM role for Lambda ────────────────────────────────────────────────────
echo ""
echo "🔑 Creating IAM role..."
TRUST='{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}'

ROLE_ARN=$(aws iam create-role \
  --role-name $ROLE_NAME \
  --assume-role-policy-document "$TRUST" \
  --query "Role.Arn" --output text 2>/dev/null \
  || aws iam get-role --role-name $ROLE_NAME --query "Role.Arn" --output text)

# Attach policies
aws iam attach-role-policy --role-name $ROLE_NAME \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

aws iam put-role-policy --role-name $ROLE_NAME --policy-name trading-bot-policy \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": ["dynamodb:PutItem","dynamodb:Query","dynamodb:Scan"],
        "Resource": "arn:aws:dynamodb:*:*:table/trading-bot-trades"
      },
      {
        "Effect": "Allow",
        "Action": ["ssm:GetParameter"],
        "Resource": "arn:aws:ssm:*:*:parameter/trading-bot/*"
      },
      {
        "Effect": "Allow",
        "Action": ["s3:PutObject","s3:GetObject"],
        "Resource": "arn:aws:s3:::'"$DASHBOARD_BUCKET"'/*"
      }
    ]
  }'

echo "   ✅ IAM role ready: $ROLE_ARN"
sleep 10  # IAM propagation delay

# ── 4. Package and deploy Lambda ──────────────────────────────────────────────
echo ""
echo "📦 Packaging Lambda function..."
cd lambda
pip install -r requirements.txt -t package/ -q
cp bot.py package/
cd package && zip -r ../bot.zip . -q && cd ..
rm -rf package
cd ..

echo "🚀 Deploying Lambda..."
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws lambda create-function \
  --region $REGION \
  --function-name $LAMBDA_NAME \
  --runtime python3.12 \
  --role $ROLE_ARN \
  --handler bot.handler \
  --zip-file fileb://lambda/bot.zip \
  --timeout 120 \
  --memory-size 256 \
  --environment "Variables={DYNAMO_TABLE=$TABLE_NAME}" \
  --no-cli-pager 2>/dev/null || \
aws lambda update-function-code \
  --region $REGION \
  --function-name $LAMBDA_NAME \
  --zip-file fileb://lambda/bot.zip \
  --no-cli-pager

echo "   ✅ Lambda deployed"

# ── 5. EventBridge schedule (every 15 min, Mon–Fri, 9:30–16:00 ET) ────────────
echo ""
echo "⏰ Setting up EventBridge schedules..."

# We need multiple rules to cover 9:30–16:00 ET (14:30–21:00 UTC)
# Single cron: every 15 min between 14:30–21:00 UTC on weekdays
aws events put-rule \
  --region $REGION \
  --name $SCHEDULE_NAME \
  --schedule-expression "cron(0/15 14-20 ? * MON-FRI *)" \
  --state ENABLED \
  --no-cli-pager

# Add 9:30 open bell specifically (14:30 UTC)
aws events put-rule \
  --region $REGION \
  --name "${SCHEDULE_NAME}-open" \
  --schedule-expression "cron(30 14 ? * MON-FRI *)" \
  --state ENABLED \
  --no-cli-pager

LAMBDA_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${LAMBDA_NAME}"

# Grant EventBridge permission to invoke Lambda
aws lambda add-permission \
  --function-name $LAMBDA_NAME \
  --statement-id eventbridge-15min \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${SCHEDULE_NAME}" \
  --no-cli-pager 2>/dev/null || true

aws lambda add-permission \
  --function-name $LAMBDA_NAME \
  --statement-id eventbridge-open \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${SCHEDULE_NAME}-open" \
  --no-cli-pager 2>/dev/null || true

# Wire targets
aws events put-targets \
  --region $REGION \
  --rule $SCHEDULE_NAME \
  --targets "Id=bot,Arn=$LAMBDA_ARN" \
  --no-cli-pager

aws events put-targets \
  --region $REGION \
  --rule "${SCHEDULE_NAME}-open" \
  --targets "Id=bot-open,Arn=$LAMBDA_ARN" \
  --no-cli-pager

echo "   ✅ Schedules active (every 15 min, Mon–Fri market hours)"

# ── 6. S3 dashboard bucket ────────────────────────────────────────────────────
echo ""
echo "🌐 Creating dashboard S3 bucket..."
aws s3 mb s3://$DASHBOARD_BUCKET --region $REGION 2>/dev/null || true
aws s3 website s3://$DASHBOARD_BUCKET --index-document index.html
aws s3api put-bucket-policy \
  --bucket $DASHBOARD_BUCKET \
  --policy '{
    "Version":"2012-10-17",
    "Statement":[{
      "Sid":"PublicRead",
      "Effect":"Allow",
      "Principal":"*",
      "Action":"s3:GetObject",
      "Resource":"arn:aws:s3:::'"$DASHBOARD_BUCKET"'/*"
    }]
  }'

# Upload dashboard
aws s3 cp dashboard/index.html s3://$DASHBOARD_BUCKET/index.html \
  --content-type text/html

DASHBOARD_URL="http://${DASHBOARD_BUCKET}.s3-website-${REGION}.amazonaws.com"

echo "   ✅ Dashboard live at: $DASHBOARD_URL"

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Trading Bot is LIVE on AWS Free Tier"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  📊 Dashboard:  $DASHBOARD_URL"
echo "  🤖 Lambda:     $LAMBDA_NAME"
echo "  📅 Schedule:   Every 15 min, Mon–Fri 9:30–4pm ET"
echo "  💾 Trades:     DynamoDB → $TABLE_NAME"
echo ""
echo "  To test manually:"
echo "  aws lambda invoke --function-name $LAMBDA_NAME --region $REGION out.json && cat out.json"
echo ""
echo "  To view logs:"
echo "  aws logs tail /aws/lambda/$LAMBDA_NAME --follow --region $REGION"
echo ""
