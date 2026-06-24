# AI Trading Bot — Setup Guide

Runs on AWS Lambda + EventBridge + DynamoDB + S3 (all free tier).
Trades an Alpaca paper account using Claude Haiku as the decision engine.

Watchlist: **NVDA, META, AAPL, MSFT, GOOGL, AMZN, TSLA, AMD**
Cadence: every 15 min during market hours, Mon–Fri.

---

## 1. Prerequisites

You need three things before running setup. You've already got the API keys; this is mostly about local tooling.

**AWS CLI v2** — download the Windows MSI from
https://awscli.amazonaws.com/AWSCLIV2.msi and install it. Then in a new PowerShell window:

```powershell
aws configure
# paste AWS Access Key ID, Secret Access Key, region = us-east-1, output = json
```

**Python 3.11+** — confirm with `python --version`. If it's not installed, grab it
from python.org and tick "Add to PATH" during install.

**Your API keys ready** — Alpaca paper Key + Secret, Anthropic key. The setup
script will prompt for all three and store them encrypted in AWS SSM.

---

## 2. Deploy (PowerShell — Windows-native)

From a PowerShell prompt in the project folder:

```powershell
powershell -ExecutionPolicy Bypass -File .\setup.ps1
```

That's it. The script will:

- Prompt for your three API keys and store them in AWS SSM (encrypted)
- Create the DynamoDB table for trade logging
- Create the IAM role with least-privilege permissions
- Package and deploy the Lambda function
- Create a Lambda Function URL (public) so the dashboard can read trades
- Set up EventBridge rules firing every 15 min, Mon–Fri, 9:30–4pm ET
- Create an S3 bucket, splice your keys into the dashboard, and upload it

When finished, it prints the dashboard URL and the Lambda Function URL.

### Alternative: bash (WSL, Git Bash, or macOS/Linux)

The `setup.sh` and `deploy.sh` scripts are the original bash versions. They work
identically if you prefer bash, but the PowerShell versions are the recommended
path on Windows because they need zero extra tooling.

---

## 3. After Setup

**Open the dashboard URL** printed by the script. It should load immediately
showing your Alpaca paper account balance, open positions, and (once the bot
has fired at least once) trade history.

**Manually trigger the bot** to see it run right now:

```powershell
aws lambda invoke --function-name trading-bot-runner --region us-east-1 out.json
Get-Content out.json
```

**Tail live logs** while it runs:

```powershell
aws logs tail /aws/lambda/trading-bot-runner --follow --region us-east-1
```

**Redeploy after editing `lambda\bot.py`:**

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy.ps1
```

---

## Strategy Overview

- **Watchlist:** 8 mega-cap tech names (NVDA, META, AAPL, MSFT, GOOGL, AMZN, TSLA, AMD)
- **Check interval:** every 15 min, Mon–Fri, 9:30am–4pm ET
- **Position sizing:** 10% of portfolio per position
- **Max simultaneous positions:** 5 (50% max deployed, 50% cash buffer)
- **Stop loss:** Claude prompted to auto-sell if position is down >3%
- **Profit target:** Claude prompted to consider selling if up >8% with fading momentum
- **Model:** Claude Haiku 4.5 (fast, ~$1–2/month)
- **Style:** short-term momentum on 15-min bars

All strategy logic lives in `SYSTEM_PROMPT` in `lambda\bot.py`. To change behavior:
edit the prompt, save, run `.\deploy.ps1`. Changes go live in ~10 seconds.

To upgrade to Sonnet for smarter (but pricier) decisions, change in `bot.py`:

```python
model="claude-sonnet-4-6"   # ~$3/$15 per 1M vs Haiku's $1/$5
```

---

## Cost Estimate

| Service         | Cost            |
|-----------------|-----------------|
| AWS Lambda      | $0 (free tier)  |
| AWS EventBridge | $0 (free tier)  |
| AWS DynamoDB    | $0 (free tier)  |
| AWS S3          | ~$0.01          |
| Claude Haiku    | ~$1–2           |
| **Total**       | **~$1–2/month** |

---

## File Map

```
Claude_Trading_Bot/
  README.md                  this file
  setup.ps1                  one-shot Windows-native AWS provisioner
  deploy.ps1                 quick Lambda redeploy after editing bot.py
  setup.sh                   bash equivalent of setup.ps1 (WSL/Git Bash)
  deploy.sh                  bash equivalent of deploy.ps1
  lambda/
    bot.py                   Lambda handler: scheduled trading + /trades endpoint
    requirements.txt         Python deps for the Lambda package
  dashboard/
    index.html               mobile-friendly dashboard (served from S3)
```

---

## Troubleshooting

**Dashboard loads but positions/trades are empty.** The bot only writes records
when it fires. Wait for the next 15-min tick during market hours, or run
`aws lambda invoke` to trigger it once manually.

**"Market closed — skipping" in logs.** Expected outside 9:30–4pm ET and on
weekends/holidays. EventBridge fires on a cron but the Alpaca clock endpoint
is the source of truth.

**Alpaca rejects orders with "insufficient buying power".** Check your Alpaca
paper account has buying power. Default is $100k; you can reset it in the Alpaca
web dashboard (Paper Trading → Reset Account).

**IAM role errors on first run.** AWS IAM propagation takes a few seconds.
The script waits 10s but occasionally a retry is needed — just re-run the script.

**Lambda times out.** Default timeout is 120s. If Claude responses are slow or
you expand the watchlist, bump it in the AWS console (Lambda → Configuration →
General → Timeout) or re-run setup after editing `setup.ps1`.
