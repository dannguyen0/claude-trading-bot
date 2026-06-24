# CLAUDE.md — Claude Trading Bot

This file is the handoff blueprint for any Claude agent (Claude Code, Cowork, API) picking up this project. It captures everything material that lives outside the code itself: intent, strategy rules, architecture, environment quirks, solved bugs, current state, and the next planned steps.

Repo root: `C:\Users\Dan\Documents\Professional\Python\Claude_Trading_Bot` (Windows host, deploys to AWS Linux Lambda).

---

## 1. Project Purpose & Core Trading Strategy

### 1.1 What this is

A **paper-trading options bot** that runs on AWS Lambda, uses Alpaca's paper API for order routing, and uses **Claude Haiku 4.5** (`claude-haiku-4-5-20251001`) as the decision engine for ticker selection, signal scoring, and post-event review. EventBridge cron rules invoke the same Lambda with different `{"mode": ...}` payloads. State (open positions, closed trades, notes, errors) lives in DynamoDB. A static-site S3 dashboard reads the same Lambda over an API Gateway HTTP endpoint to display performance and open positions. SNS sends an email on every open/close/partial close and on any uncaught exception.

It is intentionally **three independent strategies running in parallel on three separate paper accounts**. Each strategy has its own Alpaca keys, its own DynamoDB partition prefix, its own EventBridge schedule, and its own dashboard tab. They share the Lambda, the regime-check, the catalyst calendar, and the helper code.

### 1.2 The three strategies — friendly name → internal code name → structure

| UI name | Internal `strategy=` | DDB prefix | What it trades | Account |
| --- | --- | --- | --- | --- |
| **Conservative** | `credit_spread` | (unprefixed) | Put credit spreads, SPY/QQQ/IWM, 30–45 DTE | `~$5k` paper |
| **Aggressive** | `momentum` | `STRAT2#` | **Squeeze-Cross**: single-leg directional calls/puts on TTM-Squeeze + EMA-cross fire, scored by Claude with news context | `$10k` paper (Dan to reset on Alpaca UI for the test) |
| **Aggressive 2.0** | `naked` | `STRAT3#` | Naked single-leg directional options on a catalyst-weighted 14-ticker universe | `~$2k` paper |

**Critical naming rule:** Always use *Conservative / Aggressive / Aggressive 2.0* in conversation, in emails, and in the dashboard UI. The internal `credit_spread / momentum / naked` strings must NOT be changed — they are baked into SSM key paths, DDB partition keys, and EventBridge target payloads. Changing them silently breaks everything.

**Also critical:** "Aggressive" was rebuilt 2026-05-12 from the old momentum debit-spread design into the Squeeze-Cross design from a Gemini spec. The old momentum debit-spread functions (`run_momentum_premarket`, `run_momentum_midday`, `_execute_momentum_open`, `claude_momentum_scan`, `claude_momentum_picks`, etc.) are **retained as dead code in `bot.py`** so we can revert if needed. The dispatcher (`handler` → `if strategy == STRATEGY_MOMENTUM`) routes every scheduled `mode` to the new `run_squeeze_*` functions. Open positions opened by the new code carry `kind: "squeeze"` to distinguish from any legacy `kind: "momentum"` rows.

---

### 1.3 Conservative (Credit Spread) — rules

- **Universe**: `SPY`, `QQQ`, `IWM` only.
- **Structure**: put credit spreads only, $5 wide.
- **Short leg delta band**: 0.16–0.20 (abs value for puts).
- **DTE band**: 30–45 days at entry.
- **Min credit**: $0.50 per spread.
- **Risk caps**: max 5 concurrent spreads, ≤ $2,500 total at-risk, ≤ 2 spreads per underlying.
- **Exit rules**: 50% profit take (a GTC limit placed at fill time), 2× credit stop-loss, time-exit at ≤ 7 DTE.
- **Cadence (3 EventBridge rules, weekdays, ET)**:
  - `08:45 morning` → pull VIX + daily indicators, Claude writes a one-paragraph market context note. **No trades.** Also refreshes any missing 50%-profit-take GTCs on the prior day's open spreads.
  - `10:30 midday` → scan 30–45 DTE put chains on each ticker, hand candidates to Claude with the morning note + budget + VIX, auto-execute the spread(s) Claude picks, place 50%-profit GTCs immediately.
  - `15:00 eod` → sweep open spreads for 2× credit stop-loss and ≤ 7 DTE time exit. Close qualifying spreads, cancel their profit-take GTCs first.
- **IV signal**: VIX as IVR proxy for now (real IVR is roadmap).
- **Indicators**: RSI-14 (neutral 40–60 preferred), EMA-8/21 (trend filter), MACD, Bollinger width.

### 1.4 Aggressive (Squeeze-Cross) — rules

This replaces the original momentum debit-spread design that hadn't fired in ~2 weeks. New design from Gemini.

- **Universe**: `NVDA`, `TSLA`, `MSTR`, `AMD`, `SPY`, `QQQ`, `COIN`, `META` (`SQUEEZE_UNIVERSE`). Each ticker has a meta tag (`Momentum / Sentiment / Volatility / Macro / Crypto-Beta / Trend`) fed to Claude in the scoring prompt.
- **Structure**: single-leg directional options — calls if `direction=bullish`, puts if `bearish`. **No spreads, no hedge.** Defined max loss = premium paid.
- **Delta band**: 0.55–0.65.
- **DTE band**: 3–14 days.
- **Position sizing**: 20% of current Alpaca equity per trade (computed live from `/v2/account` at order time). `compute_squeeze_position_size(equity, premium_per_contract)`.
- **Concurrent positions**: hard cap of 5.
- **Entry signal (Level 1, pure code, no Claude)**:
  1. **TTM Squeeze fire** on 15-min bars: BB(20, 2) was inside KC(20, 1.5) on the previous bar AND has popped out on the current bar.
  2. **EMA 9/21 cross** on 15-min bars in the squeeze-direction (bullish cross = 9 over 21 in last N bars, bearish = inverse). Direction is determined by linear-regression slope of the BB/KC midpoint.
  3. **VWAP alignment**: price above today's VWAP for bullish, below for bearish.
  - Any ticker that passes all three is a "Level 1 passer" and goes to Level 2.
- **Entry signal (Level 2, Claude scoring)**:
  - For each L1 passer, pull last 5 Finnhub news headlines (last 7 days), combine with the technical readout + active regime, send to Claude Haiku via `SYSTEM_SQUEEZE_STRATEGIST`.
  - Claude returns `{action: BUY|HOLD|SKIP, score: 0–10, reason: "<one sentence>"}`.
  - Enter only on `BUY` with `score >= 7` (`SQUEEZE_SCORE_MIN`).
- **Contract selection**: `build_squeeze_contract()` picks the OCC option contract closest to delta 0.55–0.65 in the 3–14 DTE band. Validates bid/ask spread sanity. Builds a limit order at mid price.
- **Fill stalking**: `_stalk_squeeze_fill()` — submits at mid, waits 60s, if unfilled cancels and resubmits +$0.02. Max 3 retries. If still unfilled, gives up and logs `squeeze_no_fill`. **Crucially**, the DDB `SPREAD#OPEN` row is only written AFTER a real fill is confirmed (this was a bug fix — see §4).
- **Exit ladder (multi-tier scale-out)**: on every spread, store `qty_original` (immutable) and `qty` (decremented as tranches close):
  - **Stop loss**: -20% of premium paid → close ALL remaining.
  - **TP1 (+30% gain)**: close 50% of `qty_original`. Move stop on remainder to breakeven. Set `tp1_hit=True`. Sends `[Aggressive] TP1` email.
  - **TP2 (+60% gain)**: close 25% of `qty_original`. Set `tp2_hit=True, trailing=True`. Sends `[Aggressive] TP2` email.
  - **Trailing runner (final 25%)**: once `trailing=True`, exit on the first 1-min bar that closes BELOW (bullish) or ABOVE (bearish) the 9-EMA on 1-min bars. `compute_9ema_1min()`.
  - **Hard time exit**: ≤ 1 DTE remaining → close any remainder.
- **Cadence**: reuses the existing Aggressive EventBridge schedule (was originally for the old momentum design — see §1.7) but every scheduled mode now routes to a Squeeze-Cross function:
  - `09:00 premarket` → `run_squeeze_scan()` (look for L1 passers, score with Claude, queue entries).
  - `09:45 postopen / 10:30 orb / 12:00 midday / 13:30 afternoon` → `run_squeeze_tick()` (re-scan + manage open positions).
  - `15:00 preclose` → `run_squeeze_sweep()` (exit evaluation only).
  - Every 15 min, 10:00–15:45 → `run_squeeze_tick()` via `mode=exit_sweep`.
- **DDB schema for an open Squeeze-Cross row** — keys to know:
  - `strategy: "momentum"`, `kind: "squeeze"` (this is how we tell it apart from any old momentum debit-spread rows).
  - `id`: `SQ-<UNDER>-<EXPIRY>-<STRIKE><C|P>-<6hexhash>`.
  - `qty_original`, `qty`, `premium_paid`, `total_cost`, `entry_price`, `delta_at_entry`, `iv_at_entry`.
  - `stop_value`, `tp1_value`, `tp2_value` (per-contract prices, not percentages).
  - `tp1_hit`, `tp2_hit` (bool), `trailing` (bool — flips after TP2 hits).
  - `score`, `claude_reason`, `open_order_id`, `opened_at`.

### 1.5 Aggressive 2.0 (Naked Options) — rules

- **Universe**: `NAKED_UNIVERSE_LAYER1` always scanned (`SPY, QQQ, NVDA, TSLA, AAPL`) + `NAKED_UNIVERSE_LAYER2_DEFAULT` (`AMD, META, GOOGL, MRVL, AMZN, MSFT, COIN, PLTR, SMCI`) — Layer 2 currently always-included (Phase B would narrow to catalyst-active tickers only).
- **Structure**: single-leg naked calls or puts. No hedge. Max loss = premium paid.
- **Delta band**: 0.30–0.55 (avoid lottery tickets, avoid deep ITM).
- **DTE band**: 0–28 days. Stop tier varies by DTE (see below).
- **Position caps**: max 2 concurrent, ≤ $2,000 total at-risk.
- **Sizing by Claude score (0–10)**:
  - Score ≥ 8 → up to $1,000 premium (`NAKED_DEBIT_HIGH`).
  - Score 6–7 → up to $500 (`NAKED_DEBIT_MID`).
  - Score < 6 → skip.
- **DTE-tiered stop loss (% of premium paid)**:
  - 0 DTE → 30%; 1–3 DTE → 35%; 4–7 → 40%; 8–14 → 42%; 15–28 → 45%.
  - Absolute floor: 60% (`NAKED_STOP_FLOOR_PCT`).
- **AWR validation**: candidate must have `breakeven move ≤ 60% of 8-week Average Weekly Range` (`compute_awr()`).
- **Bid-ask sanity**: ≤ 10% of mid.
- **Earnings rule**: refuse trades when target's earnings is today or tomorrow (`NAKED_EARNINGS_MIN_DAYS = 2`). The DTE chosen for the contract gets capped to expire BEFORE the earnings date when relevant.
- **Catalyst calendar** (drives scoring weight): `/trading-bot/catalyst-calendar` SSM param. Auto-refreshed Sunday from Finnhub economic calendar (FOMC/CPI/PPI/NFP/FDA). Manual entries added via `update-catalyst-calendar.ps1`. Earnings dates pulled from `/trading-bot/earnings-calendar` (also Finnhub).
- **Cadence (6 EventBridge rules, ET)**:
  - `09:00 premarket` → `run_naked_premarket()`: scan universe, build summaries, Claude scores all candidates, save scan note. Persists `latest_naked_scan` for entry runs.
  - `10:00 / 12:00 / 14:00 entry` → `run_naked_entry()`: read latest scan, attempt to open the top scored candidates within budget.
  - `15:00 preclose` → final entry attempt of the day.
  - Every 15 min, 10:00–15:45 → `run_naked_exit_sweep()`: walk open positions, evaluate stop / TP / time exits.
- **DDB schema for open Naked row** — keys to know:
  - `strategy: "naked"`, `kind: "naked"`.
  - `id`: `N-<UNDER>-<EXPIRY>-<STRIKE><C|P>-<6hex>`.
  - `qty`, `premium_paid`, `total_cost`, `stop_loss_pct`, `stop_loss_value_per_contract`.
  - `breakeven`, `awr_at_entry`, `catalyst`, `score`, `rationale`, `opened_at`.

### 1.6 Cross-cutting: market regime gate

A single regime check runs `08:30 ET` weekdays (`trading-bot-mom-regime` rule, payload `{"mode":"regime_check"}`). It pulls VIX + SPY daily indicators, asks Claude to classify the market as **green / yellow / red** with a one-sentence note and `new_trades_allowed: true|false`, and writes the result to `/trading-bot/regime` (SSM, plain String). Both Aggressive and Aggressive 2.0 consult this before opening any new position. Conservative does not gate on it.

### 1.7 Daily reconciliation

Runs `08:00 ET` weekdays (`trading-bot-reconcile`, payload `{"mode":"reconcile"}`). `run_reconciliation()` iterates all three strategies, lists `SPREAD#OPEN` rows from DDB and compares to Alpaca's `/v2/positions`. Any DDB row whose `contract_symbol` is NOT in Alpaca's live positions is auto-archived as `phantom position - reconciliation`. Prevents the 15-min retry loop we hit (see §4.1).

---

## 2. Architecture & File Structure

### 2.1 Top-level repo layout

```
Claude_Trading_Bot/
├── lambda/
│   ├── bot.py              # ~5,200 lines — the entire bot
│   └── requirements.txt    # anthropic==0.49.0, requests==2.32.3, boto3==1.35.0
├── dashboard/
│   └── index.html          # Static SPA, 3 tabs (Conservative / Aggressive / Aggressive 2.0)
├── deploy.ps1              # Repackage + push bot.py to Lambda (~15s end-to-end)
├── deploy-dashboard.ps1    # Upload dashboard/index.html to S3 bucket
├── setup.ps1               # One-time AWS provisioning (Lambda role + table + bucket + first rule)
├── setup-api-gateway.ps1   # API Gateway HTTP API for dashboard reads
├── reconfigure-schedule.ps1      # Conservative: 3 EventBridge rules (morning/midday/eod)
├── setup-momentum-schedule.ps1   # Aggressive: 8 EventBridge rules including the exit-sweep
├── setup-naked-schedule.ps1      # Aggressive 2.0: 6 EventBridge rules
├── setup-reconcile-schedule.ps1  # Daily 8:00 ET reconciliation
├── update-catalyst-calendar.ps1  # Manual append to /trading-bot/catalyst-calendar SSM param
├── grant-ssm-write.ps1     # Adds ssm:PutParameter to the Lambda role
├── add-alerts.ps1          # SNS topic + email subscription provisioning
├── cleanup-zombie-rules.ps1      # One-off: deleted old trading-bot-15min* rules 2026-04-27
└── cleanup-naked-spam.ps1        # One-off: deleted today's STRAT3 error rows from DDB
```

There is no other source — `bot.py` is a deliberately single-file Lambda. Everything is one Python module so the deploy is just "zip + push".

### 2.2 `bot.py` internal layout (top to bottom, by purpose)

Use these section comments as section IDs when grepping. Line numbers are approximate (the file evolves).

1. **Imports + `_to_ddb` Decimal helper** (≈ 1–50). Recursively converts floats → `Decimal(str(...))` for DynamoDB writes.
2. **`# ---- Config`** (≈ 51–300). Constants for all three strategies, plus the score-tier helpers (`momentum_size_for_score`, `naked_size_for_score`, `naked_stop_for_dte`, `momentum_stop_for_score`, `momentum_time_gain_for_score`, `get_naked_universe`, `get_macro_catalysts`, `get_active_catalysts`, `compute_awr`).
3. **`# ---- HTTP session with retries`** (≈ 303). Single `requests.Session` with `urllib3.Retry` for Alpaca + Finnhub + Polygon. All outbound HTTP goes through here.
4. **`# ---- AWS clients`** + `get_secret` (≈ 316–335). Module-level `boto3` clients (DDB, SSM, SNS). `get_secret` reads SSM `SecureString` with WithDecryption, cached in-process for warm containers (catalyst-calendar bypasses the cache deliberately).
5. **`# ---- Strategy dispatch`** (≈ 334–360). The three `STRATEGY_*` constants and `_STRATEGY_KEY_PATHS` mapping each to its SSM Alpaca key/secret path. `set_active_strategy(name)` flips the module-level flag (Lambda containers are single-threaded so this is safe). `alpaca_headers()` reads the flag to pick which paper account to call.
6. **`# ---- Alpaca helpers`** (≈ 360–680). `get_account`, `get_positions`, `market_is_open`, `get_15min_bars`, `get_intraday_bars` (used for ORB + Squeeze + VWAP), `compute_orb_status`, `get_daily_bars` (REQUIRES `start`/`end` — see §4.4), `get_latest_quote`, `get_options_contracts`, `get_option_snapshots`, `place_mleg_order` (Conservative spreads), `place_single_leg_order` (Aggressive + Aggressive 2.0), `cancel_order`.
7. **`# ---- External data: VIX`** (≈ 672–690). Free StoqQ feed.
8. **`# ---- Indicators`** (≈ 690–795). `ema, rsi_14, ema_series, macd, bb_width_pct, volume_ratio, atr_14`.
9. **`# ---- Market regime gate`** (≈ 795–905). `get_spy_indicators, claude_regime_check, save_regime, get_active_regime, run_regime_check`.
10. **`# ---- Universe refresh (Aggressive Layer 2)`** (≈ 905–1100). Polygon-driven dynamic ticker discovery, weekly. Saves to `/trading-bot/dynamic-tickers`. `run_refresh_universe`.
11. **`# ---- Daily reconciliation`** (≈ 1017). `run_reconciliation` — iterates strategies, compares DDB vs Alpaca, archives phantoms.
12. **Squeeze-Cross stack** (≈ 1170–1965): `bollinger_bands_full, true_range, atr_simple, keltner_channels, linear_regression_slope, ttm_squeeze_status, compute_vwap_today, compose_squeeze_signal, claude_squeeze_eval, compute_9ema_1min, _close_squeeze_tranche, _evaluate_squeeze_exit, _sweep_squeeze, run_squeeze_sweep, run_squeeze_tick, build_squeeze_contract, _stalk_squeeze_fill, compute_squeeze_position_size, _execute_squeeze_open, run_squeeze_entry, run_squeeze_scan`. Plus `SYSTEM_SQUEEZE_STRATEGIST` system prompt.
13. **`# ---- Earnings refresh (Finnhub)`** (≈ 1965–2140). `get_earnings_dates, _classify_econ_event, fetch_finnhub_economic_calendar, merge_econ_into_catalyst_calendar, fetch_next_earnings, run_refresh_earnings`.
14. **`# ---- DynamoDB: spread state + notes`** (≈ 2157–2225). `_pk_prefix` (returns `""` / `"STRAT2#"` / `"STRAT3#"` based on `_ACTIVE_STRATEGY`), `save_open_spread, load_open_spreads, archive_spread, save_note, log_event`.
15. **`# ---- Alerts`** (≈ 2221–2400). `send_alert` (raw SNS publish), `_alert_short_label`, `_alert_full_label`, **`send_position_alert(event_type, spread, outcome)`** — the unified entry point used by ALL six open/close/tp1/tp2 call sites. Also `format_open_email` and `format_close_email` (used only by the Conservative branch inside `send_position_alert`).
16. **`# ---- Claude: per-mode prompting`** (≈ 2380–2545). System prompts (`SYSTEM_REGIME_CHECK, SYSTEM_MORNING, SYSTEM_MIDDAY, SYSTEM_EOD, SYSTEM_MOMENTUM_PREMARKET, SYSTEM_MOMENTUM_MIDDAY, SYSTEM_NAKED_PREMARKET, SYSTEM_SQUEEZE_STRATEGIST`). `_anthropic_client`, `claude_morning_note, claude_midday_picks, claude_eod_review`, `_parse_json` (extracts JSON from Claude's `{...}` continuation prefill).
17. **Conservative-specific** (≈ 2545–3025): `build_candidates_for_ticker, compute_budget, _open_orders_by_option_symbol, _has_live_profit_take, refresh_profit_takes, run_morning, _latest_note_body, _execute_open, _wait_for_fill, run_midday, _current_spread_debit, _close_spread, run_eod`.
18. **GET handlers for dashboard** (≈ 3023–3275): `_cors_headers, _recent_trades, _open_spreads_with_marks, _days_between, _closed_spreads, _compute_analytics, _recent_notes`.
19. **Old Aggressive (debit-spread) functions — DEAD CODE, kept for revert** (≈ 3275–4625, intermixed): `get_sector, get_momentum_universe, momentum_summarize, claude_momentum_scan, save_momentum_note, run_momentum_premarket, _current_debit_spread_value, _current_underlying_price, _evaluate_momentum_exit, _close_momentum_spread, _sweep_momentum_exits, run_momentum_postopen, run_momentum_orb, run_momentum_afternoon, run_momentum_exit_sweep, run_momentum_preclose, momentum_spread_width, _latest_momentum_scan, compute_momentum_budget, build_momentum_pair, claude_momentum_picks, _execute_momentum_open, run_momentum_midday`. The dispatcher in `handler` no longer calls these; the Squeeze functions take their place.
20. **Aggressive 2.0 (Naked)** (≈ 3933–4600): `get_catalysts_for_ticker, naked_summarize_ticker, claude_naked_scan, save_naked_note, run_naked_premarket, compute_naked_budget, build_naked_contract, _execute_naked_open, run_naked_entry, _current_naked_value, _current_underlying_price_for_naked, _evaluate_naked_exit, _close_naked_position` (contains the phantom-position detector, see §4.1), `_sweep_naked_exits, run_naked_exit_sweep, run_naked_check`.
21. **`# ---- Main handler`** (≈ 5077-end): `handler(event, context)`. Branches on GET vs scheduled. Maintenance modes (`refresh_earnings, regime_check, refresh_universe, reconcile`) ignore market hours. Other modes early-return `market_closed` unless `force=True` is in the event. Then `set_active_strategy()` + per-strategy `mode` switch. Top-level `except` logs the traceback to DDB and sends an SNS alert.

### 2.3 Dashboard (`dashboard/index.html`)

Single static HTML file, no build step. Three tabs: Conservative, Aggressive (subtitle: "TTM Squeeze + EMA cross · 15-min intraday"), Aggressive 2.0. Each tab hits `/api/?strategy=<name>&action=<spreads|trades|notes|analytics>`. P&L split into separate $ and % columns. The old "Risk Deployed" donut was replaced with a "Period Performance" card. Closed-trades bar chart driven by the `analytics` action. Deploy via `deploy-dashboard.ps1` (aws s3 cp).

---

## 3. Technical Choices & Environment Constraints

### 3.1 Runtime

- **Lambda runtime**: `python3.12` on Linux x86_64. Memory 512 MB, timeout ~3 min (set during `setup.ps1`).
- **Local dev**: Windows 10/11 + PowerShell 5.1. **Dan is non-technical and uses PowerShell, not bash.** All scripts are `.ps1`. WSL is not in use.
- **AWS account**: `975193805321`, region `us-east-1`.
- **Lambda function name**: `trading-bot-runner`.
- **DynamoDB table**: `trading-bot-trades`. Key schema: `pk` (e.g., `TRADE#20260515`, `STRAT2#SPREAD#OPEN`, `STRAT3#SPREAD#CLOSED#20260515`), `sk` (timestamp#symbol or spread id).
- **S3 bucket (dashboard)**: `trading-bot-dashboard-975193805321`. Website URL: `http://trading-bot-dashboard-975193805321.s3-website-us-east-1.amazonaws.com`.
- **API Gateway HTTP API**: `trading-bot-api` at `https://gt62ul2jgk.execute-api.us-east-1.amazonaws.com`. Lambda Function URL was abandoned due to an unresolved 403.

### 3.2 External APIs

| Service | Purpose | Auth |
| --- | --- | --- |
| **Anthropic** | Claude Haiku 4.5 (`claude-haiku-4-5-20251001`) for all LLM calls. ~9 distinct prompt sites. | `/trading-bot/anthropic-key` SSM |
| **Alpaca Markets (paper)** | Quotes, bars, options chains, snapshots, order placement, account, positions. Three separate paper accounts. | `/trading-bot/{,strategy2/,strategy3/}alpaca-key` + `-secret` |
| **Finnhub (free tier)** | Per-symbol news (squeeze scoring), earnings calendar (auto-refresh Sun), economic calendar (auto-refresh Sun, drives catalyst-calendar). | `/trading-bot/finnhub-key` |
| **Polygon.io (free tier)** | Grouped daily aggregates for the Aggressive Layer 2 universe discovery (weekly refresh Sat 23:00 ET). | `/trading-bot/polygon-key` |
| **StoqQ** | Free VIX feed (Conservative regime context). | None |

### 3.3 SSM parameters (all SecureString unless noted)

- `/trading-bot/alpaca-key`, `/trading-bot/alpaca-secret` — Conservative.
- `/trading-bot/strategy2/alpaca-key`, `/trading-bot/strategy2/alpaca-secret` — Aggressive.
- `/trading-bot/strategy3/alpaca-key`, `/trading-bot/strategy3/alpaca-secret` — Aggressive 2.0.
- `/trading-bot/anthropic-key`, `/trading-bot/finnhub-key`, `/trading-bot/polygon-key`.
- `/trading-bot/earnings-calendar` (String, JSON dict `{ticker: 'YYYY-MM-DD'}`) — auto-refreshed Sunday.
- `/trading-bot/dynamic-tickers` (String, JSON list) — Aggressive Layer 2 universe, auto-refreshed Saturday.
- `/trading-bot/catalyst-calendar` (String, JSON list of `{date, type, ticker, description, source}`) — auto + manual.
- `/trading-bot/regime` (String, JSON: `{regime, bias, new_trades_allowed, inputs}`) — set 08:30 ET each weekday.

### 3.4 EventBridge rules (all weekdays, ET; cron expressions are DST-dependent)

**Conservative (3 rules)** — `reconfigure-schedule.ps1`:
- `trading-bot-morning` 08:45 ET
- `trading-bot-midday` 10:30 ET
- `trading-bot-eod` 15:00 ET

**Aggressive (Squeeze-Cross reuses old momentum rules, 8 rules)** — `setup-momentum-schedule.ps1`:
- `trading-bot-mom-regime` 08:30 ET (shared with Aggressive 2.0)
- `trading-bot-mom-premarket` 09:00 ET → `run_squeeze_scan`
- `trading-bot-mom-postopen` 09:45 → `run_squeeze_tick`
- `trading-bot-mom-orb` 10:30 → `run_squeeze_tick`
- `trading-bot-mom-midday` 12:00 → `run_squeeze_tick`
- `trading-bot-mom-afternoon` 13:30 → `run_squeeze_tick`
- `trading-bot-mom-preclose` 15:00 → `run_squeeze_sweep`
- `trading-bot-mom-exit-sweep` every 15 min 10:00–15:45 → `run_squeeze_tick`

**Aggressive 2.0 (6 rules)** — `setup-naked-schedule.ps1`:
- `trading-bot-naked-premarket` 09:00 → `run_naked_premarket`
- `trading-bot-naked-entry-1` 10:00 → `run_naked_entry`
- `trading-bot-naked-entry-2` 12:00 → `run_naked_entry`
- `trading-bot-naked-entry-3` 14:00 → `run_naked_entry`
- `trading-bot-naked-preclose` 15:00 → `run_naked_entry`
- `trading-bot-naked-exit-sweep` every 15 min 10:00–15:45 → `run_naked_exit_sweep`

**Maintenance**:
- `trading-bot-refresh-universe` Sat 23:00 ET (Polygon, Aggressive Layer 2)
- `trading-bot-refresh-earnings` Sun 00:00 ET (Finnhub earnings + econ calendar)
- `trading-bot-reconcile` 08:00 ET weekdays (DDB↔Alpaca sync, all 3 strategies)

**DST handling**: every `setup-*-schedule.ps1` has a `$DST_EDT = $true` toggle at the top. After the November flip, re-run each with `$DST_EDT = $false`. Cron expressions are stored in UTC.

### 3.5 IAM

Role: `trading-bot-lambda-role`. Permissions: read all `/trading-bot/*` SSM (with decrypt), write same subset (added 2026-04-25 via `grant-ssm-write.ps1` so the bot can persist regime/earnings/universe). Full RW on `trading-bot-trades` DDB. SNS publish on the alerts topic. CloudWatch logs.

### 3.6 Deploy workflow

```
powershell -ExecutionPolicy Bypass -File .\deploy.ps1
```

What it does:
1. Clean-create `lambda/package/`.
2. `pip install -r requirements.txt --target lambda/package --platform manylinux2014_x86_64 --implementation cp --python-version 3.12 --only-binary=:all: --upgrade --quiet`. The platform flags are **required** — without them pip pulls Windows wheels and Lambda errors on import (`No module named '_pydantic_core'` was the canonical failure). `--only-binary=:all:` prevents pip from silently falling back to a source build.
3. Copy `bot.py` in.
4. `Compress-Archive` to `lambda/bot.zip`.
5. `aws lambda update-function-code` to push.
6. Total time ~15s.

**Verifying a deploy worked**: a CloudWatch `LATEST` log group entry within ~10s of deploy, OR a manual test invoke via the Lambda console. Best one-liner manual test from PowerShell:
```
aws lambda invoke --function-name trading-bot-runner --payload (echo '{"mode":"squeeze_scan","force":true}' | ConvertTo-Json -Compress) --cli-binary-format raw-in-base64-out --region us-east-1 out.json
```
…or (more reliable on Windows quoting) write the payload to a temp file and `--payload file://path` (see §4.5 PowerShell quoting trap).

### 3.7 Why these choices

- **Lambda + EventBridge + DDB** chosen over EC2 / RDS / cron-on-laptop because Dan wanted "set it and forget it" with zero infrastructure to babysit. Lambda's per-invocation cost is trivial at this volume (a few cents/month).
- **DDB single-table design** because spread/trade/note volumes are tiny and on-demand billing is essentially free.
- **Single-file `bot.py`** because the deploy is just zip+push. A multi-module layout would require an actual build pipeline and the ROI isn't there.
- **Claude Haiku 4.5** over Sonnet/Opus because Haiku is 5–10× cheaper and the decisions here are short (BUY/HOLD/SKIP + score + one sentence). The whole bot's Anthropic spend is ~$5–$15/month.
- **PowerShell, not bash**: Dan is on Windows and not technical enough to want WSL. All scripts assume `powershell.exe`. **ASCII-only inside `.ps1` files** (em-dashes break PowerShell 5.1 parsing — see §4.2).
- **Strategy code names kept frozen**: changing `credit_spread` / `momentum` / `naked` strings would invalidate every SSM key path, DDB prefix, and EventBridge target payload. We rename in the UI, not in the code.
- **Old Aggressive code retained as dead code**: cheaper insurance than deleting and regretting. The dispatcher routes everything past it.

---

## 4. Critical Edge Cases & Solved Issues

### 4.1 Phantom positions (the NVDA bug, 2026-05-13)

**Symptom**: NVDA Aggressive 2.0 call hit +91% gain. Bot tried to close every 15 min for hours. Alpaca returned `422 position intent mismatch, inferred: sell_to_open, specified: sell_to_close`. Email inbox flooded with retry errors. Dan asked why the +100% profit "didn't fire to sell."

**Root cause**: The open path called `_wait_for_fill()` which timed out and returned `None`, but the code fell through to the limit price and wrote the DDB `SPREAD#OPEN` row anyway. Alpaca never actually held NVDA. Every subsequent 15-min sweep loaded the phantom row, tried to close it, hit the mismatch error, didn't archive, and retried forever.

**Three-part fix landed before the Squeeze-Cross rebuild**:
1. **Fill verification before DDB write** (in `_execute_squeeze_open`, `_execute_naked_open`, `_execute_open`): only `save_open_spread()` AFTER `_wait_for_fill` / `_stalk_squeeze_fill` returns a real fill price. On failure, cancel the order, log an error, return `None`. No DDB row, no phantom.
2. **Phantom auto-archive on close**: `_close_naked_position` catches Alpaca errors containing `"position intent mismatch"` OR `"inferred: sell_to_open"` and immediately archives the spread with `exit_reason: "phantom position - Alpaca had no holding (auto-archived)"`. Stops the retry loop on the first occurrence. Same pattern can be ported to `_close_squeeze_tranche` and `_close_spread` if it ever happens there.
3. **Daily reconciliation mode** (`run_reconciliation` + `trading-bot-reconcile` 08:00 ET rule): full reconciliation regardless of error path. Catches phantoms even if the close path never runs.
4. **Accurate counter**: prior `closed: N` in sweep returns was inflated by counting phantom-archives. Now only successful fills increment it.

**One-off cleanup**: `cleanup-naked-spam.ps1` deleted the day's STRAT3 error rows from DDB. Used the temp-file `file://` AWS CLI workaround because PowerShell strips inner double-quotes from JSON args.

**Avoid re-introducing**: any new open path MUST verify fill before writing DDB. Any new close path MUST handle the position-intent-mismatch error. Any new strategy MUST be added to `run_reconciliation` so reconciliation covers it.

### 4.2 PowerShell ASCII-only requirement

PowerShell 5.1 on certain Windows code pages misparses UTF-8 em-dashes (—), en-dashes (–), and box-drawing characters as Windows-1252, producing cryptic `"string is missing the terminator"` errors. **Rule**: use only plain ASCII inside `.ps1` files — `-` not `—`, plain section comments not box-drawn dividers, no bullet glyphs. Both `setup.ps1` and `deploy.ps1` already comply. If you edit a `.ps1` via the `Edit` tool and Unicode sneaks in, fully rewrite the file with `Write` rather than patching.

### 4.3 Linux-wheel pip flags

Lambda is Linux x86_64. Without `--platform manylinux2014_x86_64 --implementation cp --python-version 3.12 --only-binary=:all:`, pip pulls Windows `.pyd` wheels that Lambda can't load, producing import-time errors like `No module named 'pydantic_core._pydantic_core'`. The flags are already in `deploy.ps1` and `setup.ps1`. **Never strip them.** If a new dep gets added to `requirements.txt`, confirm `lambda/package/` contains `.so` files (Linux) and not `.pyd` files (Windows) for any native deps before deploying.

### 4.4 Alpaca bars endpoint needs `start`/`end`

`/v2/stocks/bars` returns `{"bars": {"SPY": []}}` (200 OK, empty) if you pass only `{symbols, timeframe, limit}`. `limit` does NOT anchor the time range. Always include explicit `start` and `end` ISO date strings. A 120-day window is the safe default for daily bars when you need ~30–40 closes for indicators. Add `adjustment=split` so prior closes stay consistent across stock splits. Same trap on `/v2/options/bars`. The current `get_daily_bars`, `get_15min_bars`, and `get_intraday_bars` already do this — keep it that way.

### 4.5 PowerShell strips inner double-quotes from AWS CLI args

When passing JSON to an AWS CLI command, PowerShell mangles `--filter-expression "{...}"` into garbage. The workaround used throughout the project is to **write the JSON to a temp file and reference it with `file://`**:

```
$expr = '{":pk":{"S":"' + $pk + '"}}'
[System.IO.File]::WriteAllText("$env:TEMP\q.json", $expr)
aws dynamodb query --expression-attribute-values "file://$env:TEMP/q.json" ...
```

Used in `cleanup-naked-spam.ps1`, `update-catalyst-calendar.ps1`, and the EventBridge `put-targets --targets` calls in every `setup-*-schedule.ps1`. **When generating any new PowerShell that hands JSON to AWS CLI, use the same pattern.**

### 4.6 Dispatcher coverage for unknown modes

Each per-strategy branch in `handler` falls back to a default function when the `mode` is unrecognized (`squeeze_tick`, `naked_check`, `midday`), with a `print` line so we can find the typo in CloudWatch. Don't remove these fallbacks — they've already caught two EventBridge payload typos silently.

### 4.7 Market-closed guard with `force` bypass

For modes that aren't pre-market, the handler short-circuits when Alpaca says the market is closed. A manual `{"force": true}` payload bypasses this for testing (Alpaca itself will still reject the actual order). Use this for weekend tests — exactly what we did to verify the Squeeze-Cross deploy returns empty-state shapes on a closed market.

### 4.8 Old Aggressive (momentum debit-spread) functions are dead code

They remain in `bot.py` after the Squeeze-Cross rebuild specifically so we can revert by changing the dispatcher. Don't delete them. If you need to refactor, leave a stub or move them to a `_legacy/` block — but the safest move is leaving them where they are.

---

## 5. Exact Current State & Active Tasks

### 5.1 What works right now (as of 2026-05-25)

- **Conservative**: live, has run morning/midday/eod for ~5 weeks. Has opened and closed several credit spreads cleanly.
- **Aggressive (Squeeze-Cross)**: live since 2026-05-12. All scheduled modes routing to Squeeze functions.
- **Aggressive 2.0 (Naked)**: live. Phantom-position bug fixed. Catalyst calendar refreshing weekly.
- **Daily Suggestion** (4th strategy, `STRATEGY_SUGGESTION = "suggestion"`): **fully live as of 2026-05-25**. Sends one high-conviction naked call/put to Dan's Telegram every trading day at 9:45 AM ET. NOT automated — informational only. See `memory/project_suggestion.md` for full detail.
- **Daily reconciliation**: `trading-bot-reconcile` scheduled 08:00 ET. Running.
- **Dashboard**: 3 tabs working. Period Performance card, closed-trades bar chart.
- **Email alerts**: unified `send_position_alert()` live across all 3 trading strategies.

### 5.2 Daily Suggestion system — key facts for any new session

- **Schedule (ET weekdays)**: 9:45 AM scan, 12:00 PM P&L check, 12:30 PM recap, 3:00 PM P&L check
- **Telegram card headline**: `TICKER $STRIKEC/P MMM DD EXP` (e.g. `IONQ $67C May 29 EXP`)
- **Shows top 3**: winner + 2 runners-up in card
- **Reuses Conservative Alpaca account** (read-only, no 4th account)
- **4 EventBridge rules**: trading-bot-suggestion-morning/midday/recap/preclose
- **Critical handler ordering**: `force` computed first, `daily_suggestion` dispatch second, `STRATEGY_SUGGESTION` recap/pnl dispatch third (before market gate). Do not reorder.
- **SSM payload is slim by design** (~981 chars) — full pick dict exceeds 4096-char limit. runners_up not saved to SSM.
- See `memory/project_suggestion.md` for full bug history and design decisions.

### 5.3 Active tasks (carried forward)

- **Nothing blocking**. All systems live and stable as of 2026-05-25.
- Headroom/prompt-slimming discussion was parked (cost savings too small to justify new dependency).
- Aggressive paper account reset to $10k is a manual Alpaca UI step Dan can do when convenient.

---

## 6. Future Roadmap — Immediate Next 3 Steps

In execution order:

### Step 1 — Confirm the Aggressive paper account reset to $10k.

The Squeeze-Cross sizing math (`SQUEEZE_POSITION_PCT = 0.20`, 20% of current Alpaca equity per trade) assumes a fresh $10k balance. The new code pulls equity live from Alpaca, so it'll work at whatever the account holds — but Dan wants this to be the head-to-head test against the Gemini-spec idea. Verify by checking `aws ssm get-parameter` for the keys, then hitting `/v2/account` with those keys to confirm `equity ≈ 10000`. If it's not, prompt Dan to reset on the Alpaca UI.

### Step 2 — Watch the first Monday open after deploy.

The Squeeze-Cross logic has never fired on real bars. Watch CloudWatch logs at:
- 09:00 ET (`run_squeeze_scan` — should return `candidates: N` with any L1 passers and Claude scores).
- 09:45 ET (`run_squeeze_tick` — first live tick).
- Every 15 min after — confirm no `[fatal]` exceptions.

When the first position opens, verify:
- The `[Aggressive] OPEN <ticker> bullish $<strike>C x<qty> @ $<premium>` email arrives.
- The DDB `STRAT2#SPREAD#OPEN` row has `kind: "squeeze"`, the correct `qty_original`, and the right `tp1_value` / `tp2_value` / `stop_value`.
- The dashboard's Aggressive tab renders the position with the new subtitle "TTM Squeeze + EMA cross · 15-min intraday".

If TP1 hits during the day, verify the `[Aggressive] TP1 <ticker> closed Nc +$X` email AND that the DDB row was mutated (`qty` decremented, `tp1_hit: true`, stop moved to breakeven).

### Step 3 — Resolve the Headroom-vs-manual-prompt-slimming decision.

Re-surface the choice from §5.2 to Dan in the new session. If he picks manual slimming, apply these targeted edits in `bot.py`:
- `claude_squeeze_eval` (≈ line 1393): cap `news` to 3 headlines (currently 5) and pass titles only, not full body.
- `claude_naked_scan` (≈ line 3976): cap each ticker summary to top-3 catalysts, drop the full `description` field, keep `type + ticker + days_out`.
- `claude_momentum_premarket` / `claude_momentum_midday`: pass `score_breakdown` only for borderline candidates (score 6–7), not all of them.
- Add a `_log_prompt_tokens(prompt: str, label: str)` helper that prints `len(prompt)` to CloudWatch so we can measure the actual savings.

After these edits land, redeploy via `deploy.ps1` and compare CloudWatch token logs for one trading day vs. the prior day.

---

## Reference card (cheat sheet for any new Claude session)

- **Deploy**: `powershell -ExecutionPolicy Bypass -File .\deploy.ps1`
- **Deploy dashboard**: `powershell -ExecutionPolicy Bypass -File .\deploy-dashboard.ps1`
- **Manual invoke for testing**:
  ```
  aws lambda invoke --function-name trading-bot-runner ^
      --payload (echo '{"mode":"squeeze_scan","force":true}' | ConvertTo-Json -Compress) ^
      --cli-binary-format raw-in-base64-out --region us-east-1 out.json
  ```
- **Tail CloudWatch**: `aws logs tail /aws/lambda/trading-bot-runner --follow --region us-east-1`
- **Check open spreads**:
  ```
  aws dynamodb query --table-name trading-bot-trades ^
      --key-condition-expression "pk = :pk" ^
      --expression-attribute-values "file://%TEMP%\q.json" ^
      --region us-east-1
  ```
  where `q.json` is `{":pk":{"S":"STRAT2#SPREAD#OPEN"}}` (or `SPREAD#OPEN` for Conservative, `STRAT3#SPREAD#OPEN` for Aggressive 2.0).
- **One-line ground rules**:
  - Use Conservative / Aggressive / Aggressive 2.0 in UI + emails, never the code names.
  - Verify fill before writing DDB on any new open path.
  - PowerShell files ASCII-only; AWS CLI JSON args via `file://` tempfiles.
  - Always include `start`/`end` on Alpaca `/v2/stocks/bars`.
  - Don't delete dead Aggressive momentum-debit code — it's the revert path.
  - Ask Dan before deploying live trading changes; he's non-technical and wants one-step-at-a-time explanations.
