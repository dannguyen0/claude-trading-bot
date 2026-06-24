"""
AI Trading Bot - Lambda Handler (put credit spreads on SPY/QQQ/IWM)

Three scheduled modes driven by EventBridge event {"mode": ...}:
  - morning (08:45 ET): pull VIX + daily indicators, have Claude write a
    one-paragraph market context note to DynamoDB. No trades.
  - midday  (10:30 ET): scan 30-45 DTE put chains on SPY/QQQ/IWM, have Claude
    pick the spread(s) to open. Auto-execute multi-leg orders and immediately
    place GTC profit-take (close at 50% of credit). Email confirmations via SNS.
  - eod     (15:00 ET): sweep open spreads for 2x-credit stop-loss and <=7 DTE
    time exit. Close qualifying spreads, cancel their profit-take GTCs.

Also serves GET endpoints via Lambda Function URL for the dashboard:
  - ?action=trades  -> recent trade/hold events
  - ?action=spreads -> currently open spreads with live marks
  - ?action=notes   -> latest morning context notes
"""

import json
import os
import re
import uuid
import time
import math
import traceback
import boto3
import anthropic
import requests
from decimal import Decimal
from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _to_ddb(obj):
    """Recursively convert floats to Decimal so boto3's DynamoDB resource
    client will accept them. Using str() for the float->Decimal conversion
    avoids binary-float precision artifacts like 0.1 -> 0.10000000000000000555.
    """
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return "0"
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _to_ddb(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_ddb(v) for v in obj]
    return obj

# ---- Config ----------------------------------------------------------------
UNIVERSE            = ["SPY", "QQQ", "IWM"]
ACCOUNT_RISK_CAP    = 2500.00     # dollars; max total at-risk across open spreads
MAX_SPREADS         = 5           # no more than 5 concurrent spreads
SPREAD_WIDTH        = 5.00        # dollars between short and long strikes
MIN_CREDIT          = 0.50        # per-spread minimum credit to accept
SHORT_DELTA_LO      = 0.16        # short-leg delta band (put -> abs value)
SHORT_DELTA_HI      = 0.20
DTE_MIN             = 30          # days to expiry at entry
DTE_MAX             = 45
PROFIT_TAKE_PCT     = 0.50        # close at 50% of credit captured
LOSS_STOP_MULT      = 2.00        # close if debit >= 2x credit received
TIME_EXIT_DTE       = 7           # close on or before this DTE
MAX_PER_UNDERLYING  = 2           # Conservative: cap open spreads per ticker (prevents 5-stack concentration)

# Strategy 2 v3 (Squeeze-Cross — replaces momentum debit spreads on the
# same account / DDB prefix / EventBridge schedule. Internal name stays
# 'momentum' so existing plumbing keeps working.)
SQUEEZE_UNIVERSE         = [
    # Core anchors -- always scanned, proven intraday liquidity + options
    "NVDA", "TSLA", "MSTR", "AMD",  "SPY",  "QQQ",
    "COIN", "META", "PLTR", "AVGO", "AMZN", "GOOGL",
    "ARM",  "APP",  "HOOD",
]
SQUEEZE_WATCHLIST_META   = {
    "NVDA": "Momentum",    "TSLA": "Sentiment",   "MSTR": "Volatility",
    "AMD":  "Momentum",    "SPY":  "Macro",        "QQQ":  "Macro",
    "COIN": "Crypto-Beta", "META": "Trend",        "PLTR": "Momentum",
    "AVGO": "Momentum",    "AMZN": "Trend",        "GOOGL":"Trend",
    "ARM":  "Momentum",    "APP":  "Momentum",     "HOOD": "Crypto-Beta",
}
SQUEEZE_POSITION_PCT     = 0.20     # 20% of current equity per trade
SQUEEZE_DELTA_MIN        = 0.55
SQUEEZE_DELTA_MAX        = 0.65
SQUEEZE_DTE_MIN          = 3
SQUEEZE_DTE_MAX          = 14
SQUEEZE_SCORE_MIN        = 7        # Claude must return >= 7 to enter
SQUEEZE_MAX_POSITIONS    = 5        # hard cap
SQUEEZE_STOP_PCT         = 0.20     # -20% premium stop
SQUEEZE_TP1_GAIN_PCT     = 0.30     # +30% trigger
SQUEEZE_TP1_SCALE_PCT    = 0.50     # close 50% of original qty
SQUEEZE_TP2_GAIN_PCT     = 0.60     # +60% trigger
SQUEEZE_TP2_SCALE_PCT    = 0.25     # close 25% of original qty
# Remaining 25% trails on 9 EMA close (1-min) via the exit sweep

# Strategy 2 (Aggressive: momentum debit spreads) — v2.0 score-tiered limits
MOMENTUM_MAX_POSITIONS      = 5         # max concurrent positions
MOMENTUM_TOTAL_RISK_CAP     = 500.00    # 5 stops x $100 = real exposure across 5 spreads
MOMENTUM_DTE_MIN            = 7         # 7 only for very strong setups
MOMENTUM_DTE_MAX            = 21        # primary band 14-21
MOMENTUM_TARGET_DELTA_SHORT = 0.30      # short leg target delta
MOMENTUM_DELTA_TOLERANCE    = 0.10      # accept short delta in [0.20, 0.40]
MOMENTUM_REWARD_RISK_MIN    = 2.0       # max_gain must be >= 2 x debit
MOMENTUM_BREAKEVEN_ATR_MAX  = 1.5       # required move <= 1.5x 14-day ATR
MOMENTUM_TIME_EXIT_DTE      = 5         # close at <=5 DTE if not at +50%
MOMENTUM_EARNINGS_MIN_DAYS    = 2       # absolute floor: refuse if earnings is today or tomorrow
MOMENTUM_EARNINGS_BUFFER_DAYS = 21      # legacy var; only the MIN_DAYS check is enforced now,
                                        # per-spread DTE gets capped to expire BEFORE earnings instead

# Score-to-size + score-to-stop tiers (v2.0 0-10 scoring)
MOMENTUM_SCORE_MAX           = 10
MOMENTUM_SCORE_MIN_ENTRY     = 6        # below 6 = watch only / skip
MOMENTUM_SCORE_TIER_HIGH     = 8        # 8-10 = high conviction
MOMENTUM_DEBIT_HIGH          = 250.00   # for score 8-10
MOMENTUM_DEBIT_MID           = 150.00   # for score 6-7
MOMENTUM_STOP_HIGH_PCT       = 0.40     # -40% on score 8-10
MOMENTUM_STOP_MID_PCT        = 0.30     # -30% on score 6-7
MOMENTUM_GAIN_TAKE_PCT       = 1.00     # +100% = full close (paper-trade scale-out simplification)
MOMENTUM_GAIN_TIME_HIGH_PCT  = 0.50     # +50% with DTE<=10 close (score 8-10)
MOMENTUM_GAIN_TIME_MID_PCT   = 0.40     # +40% with DTE<=10 close (score 6-7)

def momentum_size_for_score(score: int) -> float:
    """Max debit allowed at this score. Returns 0 for sub-entry scores."""
    if score is None:
        return MOMENTUM_DEBIT_HIGH  # legacy rows without score: assume high tier
    s = int(score)
    if s >= MOMENTUM_SCORE_TIER_HIGH:
        return MOMENTUM_DEBIT_HIGH
    if s >= MOMENTUM_SCORE_MIN_ENTRY:
        return MOMENTUM_DEBIT_MID
    return 0.0

def momentum_stop_for_score(score: int) -> float:
    """Stop-loss percentage for this score (positive number)."""
    if score is not None and int(score) >= MOMENTUM_SCORE_TIER_HIGH:
        return MOMENTUM_STOP_HIGH_PCT
    return MOMENTUM_STOP_MID_PCT

def momentum_time_gain_for_score(score: int) -> float:
    """Profit target that triggers a close-at-DTE<=10 for this score."""
    if score is not None and int(score) >= MOMENTUM_SCORE_TIER_HIGH:
        return MOMENTUM_GAIN_TIME_HIGH_PCT
    return MOMENTUM_GAIN_TIME_MID_PCT

# --- Strategy 3 (Aggressive 2.0 — naked single-leg options) -----------------
# Catalyst-driven directional plays. Buy single calls or puts on high-conviction
# event setups. No spread, no hedge — pure directional with defined max loss
# (the premium paid). Stops scale by DTE because theta accelerates as expiry
# nears. See Strategy 3 spec for full rationale.

NAKED_MAX_POSITIONS         = 4
NAKED_TOTAL_RISK_CAP        = 4000.00      # 4 positions x ~$1k each
NAKED_DTE_MIN               = 0
NAKED_DTE_MAX               = 28
NAKED_DEBIT_HIGH            = 1000.00      # score 8-10 (full size)
NAKED_DEBIT_MID             = 500.00       # score 6-7  (half size)
NAKED_SCORE_MAX             = 10
NAKED_SCORE_MIN_ENTRY       = 6
NAKED_SCORE_TIER_HIGH       = 8
NAKED_DELTA_MIN             = 0.25         # avoid lottery tickets
NAKED_DELTA_MAX             = 0.60         # avoid deep ITM
NAKED_AWR_BREAKEVEN_MAX     = 0.90         # breakeven move <= 90% of 8-week AWR (relaxed for high-IV)
NAKED_BIDASK_MAX_PCT        = 0.20         # bid-ask spread <= 20% of mid
# Moneyness band used as delta proxy when Alpaca returns null greeks (paper indicative feed)
NAKED_PROXY_OTM_MIN         = 0.00        # 0% OTM  (ATM) -> delta ~0.50
NAKED_PROXY_OTM_MAX         = 0.18        # 18% OTM      -> delta ~0.25

# DTE-tiered stop loss percentages (positive numbers = % of premium paid)
NAKED_STOP_0DTE_PCT         = 0.30
NAKED_STOP_1_3DTE_PCT       = 0.35
NAKED_STOP_4_7DTE_PCT       = 0.40
NAKED_STOP_8_14DTE_PCT      = 0.42
NAKED_STOP_15_28DTE_PCT     = 0.45
NAKED_STOP_FLOOR_PCT        = 0.60         # absolute floor regardless of DTE

# Universe — Layer 1 always scanned, Layer 2 only with active catalyst
NAKED_UNIVERSE_LAYER1         = ["SPY", "QQQ", "NVDA", "TSLA", "AAPL"]
NAKED_UNIVERSE_LAYER2_DEFAULT = ["AMD", "META", "GOOGL", "MRVL", "AMZN", "MSFT", "COIN", "PLTR", "SMCI"]

def naked_size_for_score(score) -> float:
    """Max premium spend per trade by score tier. Accepts float scores (e.g. 8.2)."""
    if score is None:
        return NAKED_DEBIT_HIGH
    s = float(score)
    if s >= NAKED_SCORE_TIER_HIGH:
        return NAKED_DEBIT_HIGH
    if s >= NAKED_SCORE_MIN_ENTRY:
        return NAKED_DEBIT_MID
    return 0.0

def naked_stop_for_dte(dte: int) -> float:
    """DTE-tiered stop-loss as fraction of premium paid (e.g. 0.30 = -30%)."""
    if dte is None:
        return NAKED_STOP_8_14DTE_PCT
    d = int(dte)
    if d <= 0:  return NAKED_STOP_0DTE_PCT
    if d <= 3:  return NAKED_STOP_1_3DTE_PCT
    if d <= 7:  return NAKED_STOP_4_7DTE_PCT
    if d <= 14: return NAKED_STOP_8_14DTE_PCT
    return NAKED_STOP_15_28DTE_PCT

def get_naked_universe() -> list[str]:
    """Dynamic universe: StockTwits trending + Alpaca movers + catalyst anchors.
    Replaces the old fixed Layer1/Layer2 lists. Returns up to 30 unique tickers."""
    return get_weeklies_dynamic_universe()

def get_weeklies_dynamic_universe(max_tickers: int = 30) -> list[str]:
    """Build today's scan universe from three sources:
      1. StockTwits trending symbols
      2. Alpaca most-active movers (broad scan universe)
      3. Catalyst anchors: tickers with active earnings/macro events in next 14 days
    Deduplicates and caps at max_tickers. Always includes SPY/QQQ as macro anchors.
    Falls back to the old fixed lists if all discovery sources fail."""
    seen: set = set()
    result: list = []

    def _add(ticker: str) -> None:
        t = ticker.upper().strip()
        # Reject futures (_), crypto (.X), blank, too long, or non-alpha
        if (t and t not in seen and len(t) <= 5
                and t.isalpha()          # letters only — no underscores, dots, digits
                and len(t) >= 2):        # skip single-letter edge cases
            seen.add(t); result.append(t)

    # 1. StockTwits trending (free, no auth)
    try:
        st_tickers = get_stocktwits_trending(top=20)
        for item in st_tickers:
            _add(item.get("ticker", ""))
    except Exception as e:
        print(f"  [weeklies-universe] stocktwits warn: {e}")

    # 2. Alpaca most-actives screener
    try:
        movers = get_top_movers(top_n=25)
        for item in movers:
            _add(item.get("symbol", ""))
    except Exception as e:
        print(f"  [weeklies-universe] movers warn: {e}")

    # 3. Catalyst anchors — tickers with active events in next 14 days
    try:
        cats = get_active_catalysts(lookforward_days=14)
        for c in cats:
            t = c.get("ticker", "SPY")
            if t:
                _add(t)
    except Exception as e:
        print(f"  [weeklies-universe] catalysts warn: {e}")

    # Always include macro anchors
    for anchor in ["SPY", "QQQ", "NVDA", "TSLA", "AAPL"]:
        _add(anchor)

    # Fallback to old fixed lists if discovery returned nothing
    if len(result) < 5:
        print("  [weeklies-universe] discovery empty, using fixed fallback")
        for t in NAKED_UNIVERSE_LAYER1 + NAKED_UNIVERSE_LAYER2_DEFAULT:
            _add(t)

    universe = result[:max_tickers]
    print(f"  [weeklies-universe] {len(universe)} tickers: {universe[:10]}{'...' if len(universe)>10 else ''}")
    return universe

# --- Weeklies self-learning config -------------------------------------------
# These defaults mirror the hardcoded constants above. The reflection loop reads
# the live values from SSM (/trading-bot/weeklies-strategy-config) and overwrites
# one variable per cycle. Falls back to these defaults if SSM param is absent.
WEEKLIES_CONFIG_SSM  = "/trading-bot/weeklies-strategy-config"
WEEKLIES_REFLECT_SSM = "/trading-bot/weeklies-reflect-state"  # last reflect metadata
WEEKLIES_REFLECT_EVERY = 1   # kept for in-sweep backup; primary trigger is the daily EOD rule

WEEKLIES_CONFIG_DEFAULTS = {
    "version":          "v01",
    "delta_min":        0.30,
    "delta_max":        0.55,
    "score_threshold":  6,
    "score_high":       8,
    "dte_max":          28,
    "dte_min":          0,
    "stop_floor_pct":   0.60,
    "max_positions":    4,
    "debit_high":       1000.0,
    "debit_mid":        500.0,
    "universe_size":    30,
    "ba_max_pct":       0.10,
}

def get_weeklies_strategy_config() -> dict:
    """Read the mutable Weeklies strategy config from SSM.
    Falls back to WEEKLIES_CONFIG_DEFAULTS if the param is missing or malformed.
    Always merges with defaults so missing keys don't cause KeyErrors."""
    try:
        raw = ssm.get_parameter(Name=WEEKLIES_CONFIG_SSM, WithDecryption=False)
        loaded = json.loads(raw["Parameter"]["Value"])
        merged = {**WEEKLIES_CONFIG_DEFAULTS, **loaded}
        return merged
    except ssm.exceptions.ParameterNotFound:
        return dict(WEEKLIES_CONFIG_DEFAULTS)
    except Exception as e:
        print(f"  [weeklies-config-warn] {type(e).__name__}: {e} -- using defaults")
        return dict(WEEKLIES_CONFIG_DEFAULTS)

def _save_weeklies_strategy_config(config: dict) -> None:
    """Persist updated strategy config to SSM."""
    ssm.put_parameter(
        Name=WEEKLIES_CONFIG_SSM,
        Value=json.dumps(config, default=str),
        Type="String",
        Overwrite=True,
    )

# --- Daily Suggestion System (informational only, no orders placed) ----------
# Runs 08:15 ET. Discovers trending tickers via StockTwits + Reddit WSB +
# broad movers scan. Scores candidates with Claude Haiku. Sends one
# high-conviction naked call/put play via Telegram + SNS email. The user
# manages the trade manually. P&L check alerts at 12:00 and 15:00 ET.

SUGGESTION_BUDGET          = 500.00
SUGGESTION_SCORE_MIN       = 6.0
SUGGESTION_DELTA_MIN       = 0.35
SUGGESTION_DELTA_MAX       = 0.55
SUGGESTION_DTE_MIN         = 7
SUGGESTION_DTE_MAX         = 21
SUGGESTION_STOP_PCT        = 0.20
SUGGESTION_TARGET_PCT      = 0.20
SUGGESTION_BIDASK_MAX_PCT  = 0.12
SUGGESTION_PNL_ALERT_UP1   = 10.0
SUGGESTION_PNL_ALERT_UP2   = 20.0
SUGGESTION_PNL_ALERT_DOWN  = -15.0
SUGGESTION_REFLECT_EVERY   = 3     # archive 3 suggestions then reflect
SUGGESTION_REFLECT_SSM     = "/trading-bot/suggestion-reflect-state"
SUGGESTION_CONFIG_SSM      = "/trading-bot/suggestion-strategy-config"

SUGGESTION_SCAN_UNIVERSE = [
    "NVDA", "AMD", "AVGO", "ARM", "SMCI", "MU", "QCOM", "MRVL",
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "NFLX",
    "TSLA", "PLTR", "COIN", "MSTR", "HOOD", "APP", "RDDT",
    "SPY", "QQQ", "IWM", "SMH", "XLF",
    "JPM", "GS", "LLY", "MRNA", "NKE", "DIS", "GME",
]

# --- Catalyst calendar (manual macro events + auto earnings) ----------------
# Macro events are maintained as a JSON list in SSM /trading-bot/catalyst-calendar.
# Schema: [{"date":"YYYY-MM-DD", "type":"FOMC|CPI|PPI|NFP|FDA|other",
#           "ticker":"<optional, defaults to SPY for macro>", "description":"..."}]
# Update via:
#   $cal = '[{"date":"2026-05-13","type":"CPI","ticker":"SPY","description":"April CPI release 8:30 ET"}]'
#   aws ssm put-parameter --name /trading-bot/catalyst-calendar --type String --overwrite --value $cal --region us-east-1

def get_macro_catalysts() -> list[dict]:
    """Read manually-maintained macro catalyst calendar from SSM. Bypasses
    the in-process cache because Dan updates this Sunday-night and we want
    every Lambda invocation to see the latest, even on warm containers."""
    try:
        resp = ssm.get_parameter(Name="/trading-bot/catalyst-calendar", WithDecryption=False)
        raw = resp["Parameter"]["Value"]
        loaded = json.loads(raw)
        if isinstance(loaded, list):
            return loaded
        print(f"  [catalyst-cal-warn] expected JSON list, got {type(loaded).__name__}")
    except Exception as e:
        print(f"  [catalyst-cal-warn] {type(e).__name__}: {e}")
    return []

def get_active_catalysts(target: "date | None" = None, lookforward_days: int = 28) -> list[dict]:
    """Combined macro + earnings catalysts active in the window [today .. today+N].
    Returns list of {date, type, ticker, description, days_out} sorted by days_out."""
    today = target or date.today()
    horizon = today + timedelta(days=lookforward_days)
    out: list[dict] = []

    # Macro events from SSM
    for item in get_macro_catalysts():
        try:
            d = date.fromisoformat(str(item.get("date") or ""))
        except Exception:
            continue
        if today <= d <= horizon:
            out.append({
                "date":        d.isoformat(),
                "type":        str(item.get("type") or "macro").upper(),
                "ticker":      str(item.get("ticker") or "SPY").upper(),
                "description": item.get("description") or "",
                "days_out":    (d - today).days,
            })

    # Earnings from the existing /trading-bot/earnings-calendar (Finnhub-fed)
    earn_map = get_earnings_dates()  # {ticker: 'YYYY-MM-DD'}
    for ticker, date_str in earn_map.items():
        try:
            d = date.fromisoformat(str(date_str))
        except Exception:
            continue
        if today <= d <= horizon:
            out.append({
                "date":        d.isoformat(),
                "type":        "EARNINGS",
                "ticker":      ticker,
                "description": f"{ticker} earnings",
                "days_out":    (d - today).days,
            })

    out.sort(key=lambda x: x["days_out"])
    return out

def compute_awr(symbol: str, weeks: int = 8) -> float | None:
    """8-week Average Weekly Range. Aggregates the bot's daily bars into
    weekly buckets (Mon-Fri), takes max(high) - min(low) per week, averages.
    Returns None if not enough data."""
    bars = get_daily_bars(symbol, limit=80)  # ~16 weeks of trading days
    if not bars or len(bars) < 30:
        return None
    # Group bars by ISO week
    by_week: dict = {}
    for b in bars:
        try:
            bd = date.fromisoformat(str(b.get("t") or "")[:10])
        except Exception:
            continue
        wk = bd.isocalendar()[:2]  # (year, week-num)
        slot = by_week.setdefault(wk, {"high": float("-inf"), "low": float("inf")})
        h = float(b.get("h") or 0)
        l = float(b.get("l") or 0)
        if h > slot["high"]: slot["high"] = h
        if l > 0 and l < slot["low"]: slot["low"] = l
    # Sort weeks descending and take last N completed
    valid_weeks = [v for v in by_week.values() if v["high"] > 0 and v["low"] != float("inf")]
    if len(valid_weeks) < weeks:
        weeks = len(valid_weeks)
    if weeks <= 0:
        return None
    # Most recent N weeks
    sorted_keys = sorted(by_week.keys(), reverse=True)
    recent = [by_week[k] for k in sorted_keys[:weeks] if by_week[k]["high"] > 0 and by_week[k]["low"] != float("inf")]
    if not recent:
        return None
    ranges = [w["high"] - w["low"] for w in recent]
    return round(sum(ranges) / len(ranges), 2)
ALPACA_BASE         = "https://paper-api.alpaca.markets"
ALPACA_DATA_BASE    = "https://data.alpaca.markets"
HTTP_TIMEOUT        = (5, 20)     # (connect, read) seconds
ET                  = ZoneInfo("America/New_York")

# ---- HTTP session with retries ---------------------------------------------
_retry = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET", "POST", "DELETE", "PATCH"),
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=10)
http = requests.Session()
http.mount("https://", _adapter)
http.mount("http://", _adapter)

# ---- AWS clients -----------------------------------------------------------
dynamo    = boto3.resource("dynamodb")
trade_tbl = dynamo.Table(os.environ["DYNAMO_TABLE"])
ssm       = boto3.client("ssm")
sns       = boto3.client("sns")
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN", "")

# Cache secrets for the life of the Lambda container (saves ~50ms/call)
_secret_cache: dict[str, str] = {}

def get_secret(name: str) -> str:
    if name in _secret_cache:
        return _secret_cache[name]
    resp = ssm.get_parameter(Name=name, WithDecryption=True)
    val = resp["Parameter"]["Value"]
    _secret_cache[name] = val
    return val

# ---- Strategy dispatch -----------------------------------------------------
# Each scheduled invoke (and dashboard fetch) picks one strategy to act under.
# alpaca_headers() reads this so the right paper account's keys are used.
# Lambda is single-threaded per container, so a module-level flag is safe.
STRATEGY_CREDIT_SPREAD = "credit_spread"
STRATEGY_MOMENTUM      = "momentum"
STRATEGY_NAKED         = "naked"            # S3 — Aggressive 2.0, single-leg directional
STRATEGY_SUGGESTION    = "suggestion"       # Daily Options Play (informational only, no orders)
_ACTIVE_STRATEGY: str  = STRATEGY_CREDIT_SPREAD

_STRATEGY_KEY_PATHS = {
    STRATEGY_CREDIT_SPREAD: ("/trading-bot/alpaca-key",             "/trading-bot/alpaca-secret"),
    STRATEGY_MOMENTUM:      ("/trading-bot/strategy2/alpaca-key",   "/trading-bot/strategy2/alpaca-secret"),
    STRATEGY_NAKED:         ("/trading-bot/strategy3/alpaca-key",   "/trading-bot/strategy3/alpaca-secret"),
    STRATEGY_SUGGESTION:    ("/trading-bot/alpaca-key",             "/trading-bot/alpaca-secret"),
}

def set_active_strategy(name: str) -> str:
    """Set the strategy in effect for this invocation. Returns the resolved name."""
    global _ACTIVE_STRATEGY
    s = (name or STRATEGY_CREDIT_SPREAD).lower().strip()
    if s not in _STRATEGY_KEY_PATHS:
        print(f"  [strategy-warn] unknown strategy '{s}', defaulting to credit_spread")
        s = STRATEGY_CREDIT_SPREAD
    _ACTIVE_STRATEGY = s
    return s

# ---- Alpaca helpers --------------------------------------------------------

def alpaca_headers() -> dict:
    key_path, secret_path = _STRATEGY_KEY_PATHS[_ACTIVE_STRATEGY]
    return {
        "APCA-API-KEY-ID":     get_secret(key_path),
        "APCA-API-SECRET-KEY": get_secret(secret_path),
        "Content-Type": "application/json",
    }

def get_account() -> dict:
    r = http.get(f"{ALPACA_BASE}/v2/account", headers=alpaca_headers(), timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def get_positions() -> list[dict]:
    r = http.get(f"{ALPACA_BASE}/v2/positions", headers=alpaca_headers(), timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def market_is_open() -> bool:
    r = http.get(f"{ALPACA_BASE}/v2/clock", headers=alpaca_headers(), timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()["is_open"]

def get_15min_bars(symbol: str, days_back: int = 5) -> list[dict]:
    """15-minute bars over the last N market days. Squeeze evaluation uses
    ~22-30 bars of history (~1.5 days of session bars) for a 20-period BB/KC."""
    end_utc   = datetime.now(timezone.utc)
    start_utc = end_utc - timedelta(days=days_back)
    params = {
        "symbols":    symbol,
        "timeframe":  "15Min",
        "start":      start_utc.isoformat(),
        "end":        end_utc.isoformat(),
        "feed":       "iex",
        "limit":      500,
        "adjustment": "split",
    }
    r = http.get(
        f"{ALPACA_DATA_BASE}/v2/stocks/bars",
        headers=alpaca_headers(),
        params=params,
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    return r.json().get("bars", {}).get(symbol, [])

def fetch_finnhub_news(symbol: str, key: str, days_back: int = 7, max_items: int = 5) -> list[dict]:
    """Pull recent company news headlines for the Squeeze strategist call.
    Free tier - returns up to ~50 items per call, we keep the top N by date."""
    today = date.today()
    from_d = today - timedelta(days=days_back)
    try:
        r = http.get(
            f"{FINNHUB_BASE}/company-news",
            params={"symbol": symbol, "from": from_d.isoformat(), "to": today.isoformat(), "token": key},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        items = r.json() or []
    except Exception as e:
        print(f"  [news-warn] {symbol}: {type(e).__name__}: {e}")
        return []
    # Items are dicts with: datetime, headline, source, summary, url, related, image, id, category
    # Most recent first
    items.sort(key=lambda x: x.get("datetime", 0), reverse=True)
    out = []
    for it in items[:max_items]:
        out.append({
            "headline": (it.get("headline") or "")[:200],
            "source":   it.get("source") or "",
            "ts":       it.get("datetime", 0),
        })
    return out

def get_intraday_bars(symbol: str, start_utc: datetime) -> list[dict]:
    """1-minute bars from start_utc until now. Used for Opening Range Breakout."""
    end_utc = datetime.now(timezone.utc)
    params = {
        "symbols":    symbol,
        "timeframe":  "1Min",
        "start":      start_utc.isoformat(),
        "end":        end_utc.isoformat(),
        "feed":       "iex",
        "limit":      500,
        "adjustment": "split",
    }
    r = http.get(
        f"{ALPACA_DATA_BASE}/v2/stocks/bars",
        headers=alpaca_headers(),
        params=params,
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    return r.json().get("bars", {}).get(symbol, [])

def compute_orb_status(symbol: str) -> dict:
    """Read 1-min bars from today's 9:30 ET open and compute the opening range
    (9:30-10:00 ET) plus current breakout status (post-10:00). Volume ratio
    compares the post-range minutes to the range minutes (per-bar average)."""
    now_et = datetime.now(ET)
    open_et = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    range_end_et = now_et.replace(hour=10, minute=0, second=0, microsecond=0)
    open_utc = open_et.astimezone(timezone.utc)

    # Don't bother if we're before 10:00 ET — range isn't complete yet
    if now_et < range_end_et:
        return {"complete": False, "reason": "before-10:00-ET"}

    try:
        bars = get_intraday_bars(symbol, start_utc=open_utc)
    except Exception as e:
        return {"complete": False, "reason": f"fetch-fail: {type(e).__name__}: {e}"}
    if not bars:
        return {"complete": False, "reason": "no-bars"}

    range_bars = []
    later_bars = []
    for b in bars:
        try:
            bt = datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(ET)
        except Exception:
            continue
        if open_et <= bt < range_end_et:
            range_bars.append(b)
        elif bt >= range_end_et:
            later_bars.append(b)

    if not range_bars:
        return {"complete": False, "reason": "no-range-bars"}

    range_high = max(float(b.get("h") or 0) for b in range_bars)
    range_low  = min(float(b.get("l") or 0) for b in range_bars if float(b.get("l") or 0) > 0)
    range_vol_per_min  = sum(int(b.get("v") or 0) for b in range_bars) / max(len(range_bars), 1)
    later_vol_per_min  = sum(int(b.get("v") or 0) for b in later_bars) / max(len(later_bars), 1) if later_bars else 0
    vol_ratio = round(later_vol_per_min / range_vol_per_min, 2) if range_vol_per_min > 0 else 0

    current = float(later_bars[-1].get("c") or 0) if later_bars else float(range_bars[-1].get("c") or 0)
    broke_high = current > range_high and vol_ratio >= 1.5
    broke_low  = current < range_low  and vol_ratio >= 1.5

    return {
        "complete":   True,
        "high":       round(range_high, 2),
        "low":        round(range_low, 2),
        "current":    round(current, 2),
        "vol_ratio":  vol_ratio,
        "broke_high": broke_high,
        "broke_low":  broke_low,
    }

def get_daily_bars(symbol: str, limit: int = 60) -> list[dict]:
    """Daily bars on an equity underlying. Used for RSI/EMA.
    Alpaca's /v2/stocks/bars returns an empty list when called without a
    start/end window (the 'limit' alone isn't enough to anchor the range).
    We ask for a 120-day window so weekends/holidays still leave us with
    enough closes for the 21-period EMA. 'adjustment=split' keeps prior
    closes consistent across stock splits."""
    end   = date.today().isoformat()
    start = (date.today() - timedelta(days=120)).isoformat()
    params = {
        "symbols":    symbol,
        "timeframe":  "1Day",
        "limit":      limit,
        "feed":       "iex",
        "start":      start,
        "end":        end,
        "adjustment": "split",
    }
    r = http.get(
        f"{ALPACA_DATA_BASE}/v2/stocks/bars",
        headers=alpaca_headers(),
        params=params,
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    bars = r.json().get("bars", {}).get(symbol, [])
    if not bars:
        # Surface the empty result so we notice it instead of silently
        # producing all-null indicators downstream.
        print(f"  [bars-empty] {symbol}: no bars returned for {start}..{end}")
    return bars

def get_latest_quote(symbol: str) -> dict:
    r = http.get(
        f"{ALPACA_DATA_BASE}/v2/stocks/{symbol}/quotes/latest",
        headers=alpaca_headers(),
        params={"feed": "iex"},
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    return r.json().get("quote", {})

# ---- Alpaca options helpers ------------------------------------------------

def get_options_contracts(underlying: str, exp_gte: str, exp_lte: str, contract_type: str = "put", status: str = "active") -> list[dict]:
    """Fetch option contracts matching window. Paginates if the chain is big."""
    params = {
        "underlying_symbols": underlying,
        "expiration_date_gte": exp_gte,
        "expiration_date_lte": exp_lte,
        "type": contract_type,
        "status": status,
        "limit": 1000,
    }
    out: list[dict] = []
    next_page: str | None = None
    while True:
        if next_page:
            params["page_token"] = next_page
        r = http.get(
            f"{ALPACA_BASE}/v2/options/contracts",
            headers=alpaca_headers(),
            params=params,
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        j = r.json()
        out.extend(j.get("option_contracts", []))
        next_page = j.get("next_page_token")
        if not next_page:
            break
    return out

def get_option_snapshots(contract_symbols: list[str]) -> dict:
    """Batch snapshot (bid/ask/greeks/iv) for a list of option contract symbols."""
    if not contract_symbols:
        return {}
    # API caps symbols per call; chunk at 100.
    snapshots: dict = {}
    for i in range(0, len(contract_symbols), 100):
        chunk = contract_symbols[i:i + 100]
        r = http.get(
            f"{ALPACA_DATA_BASE}/v1beta1/options/snapshots",
            headers=alpaca_headers(),
            params={"symbols": ",".join(chunk), "feed": "indicative"},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        snapshots.update(r.json().get("snapshots", {}))
    return snapshots

def place_mleg_order(legs: list[dict], limit_price: float, tif: str = "day") -> dict:
    """
    Place a multi-leg options order.
    legs: [{"symbol": <occ>, "side": "buy"|"sell", "ratio_qty": "1",
            "position_intent": "sell_to_open"|"buy_to_open"|
                               "buy_to_close" |"sell_to_close"}]
    limit_price: net credit (positive when opening a credit spread; positive debit when closing).
    """
    body = {
        "order_class": "mleg",
        "qty": "1",
        "type": "limit",
        "time_in_force": tif,
        "limit_price": f"{limit_price:.2f}",
        "legs": legs,
    }
    r = http.post(
        f"{ALPACA_BASE}/v2/orders",
        headers=alpaca_headers(),
        json=body,
        timeout=HTTP_TIMEOUT,
    )
    if r.status_code >= 400:
        # Surface Alpaca's actual rejection text (raise_for_status alone discards it).
        # Truncated to keep DDB rows reasonable.
        raise RuntimeError(
            f"Alpaca {r.status_code} on POST /v2/orders: {r.text[:500]}"
        )
    return r.json()

def place_single_leg_order(symbol: str, side: str, limit_price: float,
                           qty: int = 1, tif: str = "day",
                           position_intent: str = "buy_to_open") -> dict:
    """Place a single-leg option order. Used by Strategy 3 (naked options).
    side: 'buy' (open long call/put) or 'sell' (close existing position).
    position_intent: 'buy_to_open' | 'sell_to_close'."""
    body = {
        "symbol":          symbol,
        "qty":             str(qty),
        "side":            side,
        "type":            "limit",
        "time_in_force":   tif,
        "limit_price":     f"{limit_price:.2f}",
        "position_intent": position_intent,
    }
    r = http.post(
        f"{ALPACA_BASE}/v2/orders",
        headers=alpaca_headers(),
        json=body,
        timeout=HTTP_TIMEOUT,
    )
    if r.status_code >= 400:
        raise RuntimeError(
            f"Alpaca {r.status_code} on POST /v2/orders (single-leg {symbol}): {r.text[:500]}"
        )
    return r.json()

def cancel_order(order_id: str) -> None:
    """Best-effort cancel. 404/422 are fine (already filled/cancelled)."""
    try:
        r = http.delete(
            f"{ALPACA_BASE}/v2/orders/{order_id}",
            headers=alpaca_headers(),
            timeout=HTTP_TIMEOUT,
        )
        if r.status_code >= 500:
            r.raise_for_status()
    except Exception as e:
        print(f"  [cancel-warn] {order_id}: {type(e).__name__}: {e}")

# ---- External data: VIX (IVR proxy) ----------------------------------------

def get_vix() -> float | None:
    """Spot VIX via Yahoo chart endpoint. No auth, no key. None on failure."""
    try:
        r = http.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX",
            params={"interval": "1d", "range": "5d"},
            timeout=HTTP_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0 (trading-bot)"},
        )
        r.raise_for_status()
        meta = r.json()["chart"]["result"][0]["meta"]
        return float(meta.get("regularMarketPrice"))
    except Exception as e:
        print(f"  [vix-warn] {type(e).__name__}: {e}")
        return None

# ---- Indicators ------------------------------------------------------------

def ema(values: list[float], span: int) -> float | None:
    """Standard EMA; returns the latest value."""
    if len(values) < span:
        return None
    k = 2.0 / (span + 1)
    e = sum(values[:span]) / span   # seed with SMA
    for v in values[span:]:
        e = v * k + e * (1 - k)
    return e

def rsi_14(closes: list[float]) -> float | None:
    """Wilder RSI over 14 periods. Returns None if insufficient data."""
    if len(closes) < 15:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_g = sum(gains[:14]) / 14
    avg_l = sum(losses[:14]) / 14
    for i in range(14, len(gains)):
        avg_g = (avg_g * 13 + gains[i]) / 14
        avg_l = (avg_l * 13 + losses[i]) / 14
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100.0 - (100.0 / (1.0 + rs))

def ema_series(values: list[float], span: int) -> list[float]:
    """Full EMA series (one value per bar from index span-1 onward).
    Used by MACD which needs the EMA of an EMA."""
    if len(values) < span:
        return []
    k = 2.0 / (span + 1)
    e = sum(values[:span]) / span
    out = [e]
    for v in values[span:]:
        e = v * k + e * (1 - k)
        out.append(e)
    return out

def macd(closes: list[float], fast: int = 12, slow: int = 26, signal: int = 9) -> tuple:
    """Standard 12/26/9 MACD. Returns (macd_line, signal_line, histogram) - all rounded."""
    if len(closes) < slow + signal:
        return None, None, None
    ema_fast = ema_series(closes, fast)
    ema_slow = ema_series(closes, slow)
    # Align ends: ema_fast has (slow - fast) more leading values than ema_slow
    skip = slow - fast
    ema_fast_aligned = ema_fast[skip:]
    macd_line = [f - s for f, s in zip(ema_fast_aligned, ema_slow)]
    sig_line = ema_series(macd_line, signal)
    if not sig_line:
        return None, None, None
    return round(macd_line[-1], 4), round(sig_line[-1], 4), round(macd_line[-1] - sig_line[-1], 4)

def bb_width_pct(closes: list[float], period: int = 20, mult: float = 2.0) -> float | None:
    """Bollinger Band width as a % of mid. Tight bands = consolidation = potential breakout coil."""
    if len(closes) < period:
        return None
    window = closes[-period:]
    mid = sum(window) / period
    if mid == 0:
        return None
    var = sum((x - mid) ** 2 for x in window) / period
    std = var ** 0.5
    return round((2 * mult * std) / mid * 100, 2)

def volume_ratio(bars: list[dict], period: int = 20) -> float | None:
    """Today's volume vs prior-N-day average. >1.3x = above-average confirmation signal."""
    if len(bars) < period + 1:
        return None
    today_vol = float(bars[-1].get("v") or 0)
    prior_avg = sum(float(b.get("v") or 0) for b in bars[-(period + 1):-1]) / period
    if prior_avg == 0:
        return None
    return round(today_vol / prior_avg, 2)

def atr_14(bars: list[dict], period: int = 14) -> float | None:
    """Average True Range over 14 bars. Used to size breakeven move requirements."""
    if len(bars) < period + 1:
        return None
    trs = []
    for i in range(1, len(bars)):
        h = float(bars[i].get("h") or 0)
        l = float(bars[i].get("l") or 0)
        pc = float(bars[i - 1].get("c") or 0)
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return round(sum(trs[-period:]) / period, 2)

# ---- Market regime gate (Aggressive Call 0) -------------------------------
# 8:30 ET pre-open: classify market regime. Aggressive entries honor this:
# red -> no new trades, yellow -> bullish-only score-8+ at $150, green -> normal.

SYSTEM_REGIME_CHECK = """You are a risk-first trading assistant. Your job is to classify market regime for a momentum debit-spread bot. Apply rules conservatively - when in doubt, return regime=red.

Rules:
- VIX < 30 = green
- VIX 30-40 = yellow (bullish setups only, score 8+ only, $150 max debit)
- VIX > 40 = red (no new trades today)
- SPY below 50-EMA = bearish bias only (or skip if VIX is also elevated)
- SPY 5-day return < -3% = trend broken, return red regardless of VIX

Return JSON only, no preamble."""

def get_spy_indicators() -> dict:
    """SPY price + 50-day EMA + 5-day return for the regime gate."""
    bars = get_daily_bars("SPY", limit=80)
    closes = [float(b["c"]) for b in bars] if bars else []
    if len(closes) < 51:
        return {"price": None, "ema50": None, "ret_5d_pct": None}
    last = closes[-1]
    ema50_val = ema(closes, 50)
    five_ago = closes[-6] if len(closes) >= 6 else None
    ret_5d = ((last - five_ago) / five_ago * 100) if (five_ago and five_ago > 0) else None
    return {
        "price":      round(last, 2),
        "ema50":      round(ema50_val, 2) if ema50_val is not None else None,
        "ret_5d_pct": round(ret_5d, 2) if ret_5d is not None else None,
    }

def claude_regime_check(vix: float | None, spy: dict) -> dict:
    """Have Claude apply the regime rubric and return green/yellow/red."""
    client = _anthropic_client()
    prompt = (
        f"Market regime check for {date.today().isoformat()}.\n"
        f"VIX: {vix if vix is not None else 'n/a'}\n"
        f"SPY price: ${spy.get('price')} | 50-day EMA: ${spy.get('ema50')} | 5-day return: {spy.get('ret_5d_pct')}%\n"
        f"\nApply the rubric and return JSON only:\n"
        '{"regime": "green"|"yellow"|"red", "new_trades_allowed": true|false, '
        '"bias": "bullish"|"bearish"|"neutral", "vix_ok": true|false, '
        '"trend_ok": true|false, "notes": "one sentence"}'
    )
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        system=SYSTEM_REGIME_CHECK,
        messages=[
            {"role": "user", "content": prompt},
            {"role": "assistant", "content": "{"},
        ],
    )
    raw = msg.content[0].text.strip()
    text = raw if raw.startswith("{") else "{" + raw
    return _parse_json(text, fallback={
        "regime": "red", "new_trades_allowed": False,
        "bias": "neutral", "vix_ok": False, "trend_ok": False,
        "notes": "fallback - parse error",
    })

def save_regime(regime: dict, inputs: dict) -> None:
    """Persist today's regime decision to SSM for downstream calls to read."""
    payload = {
        **regime,
        "inputs":    inputs,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "date":      date.today().isoformat(),
    }
    try:
        ssm.put_parameter(
            Name="/trading-bot/regime",
            Value=json.dumps(payload),
            Type="String",
            Overwrite=True,
        )
        _secret_cache.pop("/trading-bot/regime", None)
    except Exception as e:
        print(f"  [regime-save-fail] {type(e).__name__}: {e}")

def get_active_regime() -> dict:
    """Read today's regime from SSM. Fail-open to green if missing/stale -
    the Aggressive entry pipeline already has score/earnings/sector/RR gates,
    so a missed regime check shouldn't silently block all trades."""
    try:
        raw = get_secret("/trading-bot/regime")
        data = json.loads(raw)
        if data.get("date") == date.today().isoformat():
            return data
    except Exception as e:
        print(f"  [regime-read-warn] {type(e).__name__}: {e}")
    return {
        "regime": "green",
        "new_trades_allowed": True,
        "bias": "neutral",
        "stale": True,
        "notes": "regime-not-set-today (fail-open default)",
    }

def run_regime_check() -> dict:
    """8:30 ET. Classify market regime and persist for the day."""
    print("[regime-check] assessing")
    vix = get_vix()
    spy = get_spy_indicators()
    print(f"  VIX={vix} SPY=${spy.get('price')} 50EMA=${spy.get('ema50')} 5d={spy.get('ret_5d_pct')}%")
    decision = claude_regime_check(vix, spy)
    inputs = {"vix": vix, **spy}
    save_regime(decision, inputs)
    print(f"  regime={decision.get('regime')} bias={decision.get('bias')} new_trades={decision.get('new_trades_allowed')}")
    return {
        "status":             "ok",
        "mode":               "regime_check",
        "regime":             decision.get("regime"),
        "bias":               decision.get("bias"),
        "new_trades_allowed": decision.get("new_trades_allowed"),
        "notes":              decision.get("notes"),
        "inputs":             inputs,
    }

# ---- Universe refresh (Aggressive Layer 2 — curated list with options gate)
#
# Originally this scraped Finviz, but Finviz now serves a JS-rendered page
# that's not parseable from Lambda. Pivoted to a curated list + Alpaca
# options eligibility check. Update the list any time with:
#
#   $list = '["NVDA","MSFT","GOOGL","META","AVGO","AMD","ORCL","CRM","NOW","PLTR","TSLA","MU"]'
#   aws ssm put-parameter --name /trading-bot/dynamic-tickers --type String `
#       --overwrite --value $list --region us-east-1
#
# The Sunday weekly job re-validates the curated list against Alpaca's options
# chain (drops anything that lost option-tradability) and writes the survivors
# back to SSM. No scraping involved.

# Curated default if Dan never sets the SSM param. Mix of mega-cap tech +
# semis + AI/data plays. Dropped any with chronic earnings overlap risk.
MOMENTUM_LAYER2_CURATED_DEFAULT = [
    "NVDA", "MSFT", "GOOGL", "META", "AVGO",
    "AMD",  "ORCL", "CRM",   "NOW",  "PLTR",
    "TSLA", "MU",
]

def fetch_polygon_universe(max_tickers: int = 30) -> list[str]:
    """Pull Polygon's grouped daily aggregates for the last completed trading
    day. One call returns OHLCV for every US-listed stock that traded that day.
    Filter by price ($20-$1000) and volume (>500k), sort by $-volume desc
    (liquidity proxy). Free tier: 5 calls/min, 1 call here."""
    try:
        key = get_secret("/trading-bot/polygon-key")
    except Exception as e:
        print(f"  [polygon-key-missing] {type(e).__name__}: {e}")
        return []

    # Use the most recent weekday — Polygon's grouped aggs publish after close.
    # Walk back up to 5 days to skip weekends/holidays.
    from_date = date.today()
    last_err = None
    for _ in range(5):
        from_date = from_date - timedelta(days=1)
        if from_date.weekday() >= 5:  # Saturday/Sunday
            continue
        url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{from_date.isoformat()}"
        try:
            r = http.get(
                url,
                params={"adjusted": "true", "apiKey": key},
                timeout=30,
            )
            if r.status_code == 200:
                data = r.json()
                if data.get("resultsCount", 0) > 0:
                    print(f"  [polygon] grouped aggs for {from_date} returned {data.get('resultsCount')} stocks")
                    break
                # 200 with 0 results means the date had no trading - keep walking back
                last_err = f"{from_date}: 200 OK but resultsCount=0"
                continue
            last_err = f"{from_date}: HTTP {r.status_code}: {r.text[:200]}"
            print(f"  [polygon-try] {last_err}")
        except Exception as e:
            last_err = f"{from_date}: {type(e).__name__}: {e}"
            print(f"  [polygon-try] {last_err}")
    else:
        print(f"  [polygon-fail] no usable trading day in last 5 days: {last_err}")
        return []

    results = data.get("results") or []
    PRICE_MIN, PRICE_MAX = 20.0, 1000.0
    VOL_MIN = 500_000

    passed: list[tuple[str, float, int]] = []
    for r_item in results:
        sym = (r_item.get("T") or "").strip().upper()
        # Skip non-standard tickers (warrants, units, depositary, share classes after dot)
        if not sym or len(sym) > 5 or "." in sym:
            continue
        price  = float(r_item.get("c") or 0)
        volume = int(r_item.get("v") or 0)
        if not (PRICE_MIN <= price <= PRICE_MAX):
            continue
        if volume < VOL_MIN:
            continue
        dollar_vol = price * volume
        passed.append((sym, dollar_vol, volume))

    passed.sort(key=lambda x: x[1], reverse=True)
    print(f"  [polygon] {len(passed)} passed price/volume filter; top 5 by $vol: {[p[0] for p in passed[:5]]}")
    return [p[0] for p in passed[:max_tickers]]

def filter_for_options_eligibility(tickers: list[str], max_check: int = 20) -> list[str]:
    """Confirm Alpaca has option contracts in the next 30 days for each ticker.
    Capped at max_check to stay under Lambda timeout (each Alpaca call ~1s)."""
    eligible: list[str] = []
    today = date.today()
    gte = today.isoformat()
    lte = (today + timedelta(days=30)).isoformat()
    for t in tickers[:max_check]:
        try:
            r = http.get(
                f"{ALPACA_BASE}/v2/options/contracts",
                headers=alpaca_headers(),
                params={
                    "underlying_symbols":  t,
                    "expiration_date_gte": gte,
                    "expiration_date_lte": lte,
                    "limit":               5,
                },
                timeout=HTTP_TIMEOUT,
            )
            if r.status_code == 200 and (r.json().get("option_contracts") or []):
                eligible.append(t)
            else:
                print(f"  [options-skip] {t}: no chain in next 30d")
        except Exception as e:
            print(f"  [options-check-fail] {t}: {type(e).__name__}: {e}")
    return eligible

def run_reconciliation() -> dict:
    """Daily DDB <-> Alpaca sync. For each strategy, fetch the broker's actual
    open positions and archive any DDB open-spread row whose contract symbols
    Alpaca doesn't recognize. Catches: never-filled opens (phantoms),
    externally-closed positions, split-adjusted symbol drift, anything else
    causing the ledger to diverge from reality. Scheduled pre-market so the
    every-15-min sweeps don't waste cycles on phantoms."""
    print("[reconcile] daily DDB<->Alpaca sync starting")
    summary: dict = {}

    for strategy in (STRATEGY_CREDIT_SPREAD, STRATEGY_MOMENTUM, STRATEGY_NAKED):
        set_active_strategy(strategy)
        info = {"ddb_open": 0, "alpaca_positions": 0, "phantoms_archived": 0, "errors": []}

        # 1. Read Alpaca actual positions for this account
        try:
            actual = get_positions()
        except Exception as e:
            err = f"alpaca-positions-fail: {type(e).__name__}: {e}"
            print(f"  [{strategy}] {err}")
            info["errors"].append(err)
            summary[strategy] = info
            continue
        actual_symbols = {(p.get("symbol") or "").upper() for p in actual if p.get("symbol")}
        info["alpaca_positions"] = len(actual)

        # 2. Read DDB open spreads for this account
        try:
            opens = load_open_spreads()
        except Exception as e:
            err = f"ddb-read-fail: {type(e).__name__}: {e}"
            print(f"  [{strategy}] {err}")
            info["errors"].append(err)
            summary[strategy] = info
            continue
        info["ddb_open"] = len(opens)
        print(f"  [{strategy}] ddb={info['ddb_open']} alpaca={info['alpaca_positions']}")

        # 3. Detect phantoms by comparing contract symbols
        for s in opens:
            missing: list[str] = []
            if strategy == STRATEGY_NAKED:
                contract = (s.get("contract_symbol") or "").upper()
                if contract and contract not in actual_symbols:
                    missing.append(f"{contract} not held")
            else:
                # mleg strategies: both legs must be present
                sh = (s.get("short_symbol") or "").upper()
                lo = (s.get("long_symbol") or "").upper()
                if sh and sh not in actual_symbols:
                    missing.append(f"short leg {sh} missing")
                if lo and lo not in actual_symbols:
                    missing.append(f"long leg {lo} missing")
            if not missing:
                continue

            reason = "reconcile: " + "; ".join(missing)
            print(f"  [{strategy}] phantom: {s.get('id')} - {reason}")
            outcome = {
                "exit_premium":  0,
                "exit_total":    0,
                "pnl_dollars":   0,
                "pnl_pct":       0,
                "exit_reason":   reason + " (auto-archived by reconciliation)",
            }
            try:
                archive_spread(s, outcome)
                log_event("close",
                          (s.get("underlying") or s.get("ticker") or "BOT"),
                          {**s, **outcome}, reason)
                info["phantoms_archived"] += 1
            except Exception as ae:
                err = f"archive-fail {s.get('id')}: {type(ae).__name__}: {ae}"
                print(f"  [{strategy}] {err}")
                info["errors"].append(err)

        summary[strategy] = info

    total_phantoms = sum(s.get("phantoms_archived", 0) for s in summary.values())
    print(f"[reconcile] complete - archived {total_phantoms} phantoms across {len(summary)} strategies")
    return {
        "status":         "ok",
        "mode":           "reconcile",
        "total_phantoms": total_phantoms,
        "by_strategy":    summary,
    }

def run_refresh_universe() -> dict:
    """Sunday: refresh Layer 2 via Polygon snapshot (primary) or curated default
    (fallback). Validates each candidate against Alpaca options eligibility,
    saves survivors to SSM."""
    print("[refresh-universe] starting")

    # Try Polygon first - real market discovery
    polygon_picks = fetch_polygon_universe(max_tickers=30)
    source = "polygon"

    if polygon_picks:
        candidates = polygon_picks
        print(f"  Polygon delivered {len(candidates)} candidates")
    else:
        # Fallback chain: existing SSM list -> curated default
        print("  Polygon returned 0; falling back to existing SSM or curated default")
        existing: list[str] | None = None
        try:
            raw = get_secret("/trading-bot/dynamic-tickers")
            loaded = json.loads(raw)
            if isinstance(loaded, list) and loaded:
                existing = [str(t).upper() for t in loaded if isinstance(t, str)]
        except Exception:
            pass
        candidates = existing or list(MOMENTUM_LAYER2_CURATED_DEFAULT)
        source = "ssm-existing" if existing else "curated-default"
        print(f"  using {source}: {len(candidates)} candidates")

    # Dedupe Layer 1 ETFs out of the dynamic list
    layer1 = set(MOMENTUM_UNIVERSE_LAYER1)
    deduped = [t for t in candidates if t not in layer1]

    set_active_strategy(STRATEGY_MOMENTUM)
    print(f"  validating {len(deduped)} tickers via Alpaca options chain (max 20)")
    eligible = filter_for_options_eligibility(deduped, max_check=20)
    print(f"  {len(eligible)}/{min(len(deduped), 20)} have tradable options")

    # Cap final list at 12 to keep Claude prompts manageable
    final_list = eligible[:12]

    try:
        ssm.put_parameter(
            Name="/trading-bot/dynamic-tickers",
            Value=json.dumps(final_list),
            Type="String",
            Overwrite=True,
        )
        _secret_cache.pop("/trading-bot/dynamic-tickers", None)
    except Exception as e:
        print(f"  [universe-save-fail] {type(e).__name__}: {e}")
        return {"status": "error", "error": str(e), "fetched": final_list}

    return {
        "status":              "ok",
        "mode":                "refresh_universe",
        "source":              source,
        "layer1":              list(MOMENTUM_UNIVERSE_LAYER1),
        "layer2":              final_list,
        "total_universe_size": len(MOMENTUM_UNIVERSE_LAYER1) + len(final_list),
        "dropped":             [t for t in deduped[:20] if t not in eligible],
    }

# --- Squeeze-Cross indicators (Strategy 2 v2 rebuild) -----------------------
# TTM Squeeze + 9/21 EMA cross + intraday VWAP. All hand-rolled to avoid
# pulling pandas_ta into the Lambda zip (would blow past the 250MB limit).

def bollinger_bands_full(closes: list[float], period: int = 20, mult: float = 2.0):
    """Returns (upper, mid, lower) on the last bar. None if not enough data."""
    if len(closes) < period:
        return None, None, None
    window = closes[-period:]
    mid = sum(window) / period
    var = sum((x - mid) ** 2 for x in window) / period
    std = var ** 0.5
    return round(mid + mult * std, 4), round(mid, 4), round(mid - mult * std, 4)

def true_range(highs: list[float], lows: list[float], closes: list[float], i: int) -> float:
    """True Range at index i. Uses closes[i-1] as the prior close."""
    if i < 1:
        return highs[i] - lows[i]
    h, l, pc = highs[i], lows[i], closes[i - 1]
    return max(h - l, abs(h - pc), abs(l - pc))

def atr_simple(highs: list[float], lows: list[float], closes: list[float], period: int = 20):
    """Simple ATR (average of TR over last `period` bars). Used by Keltner channels."""
    if min(len(highs), len(lows), len(closes)) < period + 1:
        return None
    trs = [true_range(highs, lows, closes, i) for i in range(-period, 0)]
    return sum(trs) / period

def keltner_channels(highs, lows, closes, period: int = 20, mult: float = 1.5):
    """Returns (upper, mid, lower) where mid = EMA(close, period) and the
    band offset is mult * ATR(period). None if data insufficient."""
    mid_val = ema(closes, period)
    atr_val = atr_simple(highs, lows, closes, period)
    if mid_val is None or atr_val is None:
        return None, None, None
    return round(mid_val + mult * atr_val, 4), round(mid_val, 4), round(mid_val - mult * atr_val, 4)

def linear_regression_slope(values: list[float], period: int = 12):
    """Closed-form OLS slope of the last `period` values vs index 0..period-1.
    Used for TTM Squeeze's momentum histogram direction."""
    if len(values) < period:
        return None
    y = values[-period:]
    n = period
    x_mean = (n - 1) / 2
    y_mean = sum(y) / n
    num = sum((i - x_mean) * (y[i] - y_mean) for i in range(n))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return (num / den) if den != 0 else 0.0

def ttm_squeeze_status(highs: list[float], lows: list[float], closes: list[float],
                       period: int = 20, bb_mult: float = 2.0, kc_mult: float = 1.5):
    """Detect TTM Squeeze fire on the current bar.
    Returns:
      {
        'squeeze_on_now':   bool,
        'squeeze_on_prev':  bool,
        'squeeze_fired':    bool   (prev was on, now is off — the entry trigger),
        'direction':        'bullish' | 'bearish' | 'neutral',
        'momentum':         float   (LR slope of midpoint, sign = direction),
        'bb_upper, bb_lower, kc_upper, kc_lower': floats
      }
    Returns None if insufficient bar history."""
    if min(len(highs), len(lows), len(closes)) < period + 2:
        return None

    # Current bar squeeze test
    bb_u, _, bb_l = bollinger_bands_full(closes, period, bb_mult)
    kc_u, _, kc_l = keltner_channels(highs, lows, closes, period, kc_mult)
    if None in (bb_u, bb_l, kc_u, kc_l):
        return None
    squeeze_on_now = (bb_u < kc_u) and (bb_l > kc_l)

    # Previous bar squeeze test (drop the most recent bar)
    bb_u2, _, bb_l2 = bollinger_bands_full(closes[:-1], period, bb_mult)
    kc_u2, _, kc_l2 = keltner_channels(highs[:-1], lows[:-1], closes[:-1], period, kc_mult)
    squeeze_on_prev = False
    if None not in (bb_u2, bb_l2, kc_u2, kc_l2):
        squeeze_on_prev = (bb_u2 < kc_u2) and (bb_l2 > kc_l2)

    # The fire: prev was squeezed, now is not (bands just expanded)
    squeeze_fired = squeeze_on_prev and not squeeze_on_now

    # Momentum direction: LR slope of midpoint (h+l)/2 over last `min(12, period)` bars
    mid_pts = [(highs[i] + lows[i]) / 2 for i in range(-min(12, period), 0)]
    slope = linear_regression_slope(mid_pts, period=len(mid_pts))
    if slope is None or slope == 0:
        direction = "neutral"
    elif slope > 0:
        direction = "bullish"
    else:
        direction = "bearish"

    return {
        "squeeze_on_now":  squeeze_on_now,
        "squeeze_on_prev": squeeze_on_prev,
        "squeeze_fired":   squeeze_fired,
        "direction":       direction,
        "momentum":        round(slope or 0, 4),
        "bb_upper":        bb_u,
        "bb_lower":        bb_l,
        "kc_upper":        kc_u,
        "kc_lower":        kc_l,
    }

def compute_vwap_today(symbol: str) -> float | None:
    """Volume-weighted average price from today's 9:30 ET open through now.
    Required by the VWAP-alignment entry gate. Fetches 1Min bars via the
    existing intraday helper, resets at the open every morning."""
    now_et  = datetime.now(ET)
    open_et = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    if now_et < open_et:
        return None  # before the open
    open_utc = open_et.astimezone(timezone.utc)
    try:
        bars = get_intraday_bars(symbol, start_utc=open_utc)
    except Exception as e:
        print(f"  [vwap-warn] {symbol}: {type(e).__name__}: {e}")
        return None
    if not bars:
        return None
    total_pv = 0.0
    total_v  = 0.0
    for b in bars:
        try:
            h = float(b.get("h") or 0)
            l = float(b.get("l") or 0)
            c = float(b.get("c") or 0)
            v = float(b.get("v") or 0)
        except Exception:
            continue
        if v <= 0 or h <= 0 or l <= 0:
            continue
        typical = (h + l + c) / 3
        total_pv += typical * v
        total_v  += v
    return round(total_pv / total_v, 2) if total_v > 0 else None


SYSTEM_SQUEEZE_STRATEGIST = """You are a Risk Management Engine for an aggressive 15-minute momentum options strategy. A TTM Squeeze has fired with EMA 9/21 cross + VWAP alignment confirming the direction. Your job: check for 'Bull Traps' or 'Negative Catalyst Divergence' before approving the trade.

You'll receive:
- Ticker + direction (bullish/bearish) + watchlist type tag
- Technical state (price vs VWAP, squeeze momentum slope, EMAs)
- Recent news headlines (last 7 days, most recent first)
- Market regime context

Scoring (0-10):
- 0-3: Major contrary catalyst (downgrade, regulatory negative, imminent earnings against direction). Abort.
- 4-6: Mixed - technicals say go, news suggests caution. WAIT.
- 7-8: Neutral or supportive news. PROCEED at delta 0.55-0.60.
- 9-10: Strongly aligned news (positive for bullish, negative for bearish). PROCEED at delta 0.60-0.65 (more conviction = closer to ITM).

Strict rule: score 6 or below = action WAIT, no entry.

Output JSON only, no preamble:
{"action": "BUY" | "WAIT", "score": 0-10, "strike_delta": 0.55-0.65, "reason": "<25 words>"}"""

def get_top_movers(top_n: int = 20) -> list[str]:
    """Fetch today's most-active stocks from Alpaca's screener endpoint.
    Filters for price > $5 and clean ticker format. Falls back to [] on any
    error so the scan still runs using the permanent anchor universe."""
    try:
        params = {"by": "volume", "top": top_n}
        r = http.get(
            f"{ALPACA_DATA_BASE}/v1beta1/screener/stocks/most-actives",
            headers=alpaca_headers(),
            params=params,
            timeout=HTTP_TIMEOUT,
        )
        if r.status_code != 200:
            print(f"  [movers-skip] screener returned {r.status_code} - using anchor universe only")
            return []
        items = r.json().get("most_actives", [])
        symbols = []
        for item in items:
            sym   = (item.get("symbol") or "").upper().strip()
            price = float(item.get("price") or 0)
            # Skip: empty, penny stocks, special chars (BRK.B), very long (ETF variants)
            if not sym or price < 5 or len(sym) > 5:
                continue
            if any(c in sym for c in (".", "/", "-")):
                continue
            symbols.append(sym)
        return symbols
    except Exception as e:
        print(f"  [movers-fail] {type(e).__name__}: {e}")
        return []

def build_squeeze_universe() -> tuple[list[str], dict[str, str]]:
    """Permanent anchor universe + today's Alpaca most-actives, deduped.
    Hard-caps at 30 tickers so the Lambda scan stays well under timeout.
    Returns (universe_list, watchlist_meta_dict)."""
    universe = list(SQUEEZE_UNIVERSE)
    meta     = dict(SQUEEZE_WATCHLIST_META)
    seen     = set(universe)

    movers  = get_top_movers(top_n=20)
    added   = []
    for sym in movers:
        if sym in seen or len(universe) >= 30:
            continue
        universe.append(sym)
        seen.add(sym)
        meta[sym] = "Dynamic"
        added.append(sym)

    if added:
        print(f"  [dynamic] added {len(added)} movers to universe: {added}")
    print(f"  [universe] scanning {len(universe)} tickers total")
    return universe, meta

def compose_squeeze_signal(symbol: str, meta_override: dict | None = None) -> dict:
    """Full Level 1 technical evaluation for one ticker. Returns the signal
    dict that feeds Claude. level1_pass=True means squeeze fired AND EMA cross
    AND VWAP all aligned for the same direction.
    meta_override: pass the dict from build_squeeze_universe() so dynamic
    tickers show their 'Dynamic' tag rather than '?'."""
    try:
        bars = get_15min_bars(symbol, days_back=5)
    except Exception as e:
        return {"symbol": symbol, "level1_pass": False, "reason": f"bars-fail: {e}"}
    if not bars or len(bars) < 25:
        return {"symbol": symbol, "level1_pass": False, "reason": f"insufficient 15m bars ({len(bars) if bars else 0})"}

    highs  = [float(b.get("h") or 0) for b in bars]
    lows   = [float(b.get("l") or 0) for b in bars]
    closes = [float(b.get("c") or 0) for b in bars]
    if any(c <= 0 for c in closes[-25:]):
        return {"symbol": symbol, "level1_pass": False, "reason": "bad bar data"}

    ema9  = ema(closes, 9)
    ema21 = ema(closes, 21)
    if ema9 is None or ema21 is None:
        return {"symbol": symbol, "level1_pass": False, "reason": "ema undefined"}

    sq = ttm_squeeze_status(highs, lows, closes)
    if not sq:
        return {"symbol": symbol, "level1_pass": False, "reason": "squeeze undefined"}

    current_price = closes[-1]
    vwap = compute_vwap_today(symbol)

    ema_long_ok  = ema9 > ema21
    ema_short_ok = ema9 < ema21

    # Direction must agree across EMA cross + squeeze momentum slope
    direction = None
    if ema_long_ok and sq["direction"] == "bullish":
        direction = "bullish"
    elif ema_short_ok and sq["direction"] == "bearish":
        direction = "bearish"

    # VWAP alignment: price above VWAP for bull, below for bear (or unknown VWAP = neutral)
    if direction == "bullish":
        vwap_aligned = (vwap is None) or (current_price > vwap)
    elif direction == "bearish":
        vwap_aligned = (vwap is None) or (current_price < vwap)
    else:
        vwap_aligned = False

    level1_pass = bool(sq["squeeze_fired"]) and (direction is not None) and vwap_aligned

    return {
        "symbol":         symbol,
        "watchlist_type": (meta_override or SQUEEZE_WATCHLIST_META).get(symbol, "?"),
        "level1_pass":    level1_pass,
        "current_price":  round(current_price, 2),
        "direction":      direction,
        "ema9":           round(ema9, 2),
        "ema21":          round(ema21, 2),
        "ema_aligned":    direction is not None,
        "squeeze_fired":  sq["squeeze_fired"],
        "squeeze_on_now": sq["squeeze_on_now"],
        "momentum":       sq["momentum"],
        "bb_upper":       sq["bb_upper"],
        "bb_lower":       sq["bb_lower"],
        "kc_upper":       sq["kc_upper"],
        "kc_lower":       sq["kc_lower"],
        "vwap":           vwap,
        "vwap_aligned":   vwap_aligned,
    }

def claude_squeeze_eval(signal: dict, news: list[dict], regime: dict | None = None) -> dict:
    """Level 2: send the technical signal + news headlines to Claude for the
    'contrary catalyst' risk check. Returns {action, score, strike_delta, reason}."""
    client = _anthropic_client()
    direction = signal.get("direction", "?")
    vwap = signal.get("vwap")
    price = signal["current_price"]
    vwap_side = "above" if vwap and price > vwap else "below" if vwap else "n/a"

    news_str = "\n".join(
        f"  - {n.get('headline','')[:160]} ({n.get('source','')})"
        for n in (news or [])[:5]
    ) or "  (no recent headlines)"

    regime_str = ""
    if regime:
        regime_str = f"Regime: {regime.get('regime','?')} | bias: {regime.get('bias','?')} | new_trades_allowed: {regime.get('new_trades_allowed','?')}\n"

    prompt = (
        f"{regime_str}"
        f"15m TTM Squeeze fired {direction} on {signal['symbol']} (watchlist type: {signal.get('watchlist_type','?')}).\n"
        f"Price ${price} is {vwap_side} VWAP (${vwap}).\n"
        f"EMA9 ${signal['ema9']} vs EMA21 ${signal['ema21']} - aligned for {direction}.\n"
        f"Squeeze momentum slope: {signal['momentum']}.\n"
        f"\nRecent headlines:\n{news_str}\n"
        f"\nAnalyze for bull traps or contrary catalysts. JSON only."
    )

    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        system=SYSTEM_SQUEEZE_STRATEGIST,
        messages=[
            {"role": "user", "content": prompt},
            {"role": "assistant", "content": "{"},
        ],
    )
    raw = msg.content[0].text.strip()
    text = raw if raw.startswith("{") else "{" + raw
    parsed = _parse_json(text, fallback={"action": "WAIT", "score": 0, "strike_delta": 0.60, "reason": "parse-fail"})
    parsed.setdefault("_meta", {})["stop_reason"] = getattr(msg, "stop_reason", "?")
    return parsed

def compute_9ema_1min(symbol: str, lookback_minutes: int = 60) -> float | None:
    """9-period EMA of 1-min closes — the Squeeze-Cross trailing reference."""
    start_utc = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
    try:
        bars = get_intraday_bars(symbol, start_utc=start_utc)
    except Exception as e:
        print(f"  [9ema-warn] {symbol}: {type(e).__name__}: {e}")
        return None
    if not bars or len(bars) < 9:
        return None
    closes = [float(b.get("c") or 0) for b in bars if float(b.get("c") or 0) > 0]
    if len(closes) < 9:
        return None
    val = ema(closes, 9)
    return round(val, 2) if val is not None else None

def _close_squeeze_tranche(spread: dict, qty_to_close: int, reason: str,
                           current_value: float | None) -> dict | None:
    """Sell-to-close N contracts (a partial tranche). Does NOT archive — caller
    is responsible for updating the spread row state afterward. Returns
    {qty_closed, premium, realized_pnl} on success, None on failure."""
    qty_to_close = min(qty_to_close, int(spread.get("qty", 0) or 0))
    if qty_to_close <= 0:
        return None
    limit = max(round(current_value * 0.9, 2), 0.01) if (current_value and current_value > 0) else 0.01
    underlying = spread.get("underlying") or spread.get("ticker") or "BOT"

    try:
        order = place_single_leg_order(
            symbol=spread["contract_symbol"],
            side="sell",
            limit_price=limit,
            qty=qty_to_close,
            tif="day",
            position_intent="sell_to_close",
        )
    except Exception as e:
        err = str(e)
        # Phantom detection (re-use same logic as naked)
        if "position intent mismatch" in err or "inferred: sell_to_open" in err:
            print(f"  [squeeze-phantom-tranche] {spread.get('id')}: archiving full row")
            archive_spread(spread, {
                "exit_premium": 0, "exit_total": 0, "pnl_dollars": 0, "pnl_pct": 0,
                "exit_reason": "phantom on tranche close - Alpaca had no holding",
            })
            return {"qty_closed": 0, "phantom": True}
        print(f"  [squeeze-tranche-fail] {spread.get('id')}: {type(e).__name__}: {e}")
        log_event("error", underlying, {"stage": "squeeze_tranche", "error": err, "id": spread.get("id")}, reason)
        return None

    filled = _wait_for_fill(order.get("id", ""), target=limit)
    final_premium = round(abs(float(filled if filled else limit)), 2)
    premium_paid = float(spread.get("premium_paid", 0) or 0)
    realized = round((final_premium - premium_paid) * 100 * qty_to_close, 2)

    log_event("close_partial", underlying, {
        **spread,
        "tranche_qty":     qty_to_close,
        "tranche_premium": final_premium,
        "tranche_pnl":     realized,
    }, reason)
    print(f"  [squeeze-tranche] {spread.get('id')} sold {qty_to_close}c @ ${final_premium:.2f} -> realized ${realized:+.0f}")
    return {"qty_closed": qty_to_close, "premium": final_premium, "realized_pnl": realized}

def _evaluate_squeeze_exit(spread: dict, current_value: float | None,
                           current_underlying: float | None, ema9_1m: float | None,
                           today: "date") -> tuple:
    """Squeeze-Cross exit decision. Returns (action, reason) where action is one of:
    'hold' | 'stop' | 'tp1' | 'tp2' | 'trail_exit' | 'time_exit'."""
    premium = float(spread.get("premium_paid", 0) or 0)
    if premium <= 0:
        return ("hold", "missing premium basis")
    if current_value is None:
        return ("hold", "no live quote")
    pnl_pct = (current_value - premium) / premium

    try:
        expiry = date.fromisoformat(spread["expiry"])
    except Exception:
        expiry = today
    dte = (expiry - today).days

    # 1. Time exit — close anything at or past expiry day
    if dte <= 0:
        return ("time_exit", f"{dte}DTE at expiry")

    # 2. Stop loss (could be -20% original or breakeven after TP1)
    stop_value = float(spread.get("stop_value", 0) or 0)
    if current_value <= stop_value:
        if spread.get("tp1_hit"):
            return ("stop", f"breakeven stop hit (current ${current_value} <= ${stop_value} after TP1)")
        return ("stop", f"-{int(SQUEEZE_STOP_PCT*100)}% stop hit (current ${current_value} <= ${stop_value})")

    # 3. TP1 — first profit-take tranche
    if not spread.get("tp1_hit"):
        tp1 = float(spread.get("tp1_value", 0) or 0)
        if tp1 > 0 and current_value >= tp1:
            return ("tp1", f"+{pnl_pct*100:.0f}% gain - TP1 fires (close 50%, stop to BE)")

    # 4. TP2 — second profit-take tranche
    if spread.get("tp1_hit") and not spread.get("tp2_hit"):
        tp2 = float(spread.get("tp2_value", 0) or 0)
        if tp2 > 0 and current_value >= tp2:
            return ("tp2", f"+{pnl_pct*100:.0f}% gain - TP2 fires (close 25%, start trail)")

    # 5. Trailing exit — runner closes when 1-min candle closes against 9 EMA
    if spread.get("trailing") and ema9_1m is not None and current_underlying is not None:
        direction = (spread.get("direction") or "").lower()
        if direction == "bullish" and current_underlying < ema9_1m:
            return ("trail_exit", f"bullish runner: ${current_underlying} < 9EMA ${ema9_1m}")
        if direction == "bearish" and current_underlying > ema9_1m:
            return ("trail_exit", f"bearish runner: ${current_underlying} > 9EMA ${ema9_1m}")

    return ("hold", f"pnl {pnl_pct*100:+.0f}% dte {dte} qty={spread.get('qty')}/{spread.get('qty_original')}")

def _sweep_squeeze() -> dict:
    """Walk open Squeeze-Cross positions, evaluate each, handle tranches and final closes."""
    set_active_strategy(STRATEGY_MOMENTUM)
    opens = load_open_spreads()
    squeeze_rows = [s for s in opens if s.get("kind") == "squeeze"]
    if not squeeze_rows:
        return {"checked": 0, "closed": 0, "decisions": []}

    today = date.today()
    fully_closed = 0
    decisions = []
    for s in squeeze_rows:
        underlying = s.get("underlying") or s.get("ticker")
        if not underlying:
            continue
        value     = _current_naked_value(s)
        ul_price  = _current_underlying_price_for_naked(underlying)
        ema9_1m   = compute_9ema_1min(underlying) if s.get("trailing") else None
        action, reason = _evaluate_squeeze_exit(s, value, ul_price, ema9_1m, today)
        dec = {"id": s.get("id"), "action": action, "reason": reason,
               "current_value": value, "underlying_price": ul_price}
        decisions.append(dec)

        if action == "hold":
            continue

        qty_orig = int(s.get("qty_original", s.get("qty", 1)) or 1)
        qty_now  = int(s.get("qty", 0) or 0)
        if qty_now <= 0:
            continue

        if action == "tp1":
            qty_close = min(max(int(round(qty_orig * SQUEEZE_TP1_SCALE_PCT)), 1), qty_now)
            result = _close_squeeze_tranche(s, qty_close, reason, value)
            if result and not result.get("phantom"):
                # Update DDB: reduce qty, mark TP1, move stop to breakeven (premium_paid)
                new_s = dict(s)
                new_s["qty"]        = qty_now - qty_close
                new_s["tp1_hit"]    = True
                new_s["stop_value"] = float(s.get("premium_paid", 0))
                save_open_spread(new_s)
                send_position_alert("tp1", new_s, result)
                print(f"  [squeeze-tp1] {s.get('id')} closed {qty_close} of {qty_orig}, stop moved to BE")
        elif action == "tp2":
            qty_close = min(max(int(round(qty_orig * SQUEEZE_TP2_SCALE_PCT)), 1), qty_now)
            result = _close_squeeze_tranche(s, qty_close, reason, value)
            if result and not result.get("phantom"):
                new_s = dict(s)
                new_s["qty"]      = qty_now - qty_close
                new_s["tp2_hit"]  = True
                new_s["trailing"] = True
                save_open_spread(new_s)
                send_position_alert("tp2", new_s, result)
                print(f"  [squeeze-tp2] {s.get('id')} closed {qty_close} of {qty_orig}, runner trailing on 9EMA")
        else:
            # stop / time_exit / trail_exit — close all remaining and archive
            outcome = _close_naked_position(s, reason, value)
            if outcome and not outcome.get("error") and not outcome.get("phantom"):
                fully_closed += 1

    return {"checked": len(squeeze_rows), "closed": fully_closed, "decisions": decisions}

def run_squeeze_sweep() -> dict:
    """Sweep mode for Squeeze-Cross — evaluates open squeeze positions and
    handles the multi-tier scale-out. Fires every 15 min from the existing
    momentum exit-sweep schedule once Phase F is live."""
    print("[squeeze-sweep] running")
    try:
        result = _sweep_squeeze()
    except Exception as e:
        print(f"  [squeeze-sweep-fail] {type(e).__name__}: {e}")
        return {"status": "error", "strategy": STRATEGY_MOMENTUM, "mode": "squeeze_sweep", "error": str(e)}
    print(f"  checked={result['checked']} fully_closed={result['closed']}")
    return {"status": "ok", "strategy": STRATEGY_MOMENTUM, "mode": "squeeze_sweep", **result}

def run_squeeze_tick() -> dict:
    """Combined tick: sweep existing positions first, then scan for new fires
    and attempt entries. Ideal cadence for TTM Squeeze since fires can happen
    on any 15-min bar. Replaces the old per-mode entry/exit split."""
    print("[squeeze-tick] running")
    sweep_result = {"checked": 0, "closed": 0}
    entry_result = {"opened": []}
    try:
        sweep_result = _sweep_squeeze()
        print(f"  sweep: checked={sweep_result.get('checked', 0)} closed={sweep_result.get('closed', 0)}")
    except Exception as e:
        print(f"  [sweep-warn] {type(e).__name__}: {e}")
        sweep_result = {"error": str(e), "checked": 0, "closed": 0}
    try:
        entry_result = run_squeeze_entry()
        opened = entry_result.get("opened", []) or []
        skipped = entry_result.get("skipped", []) or []
        print(f"  entry: opened={len(opened)} skipped={len(skipped)}")
    except Exception as e:
        print(f"  [entry-warn] {type(e).__name__}: {e}")
        entry_result = {"error": str(e), "opened": []}
    return {
        "status":   "ok",
        "strategy": STRATEGY_MOMENTUM,
        "mode":     "squeeze_tick",
        "sweep":    sweep_result,
        "entry":    entry_result,
    }

def build_squeeze_contract(symbol: str, direction: str, target_delta: float,
                           current_price: float) -> dict | None:
    """Phase D: pull the options chain at 3-14 DTE, filter to delta in
    [0.55, 0.65], pick the contract closest to target_delta. Returns the
    chosen contract dict or None if nothing passes filters."""
    today = date.today()
    gte = (today + timedelta(days=SQUEEZE_DTE_MIN)).isoformat()
    lte = (today + timedelta(days=SQUEEZE_DTE_MAX)).isoformat()
    contract_type = "call" if direction == "bullish" else "put"
    contracts = get_options_contracts(symbol, gte, lte, contract_type=contract_type)
    if not contracts:
        print(f"  [contract] {symbol}: no {contract_type}s in {SQUEEZE_DTE_MIN}-{SQUEEZE_DTE_MAX} DTE")
        return None

    snaps = get_option_snapshots([c["symbol"] for c in contracts])
    candidates = []
    rej = {"delta": 0, "bidask": 0, "missing_quote": 0}
    for c in contracts:
        snap   = snaps.get(c["symbol"], {})
        greeks = snap.get("greeks") or {}
        quote  = snap.get("latestQuote") or {}
        delta = greeks.get("delta")
        bid = float(quote.get("bp") or 0); ask = float(quote.get("ap") or 0)
        if delta is None or bid <= 0 or ask <= 0:
            rej["missing_quote"] += 1; continue
        abs_delta = abs(float(delta))
        if not (SQUEEZE_DELTA_MIN <= abs_delta <= SQUEEZE_DELTA_MAX):
            rej["delta"] += 1; continue
        mid = (bid + ask) / 2
        if mid <= 0: rej["missing_quote"] += 1; continue
        ba_pct = (ask - bid) / mid
        if ba_pct > 0.10:  # bid-ask spread must be < 10% of mid
            rej["bidask"] += 1; continue
        strike = float(c["strike_price"])
        dte = (date.fromisoformat(c["expiration_date"]) - today).days
        candidates.append({
            "symbol":  c["symbol"],
            "strike":  strike,
            "expiry":  c["expiration_date"],
            "dte":     dte,
            "delta":   float(delta),
            "bid":     bid,
            "ask":     ask,
            "mid":     round(mid, 2),
            "iv":      float(greeks.get("iv") or 0),
        })

    print(f"  [contract] {symbol}: {len(contracts)} chain, {len(candidates)} passed "
          f"(rej delta={rej['delta']}, bidask={rej['bidask']}, missing={rej['missing_quote']})")
    if not candidates:
        return None

    # Pick the contract whose |delta| is closest to target_delta, then closest to mid-DTE
    target_dte_mid = (SQUEEZE_DTE_MIN + SQUEEZE_DTE_MAX) / 2
    candidates.sort(key=lambda x: (abs(abs(x["delta"]) - target_delta),
                                    abs(x["dte"] - target_dte_mid)))
    return candidates[0]

def _stalk_squeeze_fill(order_id: str, mid_price: float, ask_price: float,
                       max_retries: int = 3, sleep_s: float = 60.0):
    """Limit-order stalking: if not filled in 60 seconds, cancel and re-place
    $0.02 closer to the ask. Max 3 retries. Returns the final filled price
    or None if exhausted. Note: this blocks for up to ~3 minutes total.
    Acceptable inside a Lambda invocation (Lambda timeout is 15 min default)."""
    current_limit = mid_price
    for attempt in range(max_retries + 1):
        filled = _wait_for_fill(order_id, target=current_limit, tries=4, delay_s=sleep_s / 4)
        if filled is not None:
            return float(filled), current_limit
        # Cancel and re-place if we still have retries left
        if attempt >= max_retries:
            break
        try:
            cancel_order(order_id)
        except Exception:
            pass
        # Move $0.02 closer to the ask, but don't cross
        current_limit = min(round(current_limit + 0.02, 2), ask_price)
        if current_limit >= ask_price:
            break  # we've already nudged all the way to the ask
    return None, current_limit

def compute_squeeze_position_size(equity: float, premium_per_contract: float) -> int:
    """20% of current equity, integer contracts. Each contract costs premium*100."""
    if equity <= 0 or premium_per_contract <= 0:
        return 0
    budget = equity * SQUEEZE_POSITION_PCT
    cost_per_contract = premium_per_contract * 100
    return max(int(budget // cost_per_contract), 0)

def _execute_squeeze_open(decision: dict, contract: dict, current_price: float) -> dict | None:
    """Place buy-to-open with stalking. Size at 20% of current equity. Save
    spread with full state (original_qty, current_qty=original, tiers untriggered)
    so the exit sweep can manage the scale-out ladder."""
    underlying = decision["symbol"]
    direction  = decision["direction"]
    score      = int(decision.get("claude_score") or 0)

    # Pull current equity from Alpaca for dynamic sizing
    try:
        acct = get_account()
        equity = float(acct.get("equity") or 0)
    except Exception as e:
        print(f"  [squeeze-acct-fail] {type(e).__name__}: {e}")
        return None
    if equity <= 0:
        print(f"  [skip] {underlying}: account equity unreadable")
        return None

    mid = float(contract["mid"])
    qty = compute_squeeze_position_size(equity, mid)
    if qty < 1:
        print(f"  [skip] {underlying}: 20% of ${equity:.0f} can't afford 1 contract at ${mid:.2f}")
        return None
    total_cost = mid * 100 * qty

    # Hard-cap concurrent positions
    budget_open = len(load_open_spreads())
    if budget_open >= SQUEEZE_MAX_POSITIONS:
        print(f"  [skip] {underlying}: at max positions ({budget_open}/{SQUEEZE_MAX_POSITIONS})")
        return None

    # Initial limit order at mid
    try:
        order = place_single_leg_order(
            symbol=contract["symbol"],
            side="buy",
            limit_price=mid,
            qty=qty,
            tif="day",
            position_intent="buy_to_open",
        )
    except Exception as e:
        print(f"  [squeeze-order-fail] {underlying}: {type(e).__name__}: {e}")
        log_event("error", underlying, {"stage": "squeeze_open", "error": str(e)}, decision.get("claude_reason", ""))
        return None

    # Stalk the fill
    filled, final_limit = _stalk_squeeze_fill(order["id"], mid_price=mid, ask_price=contract["ask"])
    if filled is None:
        print(f"  [squeeze-no-fill] {underlying}: order {order['id']} never filled after 3 retries - cancelled")
        try: cancel_order(order["id"])
        except Exception: pass
        log_event("error", underlying, {"stage": "squeeze_no_fill", "order_id": order["id"]}, decision.get("claude_reason", ""))
        return None

    final_premium = round(abs(float(filled)), 2)
    final_total   = round(final_premium * 100 * qty, 2)

    # Stop & TP levels (per-contract premium prices)
    stop_value_pc  = round(final_premium * (1 - SQUEEZE_STOP_PCT), 2)
    tp1_value_pc   = round(final_premium * (1 + SQUEEZE_TP1_GAIN_PCT), 2)
    tp2_value_pc   = round(final_premium * (1 + SQUEEZE_TP2_GAIN_PCT), 2)

    spread = {
        "id":              f"SQ-{underlying}-{contract['expiry']}-{int(contract['strike'])}{('C' if direction=='bullish' else 'P')}-{uuid.uuid4().hex[:6]}",
        "strategy":        STRATEGY_MOMENTUM,    # reusing the slot
        "underlying":      underlying,
        "ticker":          underlying,
        "direction":       direction,
        "option_type":     "call" if direction == "bullish" else "put",
        "contract_symbol": contract["symbol"],
        "strike":          contract["strike"],
        "expiry":          contract["expiry"],
        "dte":             contract["dte"],
        "qty_original":    qty,
        "qty":             qty,                  # mutated as tranches close
        "premium_paid":    final_premium,
        "total_cost":      final_total,
        "stop_pct":        SQUEEZE_STOP_PCT,
        "stop_value":      stop_value_pc,
        "tp1_value":       tp1_value_pc,
        "tp2_value":       tp2_value_pc,
        "tp1_hit":         False,
        "tp2_hit":         False,
        "trailing":        False,                # flips true after TP2; runner trails 9-EMA
        "entry_price":     current_price,
        "delta_at_entry":  contract["delta"],
        "iv_at_entry":     contract["iv"],
        "open_order_id":   order["id"],
        "opened_at":       datetime.now(timezone.utc).isoformat(),
        "status":          "open",
        "score":           score,
        "claude_reason":   decision.get("claude_reason", ""),
        "kind":            "squeeze",            # to distinguish from old momentum debit-spread rows
    }
    save_open_spread(spread)
    log_event("open", underlying, spread, decision.get("claude_reason", ""))
    send_position_alert("open", spread)
    print(f"  [open-squeeze] {underlying} {direction} {contract['symbol']} qty={qty} @ ${final_premium:.2f}/c = ${final_total:.0f}")
    return spread

def run_squeeze_entry() -> dict:
    """End-to-end entry attempt: scan → score → contract → order. Replaces
    run_momentum_midday once Phase F lands. For now invokable via mode='squeeze_entry'."""
    print("[squeeze-entry] looking for entries")
    set_active_strategy(STRATEGY_MOMENTUM)

    regime = get_active_regime()
    if not regime.get("new_trades_allowed", True):
        return {"status": "ok", "mode": "squeeze_entry", "opened": [],
                "reason": f"regime {regime.get('regime')} blocks new trades"}

    scan_result = run_squeeze_scan()
    candidates = scan_result.get("candidates", []) or []
    approved   = [c for c in candidates
                  if c.get("claude_action") == "BUY"
                  and int(c.get("claude_score") or 0) >= SQUEEZE_SCORE_MIN]
    if not approved:
        return {"status": "ok", "mode": "squeeze_entry", "opened": [],
                "reason": f"no approved candidates (level1={scan_result.get('level1_passed', 0)}, "
                          f"l2_passed={scan_result.get('approved', 0)})"}

    open_tickers = {(s.get("underlying") or s.get("ticker") or "").upper()
                    for s in load_open_spreads()}
    opened: list[dict] = []
    skipped: list[dict] = []
    for cand in approved:
        sym = cand["symbol"].upper()
        if sym in open_tickers:
            skipped.append({"ticker": sym, "reason": "already open"}); continue
        target_delta = float(cand.get("strike_delta") or 0.60)
        contract = build_squeeze_contract(sym, cand["direction"], target_delta, cand["current_price"])
        if not contract:
            skipped.append({"ticker": sym, "reason": "no contract passed filters"}); continue
        spread = _execute_squeeze_open(cand, contract, cand["current_price"])
        if spread:
            opened.append(spread)
            open_tickers.add(sym)
        else:
            skipped.append({"ticker": sym, "reason": "execute failed"})
        if len(open_tickers) >= SQUEEZE_MAX_POSITIONS:
            break

    return {
        "status":   "ok",
        "strategy": STRATEGY_MOMENTUM,
        "mode":     "squeeze_entry",
        "opened":   [s["id"] for s in opened],
        "skipped":  skipped,
        "scan_l1":  scan_result.get("level1_passed", 0),
        "scan_approved": scan_result.get("approved", 0),
    }

def run_squeeze_scan() -> dict:
    """Scan for TTM Squeeze + EMA cross + VWAP signals across the full universe.
    Universe = permanent anchors + today's Alpaca most-actives (dynamic expansion).
    Level 1 passers go to Claude scoring; only BUY >= 7 result in entries."""
    print("[squeeze-scan] starting scan")
    set_active_strategy(STRATEGY_MOMENTUM)
    regime = get_active_regime()
    print(f"  regime={regime.get('regime')} new_trades={regime.get('new_trades_allowed')}")

    # Build the universe: permanent anchors + today's dynamic movers
    universe, meta = build_squeeze_universe()

    # Step 1: Level 1 technical check on each ticker
    signals = []
    for sym in universe:
        sig = compose_squeeze_signal(sym, meta_override=meta)
        signals.append(sig)
        if sig.get("level1_pass"):
            print(f"  [L1-PASS] {sym} {sig['direction']} (price ${sig['current_price']}, VWAP ${sig.get('vwap')})")
        else:
            print(f"  [L1-skip] {sym}: {sig.get('reason') or 'gates not aligned'}")

    candidates_l1 = [s for s in signals if s.get("level1_pass")]
    if not candidates_l1:
        return {
            "status":           "ok",
            "strategy":         STRATEGY_MOMENTUM,
            "mode":             "squeeze_scan",
            "level1_passed":    0,
            "candidates":       [],
            "all_signals":      signals,
        }

    # Step 2: pull news + Claude eval for each L1 passer
    try:
        finnhub_key = get_secret("/trading-bot/finnhub-key")
    except Exception as e:
        print(f"  [news-key-fail] {e}")
        finnhub_key = None

    candidates = []
    for sig in candidates_l1:
        news = fetch_finnhub_news(sig["symbol"], finnhub_key) if finnhub_key else []
        verdict = claude_squeeze_eval(sig, news, regime=regime)
        cand = {
            **sig,
            "news_headlines": [n.get("headline", "")[:100] for n in news[:3]],
            "claude_action":  verdict.get("action"),
            "claude_score":   verdict.get("score"),
            "strike_delta":   verdict.get("strike_delta"),
            "claude_reason":  verdict.get("reason"),
        }
        print(f"  [L2] {sig['symbol']}: action={verdict.get('action')} score={verdict.get('score')} - {verdict.get('reason')}")
        candidates.append(cand)

    approved = [c for c in candidates if c.get("claude_action") == "BUY" and int(c.get("claude_score") or 0) >= SQUEEZE_SCORE_MIN]

    return {
        "status":         "ok",
        "strategy":       STRATEGY_MOMENTUM,
        "mode":           "squeeze_scan",
        "level1_passed":  len(candidates_l1),
        "claude_evaled":  len(candidates),
        "approved":       len(approved),
        "candidates":     candidates,
        "all_signals":    signals,
    }

# ---- QQQ 0-5 DTE Directional Strategy -------------------------------------
# Uses the STRATEGY_MOMENTUM slot (STRAT2# DDB prefix, strategy2 Alpaca keys).
# Scans every 5 min. Enters on VWAP + EMA9/21 + volume alignment, scored by
# Claude. Hard stop -35%. Claude actively manages exits on every sweep.
# Sizing: 5% of QQQ_EFFECTIVE_EQUITY ($5k base) = $250 max premium per trade.
# Account holds $30k to clear PDT threshold; all math uses the $5k base only.

QQQ_UNIVERSE            = ["QQQ"]          # SPY can be added later
QQQ_DTE_MIN             = 0
QQQ_DTE_MAX             = 5
QQQ_DELTA_MIN           = 0.40
QQQ_DELTA_MAX           = 0.55
QQQ_EFFECTIVE_EQUITY    = 5000.0           # sizing base (not the PDT buffer)
QQQ_POSITION_PCT        = 0.10            # 10% of effective equity per trade -> $500
QQQ_STOP_PCT            = 0.35            # -35% of premium = hard stop (~$175 max loss)
QQQ_MAX_POSITIONS       = 2
QQQ_MAX_ENTRIES_PER_DAY = 3               # never grind more than 3 entries/day
QQQ_SCORE_MIN           = 7
QQQ_BA_MAX_PCT          = 0.08            # max 8% bid-ask spread (QQQ is liquid)
# Entry windows (ET): (start_h, start_m, end_h, end_m)
QQQ_ENTRY_WINDOWS       = [(9, 45, 11, 30), (13, 0, 14, 15)]
# Force-close any 0-DTE position at or after 15:15 ET — gamma risk past this point
QQQ_ZERO_DTE_CUTOFF     = (15, 15)

SYSTEM_QQQ_STRATEGIST = """You are a disciplined QQQ short-term options trader. You evaluate a 5-minute technical signal and decide whether to enter a directional call or put position (0-5 DTE).

Score 0-10:
- 3 pts: EMA 9/21 strongly aligned on 5-min (not a fresh crossover, a confirmed trend)
- 3 pts: price clearly on the correct side of VWAP with separation, not straddling
- 2 pts: volume surge confirms the move (not low-conviction drift)
- 2 pts: regime supports the direction AND time-of-day is favorable (not lunch chop)

Only score >= 7 justifies entry. A weak QQQ setup with 0-5 DTE loses to theta and bid-ask every time. Be selective.

Reply JSON only: {"action":"BUY"|"SKIP","score":0-10,"reason":"<15 words>"}"""

SYSTEM_QQQ_EXIT = """You are actively managing an open QQQ short-term options position. Review every 5 minutes and decide whether to hold, partially exit, or fully exit.

Key rules:
- 0 DTE positions after 14:30: lean heavily toward FULL_EXIT unless gain is <20% (let it breathe) or >80% (take it)
- Volume dying + price stalling = take profit now, do not wait
- Price crossed back through VWAP against your direction = FULL_EXIT
- 9-EMA (1-min) broken against direction = PARTIAL_EXIT minimum
- Do not hold losers hoping for recovery — if momentum is gone, exit and reset

Reply JSON only: {"action":"HOLD"|"PARTIAL_EXIT"|"FULL_EXIT","exit_pct":0-100,"reason":"<15 words>"}"""

SYSTEM_QQQ_MORNING = """You are writing a pre-market briefing for a QQQ 0-5 DTE directional options trader.
Strategy: buy calls (bullish) or puts (bearish) on QQQ only. Entry requires EMA9 cross EMA21 on 5-min bars + price above/below VWAP + volume >1.3x 20-bar avg. Claude score >=7 required.

Write a brief note in EXACTLY this format (each label on its own line, colon after label):
QQQ: [current price and daily trend in one phrase]
BIAS: [BULLISH / BEARISH / NEUTRAL] - [one sentence why based on EMA and daily momentum]
WATCH: [the specific price action that would fire a trade today - be concrete, e.g. "break above VWAP with volume surge on 5-min bars"]
RISK: [one key risk or condition that would keep us out today]

Under 70 words total. No hedging. Data-driven."""

def claude_qqq_morning_note(summary: dict, vix: float | None, regime: dict) -> str:
    """Generate a pre-market context note for the QQQ strategy."""
    client = _anthropic_client()
    regime_str = f"{regime.get('regime','unknown')} / {regime.get('bias','unknown')}" if regime else "unknown"
    prompt = (
        f"VIX: {vix if vix is not None else 'n/a'}\n"
        f"Regime: {regime_str}\n"
        f"QQQ last: ${summary.get('last', 'n/a')}\n"
        f"EMA8: {summary.get('ema8', 'n/a')}  EMA21: {summary.get('ema21', 'n/a')}\n"
        f"RSI14: {summary.get('rsi14', 'n/a')}\n"
        f"5d range: {summary.get('range_5d_pct', 'n/a')}%"
    )
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=200,
        system=SYSTEM_QQQ_MORNING,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text.strip()

def run_qqq_morning() -> dict:
    """9:00 AM pre-market note for the QQQ strategy. Runs before market open."""
    set_active_strategy(STRATEGY_MOMENTUM)
    print("[qqq-morning] building pre-market note")
    vix     = get_vix()
    summary = summarize_ticker("QQQ")
    regime  = get_active_regime()
    note    = claude_qqq_morning_note(summary, vix, regime)
    save_note(note, vix, [summary])
    print(f"  [qqq-morning-note] ({len(note)} chars): {note[:120]}...")
    return {"status": "ok", "mode": "qqq_morning", "vix": vix, "note": note}

def get_5min_bars(symbol: str, days_back: int = 3) -> list[dict]:
    """5-minute OHLCV bars over the last N calendar days."""
    end_utc   = datetime.now(timezone.utc)
    start_utc = end_utc - timedelta(days=days_back)
    params = {
        "symbols":    symbol,
        "timeframe":  "5Min",
        "start":      start_utc.isoformat(),
        "end":        end_utc.isoformat(),
        "feed":       "iex",
        "limit":      500,
        "adjustment": "split",
    }
    r = http.get(
        f"{ALPACA_DATA_BASE}/v2/stocks/bars",
        headers=alpaca_headers(),
        params=params,
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    return r.json().get("bars", {}).get(symbol, [])

def _in_qqq_entry_window() -> bool:
    """True if current ET time falls inside a defined entry window."""
    now = datetime.now(ET)
    h, m = now.hour, now.minute
    for (sh, sm, eh, em) in QQQ_ENTRY_WINDOWS:
        start_min = sh * 60 + sm
        end_min   = eh * 60 + em
        now_min   = h  * 60 + m
        if start_min <= now_min <= end_min:
            return True
    return False

def _qqq_entries_today() -> int:
    """Count QQQ open events logged today — enforces the daily entry cap."""
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    try:
        resp = trade_tbl.query(
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": f"STRAT2#TRADE#{today}"},
        )
        return sum(1 for item in resp.get("Items", [])
                   if item.get("kind") == "open"
                   and str(item.get("symbol", "")).upper() in QQQ_UNIVERSE)
    except Exception:
        return 0

def compose_qqq_signal(symbol: str) -> dict:
    """5-min technical signal: EMA9/21 direction + VWAP alignment + volume surge.
    All three must agree for level1_pass=True."""
    try:
        bars = get_5min_bars(symbol, days_back=3)
    except Exception as e:
        return {"symbol": symbol, "level1_pass": False, "reason": f"bars-fail: {e}"}
    if not bars or len(bars) < 25:
        return {"symbol": symbol, "level1_pass": False,
                "reason": f"insufficient 5m bars ({len(bars) if bars else 0})"}

    closes  = [float(b.get("c") or 0) for b in bars]
    volumes = [int(b.get("v") or 0) for b in bars]
    if any(c <= 0 for c in closes[-25:]):
        return {"symbol": symbol, "level1_pass": False, "reason": "bad bar data"}

    ema9  = ema(closes, 9)
    ema21 = ema(closes, 21)
    if ema9 is None or ema21 is None:
        return {"symbol": symbol, "level1_pass": False, "reason": "ema undefined"}

    direction   = "bullish" if ema9 > ema21 else "bearish"
    current     = closes[-1]
    vwap        = compute_vwap_today(symbol)
    rsi_val     = rsi_14(closes)

    vwap_ok = True
    if vwap:
        vwap_ok = (current > vwap) if direction == "bullish" else (current < vwap)

    # Volume: current 5-min bar vs 20-bar rolling average (exclude current)
    if len(volumes) >= 22:
        avg_vol = sum(volumes[-22:-2]) / 20
        vol_ok  = volumes[-1] > avg_vol * 1.30
    else:
        avg_vol = 0; vol_ok = True   # neutral if not enough history

    level1_pass = vwap_ok and vol_ok
    reason = ("ok" if level1_pass else
              f"vwap_ok={vwap_ok} vol_ok={vol_ok}")

    return {
        "symbol":        symbol,
        "level1_pass":   level1_pass,
        "direction":     direction,
        "current_price": round(current, 2),
        "ema9":          round(ema9, 2),
        "ema21":         round(ema21, 2),
        "vwap":          round(vwap, 2) if vwap else None,
        "vwap_ok":       vwap_ok,
        "vol_ok":        vol_ok,
        "current_vol":   volumes[-1] if volumes else 0,
        "avg_vol_20":    round(avg_vol, 0),
        "rsi":           round(rsi_val, 1) if rsi_val else None,
        "reason":        reason,
    }

def claude_qqq_entry_eval(signal: dict, regime: dict) -> dict:
    """Claude scores the QQQ 5-min setup. Returns {action, score, reason}."""
    user_msg = (
        f"QQQ 5-MIN SIGNAL — {signal['direction'].upper()}\n"
        f"Price ${signal['current_price']} | EMA9 ${signal['ema9']} | EMA21 ${signal['ema21']}\n"
        f"VWAP ${signal.get('vwap','n/a')} | VWAP aligned: {signal['vwap_ok']}\n"
        f"Volume: {signal['current_vol']:,} vs 20-bar avg {signal['avg_vol_20']:,.0f} | "
        f"Vol surge: {signal['vol_ok']}\n"
        f"RSI: {signal.get('rsi','n/a')}\n"
        f"Regime: {regime.get('regime','?')} | Bias: {regime.get('bias','?')} | "
        f"New trades allowed: {regime.get('new_trades_allowed', True)}\n"
        f"Time ET: {datetime.now(ET).strftime('%H:%M')}"
    )
    try:
        client = _anthropic_client()
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=80,
            system=SYSTEM_QQQ_STRATEGIST,
            messages=[
                {"role": "user",      "content": user_msg},
                {"role": "assistant", "content": "{"},
            ],
        )
        raw = "{" + (msg.content[0].text if msg.content else "")
        result = _parse_json(raw, fallback={"action": "SKIP", "score": 0, "reason": "parse-fail"})
        result.setdefault("action", "SKIP")
        result.setdefault("score", 0)
        return result
    except Exception as e:
        print(f"  [qqq-entry-eval-fail] {type(e).__name__}: {e}")
        return {"action": "SKIP", "score": 0, "reason": f"claude error: {e}"}

def claude_qqq_exit_eval(spread: dict, current_value: float,
                          bars_1min: list[dict], regime: dict) -> dict:
    """Claude decides HOLD/PARTIAL_EXIT/FULL_EXIT for an open QQQ position.
    Reuses the same analysis pattern as Weeklies but with QQQ-aware prompt."""
    premium   = float(spread.get("premium_paid", 0) or 0)
    qty       = int(spread.get("qty", 1) or 1)
    direction = spread.get("direction", "bullish")
    pnl_pct   = round((current_value - premium) / premium * 100, 1) if premium > 0 else 0
    try:
        dte = (date.fromisoformat(spread["expiry"]) - date.today()).days
    except Exception:
        dte = "?"

    recent  = bars_1min[-20:] if len(bars_1min) >= 20 else bars_1min
    closes  = [round(float(b.get("c") or 0), 2) for b in recent]
    volumes = [int(b.get("v") or 0) for b in recent]
    if len(volumes) >= 8:
        avg_prior  = sum(volumes[:-5]) / max(len(volumes) - 5, 1)
        avg_recent = sum(volumes[-5:]) / 5
        vol_trend  = ("rising"  if avg_recent > avg_prior * 1.10 else
                      "falling" if avg_recent < avg_prior * 0.85 else "flat")
    else:
        vol_trend = "n/a"
    ema9_val     = ema(closes, 9) if len(closes) >= 9 else None
    price_vs_ema = ("above" if closes and ema9_val and closes[-1] > ema9_val else
                    "below" if closes and ema9_val else "n/a")
    vwap     = compute_vwap_today(spread.get("underlying") or "QQQ")
    vwap_str = f"${vwap:.2f}" if vwap else "n/a"

    user_msg = (
        f"QQQ {direction.upper()} {spread.get('option_type','').upper()} | "
        f"{spread.get('contract_symbol')} | qty={qty} | DTE={dte}\n"
        f"Entry ${premium:.2f} | Now ${current_value:.2f} | P&L {pnl_pct:+.1f}%\n"
        f"1-min closes (last {len(recent)}): {closes}\n"
        f"Volumes: {volumes}\n"
        f"Vol trend: {vol_trend} | vs 9-EMA: {price_vs_ema} | VWAP: {vwap_str}\n"
        f"Regime: {regime.get('regime','?')} | Time ET: {datetime.now(ET).strftime('%H:%M')}\n"
        f"HOLD, PARTIAL_EXIT, or FULL_EXIT?"
    )
    try:
        client = _anthropic_client()
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=80,
            system=SYSTEM_QQQ_EXIT,
            messages=[
                {"role": "user",      "content": user_msg},
                {"role": "assistant", "content": "{"},
            ],
        )
        raw    = "{" + (msg.content[0].text if msg.content else "")
        result = _parse_json(raw, fallback={"action": "HOLD", "exit_pct": 100, "reason": "parse-fail"})
        result.setdefault("action", "HOLD")
        result.setdefault("exit_pct", 100)
        return result
    except Exception as e:
        print(f"  [qqq-exit-eval-fail] {type(e).__name__}: {e}")
        return {"action": "HOLD", "exit_pct": 0, "reason": f"claude error: {e}"}

def build_qqq_contract(direction: str, current_price: float) -> dict | None:
    """Select the best QQQ option in 0-5 DTE, delta 0.40-0.55."""
    today = date.today()
    gte   = today.isoformat()
    lte   = (today + timedelta(days=QQQ_DTE_MAX)).isoformat()
    ctype = "call" if direction == "bullish" else "put"
    contracts = get_options_contracts("QQQ", gte, lte, contract_type=ctype)
    if not contracts:
        print(f"  [qqq-contract] no {ctype}s in 0-{QQQ_DTE_MAX} DTE")
        return None
    snaps      = get_option_snapshots([c["symbol"] for c in contracts])
    candidates = []
    for c in contracts:
        snap   = snaps.get(c["symbol"], {})
        greeks = snap.get("greeks") or {}
        quote  = snap.get("latestQuote") or {}
        delta  = greeks.get("delta")
        bid    = float(quote.get("bp") or 0)
        ask    = float(quote.get("ap") or 0)
        if delta is None or bid <= 0 or ask <= 0:
            continue
        abs_delta = abs(float(delta))
        if not (QQQ_DELTA_MIN <= abs_delta <= QQQ_DELTA_MAX):
            continue
        mid = (bid + ask) / 2
        if mid <= 0 or (ask - bid) / mid > QQQ_BA_MAX_PCT:
            continue
        dte = (date.fromisoformat(c["expiration_date"]) - today).days
        candidates.append({
            "symbol": c["symbol"],
            "strike": float(c["strike_price"]),
            "expiry": c["expiration_date"],
            "dte":    dte,
            "delta":  float(delta),
            "bid":    bid,
            "ask":    ask,
            "mid":    round(mid, 2),
            "iv":     float(greeks.get("iv") or 0),
        })
    if not candidates:
        return None
    # Prefer delta closest to 0.475 (center of band), break ties by lower DTE
    candidates.sort(key=lambda c: (round(abs(abs(c["delta"]) - 0.475), 3), c["dte"]))
    return candidates[0]

def compute_qqq_position_size(contract_mid: float) -> int:
    """Contracts to buy: 5% of QQQ_EFFECTIVE_EQUITY ($250 max premium)."""
    max_prem = QQQ_EFFECTIVE_EQUITY * QQQ_POSITION_PCT   # $250
    cost_per = contract_mid * 100
    if cost_per <= 0:
        return 0
    return max(1, int(max_prem // cost_per))

def _execute_qqq_open(signal: dict, contract: dict, score: int, reason: str) -> dict | None:
    """Place buy-to-open for QQQ position and persist to DDB."""
    direction = signal["direction"]
    mid       = float(contract["mid"])
    qty       = compute_qqq_position_size(mid)
    if qty <= 0:
        print(f"  [qqq-skip] contract mid ${mid:.2f} too expensive for $250 budget")
        return None
    total_cost = round(mid * 100 * qty, 2)
    try:
        order = place_single_leg_order(
            symbol=contract["symbol"],
            side="buy",
            limit_price=mid,
            qty=qty,
            tif="day",
            position_intent="buy_to_open",
        )
    except Exception as e:
        print(f"  [qqq-order-fail] {type(e).__name__}: {e}")
        log_event("error", "QQQ", {"stage": "qqq_open", "error": str(e)}, reason)
        return None
    filled = _wait_for_fill(order["id"], target=mid)
    if filled is None:
        print(f"  [qqq-fill-timeout] cancelling {order.get('id')} - no DDB record written")
        try:
            cancel_order(order.get("id", ""))
        except Exception:
            pass
        log_event("error", "QQQ",
                  {"stage": "qqq_open_no_fill", "order_id": order.get("id"),
                   "contract": contract["symbol"]}, reason)
        return None
    final_premium = round(abs(float(filled)), 2)
    stop_value    = round(final_premium * (1 - QQQ_STOP_PCT), 2)
    spread = {
        "id":              f"QQ-{contract['expiry']}-{int(contract['strike'])}"
                           f"{'C' if direction=='bullish' else 'P'}-{uuid.uuid4().hex[:6]}",
        "strategy":        STRATEGY_MOMENTUM,
        "kind":            "qqq",
        "underlying":      "QQQ",
        "ticker":          "QQQ",
        "direction":       direction,
        "option_type":     "call" if direction == "bullish" else "put",
        "contract_symbol": contract["symbol"],
        "strike":          contract["strike"],
        "expiry":          contract["expiry"],
        "dte":             contract["dte"],
        "qty":             qty,
        "premium_paid":    final_premium,
        "total_cost":      round(final_premium * 100 * qty, 2),
        "stop_loss_value": stop_value,
        "stop_loss_pct":   QQQ_STOP_PCT,
        "entry_price":     signal["current_price"],
        "delta_at_entry":  contract["delta"],
        "iv_at_entry":     contract["iv"],
        "open_order_id":   order["id"],
        "opened_at":       datetime.now(timezone.utc).isoformat(),
        "status":          "open",
        "score":           score,
        "rationale":       reason,
    }
    save_open_spread(spread)
    log_event("open", "QQQ", spread, reason)
    send_position_alert("open", spread)
    print(f"  [qqq-open] {contract['symbol']} {direction} qty={qty} "
          f"@ ${final_premium:.2f}/c = ${total_cost:.0f} | stop=${stop_value:.2f}")
    return spread

def _current_qqq_value(spread: dict) -> float | None:
    """Current mid price for an open QQQ options contract."""
    snaps = get_option_snapshots([spread["contract_symbol"]])
    snap  = snaps.get(spread["contract_symbol"], {}) or {}
    quote = snap.get("latestQuote") or {}
    try:
        bid = float(quote.get("bp") or 0)
        ask = float(quote.get("ap") or 0)
        return round((bid + ask) / 2, 2) if bid > 0 and ask > 0 else None
    except Exception:
        return None

def _evaluate_qqq_hard_exit(spread: dict, current_value: float | None,
                             today: "date", now_et: datetime) -> tuple:
    """Hard exits only — stop loss, 0-DTE cutoff, expiry. No Claude."""
    premium = float(spread.get("premium_paid", 0) or 0)
    if premium <= 0 or current_value is None:
        return ("hold", "no basis or quote")
    pnl_pct = (current_value - premium) / premium
    # Stop loss
    if current_value <= float(spread.get("stop_loss_value", 0) or 0):
        return ("close", f"stop loss {pnl_pct*100:.0f}% (>{QQQ_STOP_PCT*100:.0f}% max)")
    # 0-DTE force close after cutoff time
    try:
        dte = (date.fromisoformat(spread["expiry"]) - today).days
    except Exception:
        dte = 1
    if dte <= 0:
        ch, cm = QQQ_ZERO_DTE_CUTOFF
        if now_et.hour > ch or (now_et.hour == ch and now_et.minute >= cm):
            return ("close", f"0-DTE force close at {now_et.strftime('%H:%M')} ET")
    if dte < 0:
        return ("close", "past expiry")
    return ("hold", f"pnl {pnl_pct*100:+.0f}% dte {dte}")

def _close_qqq_position(spread: dict, reason: str, current_value: float | None) -> dict:
    """Sell-to-close full QQQ position and archive."""
    qty = int(spread.get("qty", 1) or 1)
    limit = max(round((current_value or 0.01) * 0.92, 2), 0.01)
    try:
        order = place_single_leg_order(
            symbol=spread["contract_symbol"],
            side="sell",
            limit_price=limit,
            qty=qty,
            tif="day",
            position_intent="sell_to_close",
        )
    except Exception as e:
        err_str = str(e)
        if "position intent mismatch" in err_str or "inferred: sell_to_open" in err_str:
            print(f"  [qqq-phantom] {spread.get('id')} - auto-archiving")
            outcome = {"exit_premium": 0, "exit_total": 0, "pnl_dollars": 0,
                       "pnl_pct": 0, "exit_reason": "phantom position (auto-archived)"}
            archive_spread(spread, outcome)
            log_event("close", "QQQ", {**spread, **outcome}, "phantom")
            return {**outcome, "phantom": True}
        print(f"  [qqq-close-fail] {spread.get('id')}: {type(e).__name__}: {e}")
        log_event("error", "QQQ", {"stage": "qqq_close", "error": str(e)}, reason)
        return {"pnl_dollars": 0, "exit_reason": f"{reason} (order error)", "error": True}
    filled        = _wait_for_fill(order.get("id", ""), target=limit)
    final_premium = round(abs(float(filled if filled else limit)), 2)
    premium_paid  = float(spread.get("premium_paid", 0) or 0)
    pnl_dollars   = round((final_premium - premium_paid) * 100 * qty, 2)
    pnl_pct       = ((final_premium - premium_paid) / premium_paid * 100) if premium_paid > 0 else 0
    outcome = {
        "exit_premium": final_premium,
        "exit_total":   round(final_premium * 100 * qty, 2),
        "pnl_dollars":  pnl_dollars,
        "pnl_pct":      round(pnl_pct, 1),
        "exit_reason":  reason,
    }
    archive_spread(spread, outcome)
    log_event("close", "QQQ", {**spread, **outcome}, reason)
    send_position_alert("close", spread, outcome)
    print(f"  [qqq-close] {spread.get('id')} @ ${final_premium:.2f}/c "
          f"pnl=${pnl_dollars:+.0f} ({pnl_pct:+.1f}%) | {reason}")
    return outcome

def _partial_close_qqq_position(spread: dict, close_qty: int,
                                 reason: str, current_value: float | None) -> dict | None:
    """Partially close a QQQ position. Updates DDB qty, does not archive."""
    if close_qty <= 0:
        return None
    limit = max(round((current_value or 0.01) * 0.92, 2), 0.01)
    try:
        order = place_single_leg_order(
            symbol=spread["contract_symbol"],
            side="sell",
            limit_price=limit,
            qty=close_qty,
            tif="day",
            position_intent="sell_to_close",
        )
    except Exception as e:
        print(f"  [qqq-partial-fail] {type(e).__name__}: {e}")
        return None
    filled        = _wait_for_fill(order.get("id", ""), target=limit)
    final_premium = round(abs(float(filled if filled else limit)), 2)
    premium_paid  = float(spread.get("premium_paid", 0) or 0)
    pnl_dollars   = round((final_premium - premium_paid) * 100 * close_qty, 2)
    pnl_pct       = round((final_premium - premium_paid) / premium_paid * 100, 1) if premium_paid > 0 else 0
    remaining     = int(spread.get("qty", close_qty) or close_qty) - close_qty
    try:
        trade_tbl.update_item(
            Key={"pk": f"{_pk_prefix()}SPREAD#OPEN", "sk": spread["id"]},
            UpdateExpression="SET qty = :q",
            ExpressionAttributeValues={":q": remaining},
        )
    except Exception as ue:
        print(f"  [qqq-qty-update-fail] {type(ue).__name__}: {ue}")
    outcome = {"closed_qty": close_qty, "remaining": remaining,
               "exit_premium": final_premium, "pnl_dollars": pnl_dollars, "pnl_pct": pnl_pct}
    log_event("partial_close", "QQQ", {**spread, **outcome}, reason)
    print(f"  [qqq-partial] sold {close_qty}c @ ${final_premium:.2f} "
          f"pnl=${pnl_dollars:+.0f} | {remaining} remaining")
    return outcome

def _sweep_qqq_exits() -> dict:
    """Two-tier exit sweep for open QQQ positions.
    Tier 1: hard stops + 0-DTE cutoff (pure code).
    Tier 2: Claude active management for everything else."""
    set_active_strategy(STRATEGY_MOMENTUM)
    opens = [s for s in load_open_spreads() if s.get("kind") == "qqq"]
    if not opens:
        return {"checked": 0, "closed": 0, "partial": 0, "decisions": []}
    today   = date.today()
    now_et  = datetime.now(ET)
    regime  = get_active_regime()
    closed  = 0
    partial = 0
    decisions = []
    for s in opens:
        value = _current_qqq_value(s)
        # Tier 1: hard exits
        action, reason = _evaluate_qqq_hard_exit(s, value, today, now_et)
        if action == "close":
            print(f"  [qqq-hard-stop] {s.get('id')} - {reason}")
            outcome = _close_qqq_position(s, reason, value)
            decisions.append({"id": s.get("id"), "tier": 1, "action": "close",
                               "reason": reason, "current_value": value})
            if outcome and not outcome.get("error") and not outcome.get("phantom"):
                closed += 1
            continue
        # Tier 2: Claude
        if value is None:
            decisions.append({"id": s.get("id"), "tier": 2, "action": "hold",
                               "reason": "no live quote"})
            continue
        try:
            bars_1min = _get_exit_bars("QQQ", n=25)
            verdict   = claude_qqq_exit_eval(s, value, bars_1min, regime)
        except Exception as e:
            print(f"  [qqq-claude-exit-warn] {type(e).__name__}: {e}")
            decisions.append({"id": s.get("id"), "tier": 2, "action": "hold",
                               "reason": f"claude-error: {e}"})
            continue
        act     = (verdict.get("action") or "HOLD").upper()
        c_reason = verdict.get("reason", "")
        pnl_pct  = round((value - float(s.get("premium_paid", 0) or 0))
                         / max(float(s.get("premium_paid", 1) or 1), 0.01) * 100, 1)
        print(f"  [qqq-claude] {s.get('id')} P&L {pnl_pct:+.1f}% -> {act}: {c_reason}")
        decisions.append({"id": s.get("id"), "tier": 2, "action": act,
                          "reason": c_reason, "current_value": value, "pnl_pct": pnl_pct})
        if act == "FULL_EXIT":
            outcome = _close_qqq_position(s, f"Claude: {c_reason}", value)
            if outcome and not outcome.get("error"):
                closed += 1
        elif act == "PARTIAL_EXIT":
            exit_pct  = max(25, min(75, int(verdict.get("exit_pct") or 50)))
            total_qty = int(s.get("qty", 1) or 1)
            close_qty = max(1, round(total_qty * exit_pct / 100))
            if close_qty >= total_qty:
                outcome = _close_qqq_position(s, f"Claude partial->full: {c_reason}", value)
                if outcome and not outcome.get("error"):
                    closed += 1
            else:
                outcome = _partial_close_qqq_position(s, close_qty,
                                                       f"Claude {exit_pct}%: {c_reason}", value)
                if outcome:
                    partial += 1
    return {"checked": len(opens), "closed": closed, "partial": partial, "decisions": decisions}

def run_qqq_scan() -> dict:
    """Scan QQQ for a 5-min directional signal. Only enters during defined windows.
    Always returns immediately if not in an entry window."""
    set_active_strategy(STRATEGY_MOMENTUM)
    # Gate 1: entry window
    if not _in_qqq_entry_window():
        return {"status": "ok", "mode": "qqq_scan", "entered": False,
                "reason": f"outside entry windows {datetime.now(ET).strftime('%H:%M')}"}
    # Gate 2: regime
    regime = get_active_regime()
    if not regime.get("new_trades_allowed", True):
        return {"status": "ok", "mode": "qqq_scan", "entered": False,
                "reason": f"regime {regime.get('regime')} blocks new trades"}
    # Gate 3: position cap
    opens = [s for s in load_open_spreads() if s.get("kind") == "qqq"]
    if len(opens) >= QQQ_MAX_POSITIONS:
        return {"status": "ok", "mode": "qqq_scan", "entered": False,
                "reason": f"max positions ({QQQ_MAX_POSITIONS}) reached"}
    # Gate 4: daily entry cap
    entries_today = _qqq_entries_today()
    if entries_today >= QQQ_MAX_ENTRIES_PER_DAY:
        return {"status": "ok", "mode": "qqq_scan", "entered": False,
                "reason": f"daily cap {QQQ_MAX_ENTRIES_PER_DAY} entries reached"}

    open_tickers = {s.get("underlying", "").upper() for s in opens}
    opened = []
    for sym in QQQ_UNIVERSE:
        if sym in open_tickers:
            continue
        sig = compose_qqq_signal(sym)
        if not sig.get("level1_pass"):
            print(f"  [qqq-l1-skip] {sym}: {sig.get('reason')}")
            continue
        print(f"  [qqq-l1-pass] {sym} {sig['direction']} price=${sig['current_price']}")
        verdict = claude_qqq_entry_eval(sig, regime)
        score   = int(verdict.get("score") or 0)
        action  = (verdict.get("action") or "SKIP").upper()
        reason  = verdict.get("reason", "")
        print(f"  [qqq-claude] {sym}: {action} score={score} - {reason}")

        now_et  = datetime.now(ET).strftime("%H:%M")
        dir_str = sig["direction"].upper()

        if action != "BUY" or score < QQQ_SCORE_MIN:
            # Save a note explaining why the signal was evaluated but not taken
            note_body = (
                f"SIGNAL: {sym} {dir_str} at ${sig['current_price']:.2f} ({now_et} ET)\n"
                f"CLAUDE: SKIP score={score}/10 - {reason}\n"
                f"REASON: {'Score below threshold (need 7+)' if score < QQQ_SCORE_MIN else 'Claude said skip'}"
            )
            save_note(note_body, None, [])
            continue

        contract = build_qqq_contract(sig["direction"], sig["current_price"])
        if not contract:
            print(f"  [qqq-no-contract] {sym}: no contract passed filters")
            note_body = (
                f"SIGNAL: {sym} {dir_str} at ${sig['current_price']:.2f} ({now_et} ET)\n"
                f"CLAUDE: BUY score={score}/10 - {reason}\n"
                f"REASON: No valid contract found in 0-5 DTE, delta 0.40-0.55 band"
            )
            save_note(note_body, None, [])
            continue

        spread = _execute_qqq_open(sig, contract, score, reason)
        if spread:
            opened.append(spread["id"])
            open_tickers.add(sym)
            entries_today += 1
            note_body = (
                f"SIGNAL: {sym} {dir_str} at ${sig['current_price']:.2f} ({now_et} ET)\n"
                f"CLAUDE: BUY score={score}/10 - {reason}\n"
                f"TRADE: Opened {contract.get('symbol','?')} x{spread.get('qty',1)} @ ${spread.get('premium_paid',0):.2f}"
            )
            save_note(note_body, None, [])
        if len(open_tickers) >= QQQ_MAX_POSITIONS or entries_today >= QQQ_MAX_ENTRIES_PER_DAY:
            break
    return {"status": "ok", "mode": "qqq_scan", "entered": bool(opened),
            "opened": opened, "entries_today": entries_today}

def run_qqq_tick() -> dict:
    """Every-5-min tick: always sweep exits, scan for entries only in windows."""
    set_active_strategy(STRATEGY_MOMENTUM)
    sweep_result = {"checked": 0, "closed": 0, "partial": 0}
    scan_result  = {"entered": False}
    try:
        sweep_result = _sweep_qqq_exits()
        print(f"  [qqq-sweep] checked={sweep_result['checked']} "
              f"closed={sweep_result['closed']} partial={sweep_result.get('partial',0)}")
    except Exception as e:
        print(f"  [qqq-sweep-warn] {type(e).__name__}: {e}")
    try:
        scan_result = run_qqq_scan()
    except Exception as e:
        print(f"  [qqq-scan-warn] {type(e).__name__}: {e}")
    return {"status": "ok", "strategy": STRATEGY_MOMENTUM, "mode": "qqq_tick",
            "sweep": sweep_result, "scan": scan_result}

# ---- end QQQ strategy -------------------------------------------------------

def get_earnings_dates() -> dict:
    """Reads {ticker: 'YYYY-MM-DD'} JSON from SSM. Empty dict if not yet populated.
    Stored at /trading-bot/earnings-calendar - refreshed weekly by run_refresh_earnings()."""
    try:
        raw = get_secret("/trading-bot/earnings-calendar")
        return json.loads(raw)
    except Exception as e:
        print(f"  [earnings-warn] could not load calendar: {type(e).__name__}: {e}")
        return {}

# ---- Earnings refresh (Finnhub) --------------------------------------------

FINNHUB_BASE = "https://finnhub.io/api/v1"

def _classify_econ_event(event_name: str) -> str | None:
    """Map a Finnhub event name to our catalyst type. None = skip."""
    n = (event_name or "").lower()
    if "cpi" in n or "consumer price" in n:                                return "CPI"
    if "ppi" in n or "producer price" in n:                                return "PPI"
    if any(s in n for s in ("nonfarm payroll", "non-farm payroll", "nfp")): return "NFP"
    if "fomc" in n or "fed funds" in n or "federal reserve" in n:          return "FOMC"
    if "interest rate decision" in n:                                      return "FOMC"
    if "gdp" in n:                                                         return "GDP"
    if "retail sales" in n:                                                return "RETAIL"
    if "ism" in n and ("manufacturing" in n or "services" in n):           return "ISM"
    if "unemployment rate" in n:                                           return "NFP"   # often paired
    return None

def fetch_finnhub_economic_calendar(key: str, days_ahead: int = 45) -> list[dict]:
    """Pull high-impact US economic events from Finnhub free tier. Returns
    list of catalyst-shaped dicts tagged source='auto' so the merge logic
    can preserve manual entries."""
    today  = date.today()
    end_d  = today + timedelta(days=days_ahead)
    try:
        r = http.get(
            f"{FINNHUB_BASE}/calendar/economic",
            params={"from": today.isoformat(), "to": end_d.isoformat(), "token": key},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        events = r.json().get("economicCalendar") or []
    except Exception as e:
        print(f"  [econ-cal-warn] {type(e).__name__}: {e}")
        return []

    out = []
    seen_keys: set[str] = set()  # dedupe (date+type) — Finnhub sometimes lists CPI YoY + CPI MoM separately
    for ev in events:
        if (ev.get("country") or "").upper() != "US":
            continue
        if (ev.get("impact") or "").lower() not in ("high", "medium"):
            continue
        time_str = ev.get("time") or ""
        if not time_str:
            continue
        d_str = time_str[:10]
        event_name = (ev.get("event") or "").strip()
        cat_type = _classify_econ_event(event_name)
        if not cat_type:
            continue
        dedupe_key = f"{d_str}-{cat_type}"
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        out.append({
            "date":        d_str,
            "type":        cat_type,
            "ticker":      "SPY",   # macro events affect broad market
            "description": f"{event_name} ({time_str[11:16]} ET-ish)" if len(time_str) > 11 else event_name,
            "source":      "auto",
        })
    print(f"  [econ-cal] pulled {len(out)} unique high-impact US events for next {days_ahead}d")
    return out

def merge_econ_into_catalyst_calendar(auto_events: list[dict]) -> dict:
    """Replace existing source='auto' entries in the catalyst calendar with
    fresh data while preserving manually-added entries (no source tag, or
    source='manual'). Writes back to SSM."""
    try:
        existing = get_macro_catalysts() or []
    except Exception as e:
        print(f"  [merge-read-warn] {type(e).__name__}: {e}")
        existing = []
    manual = [e for e in existing if e.get("source") not in ("auto",)]
    combined = manual + auto_events
    try:
        ssm.put_parameter(
            Name="/trading-bot/catalyst-calendar",
            Value=json.dumps(combined),
            Type="String",
            Overwrite=True,
        )
        _secret_cache.pop("/trading-bot/catalyst-calendar", None)
    except Exception as e:
        print(f"  [merge-write-fail] {type(e).__name__}: {e}")
        return {"manual": len(manual), "auto": len(auto_events), "saved": False, "error": str(e)}
    print(f"  [merge] catalyst calendar = {len(manual)} manual + {len(auto_events)} auto = {len(combined)} total")
    return {"manual": len(manual), "auto": len(auto_events), "saved": True, "total": len(combined)}

def fetch_next_earnings(symbol: str, key: str, days_ahead: int = 90) -> str | None:
    """Return the next upcoming earnings date for one ticker as 'YYYY-MM-DD',
    or None if no earnings are scheduled within the window (true for ETFs)."""
    today = date.today()
    end   = today + timedelta(days=days_ahead)
    try:
        r = http.get(
            f"{FINNHUB_BASE}/calendar/earnings",
            params={"from": today.isoformat(), "to": end.isoformat(),
                    "symbol": symbol, "token": key},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        items = r.json().get("earningsCalendar", []) or []
    except Exception as e:
        print(f"  [finnhub-warn] {symbol}: {type(e).__name__}: {e}")
        return None
    if not items:
        return None
    # Sort by date ascending and take the soonest
    items.sort(key=lambda x: x.get("date") or "9999-12-31")
    return items[0].get("date")

def run_refresh_earnings() -> dict:
    """Pull next earnings date for each ticker in the momentum universe and
    persist as JSON to SSM. Scheduled weekly (Sunday)."""
    print("[refresh-earnings] fetching from Finnhub")
    try:
        key = get_secret("/trading-bot/finnhub-key")
    except Exception as e:
        msg = f"finnhub key missing in SSM: {type(e).__name__}: {e}"
        print(f"  [refresh-fail] {msg}")
        return {"status": "error", "error": msg}

    out: dict[str, str] = {}
    for sym in get_momentum_universe():
        d = fetch_next_earnings(sym, key)
        if d:
            out[sym] = d
            print(f"  {sym}: next earnings {d}")
        else:
            print(f"  {sym}: no earnings in window (ETF or far out)")

    try:
        ssm.put_parameter(
            Name="/trading-bot/earnings-calendar",
            Value=json.dumps(out),
            Type="SecureString",
            Overwrite=True,
        )
        # Bust the in-process cache so the next read sees fresh data.
        _secret_cache.pop("/trading-bot/earnings-calendar", None)
    except Exception as e:
        msg = f"ssm write failed: {type(e).__name__}: {e}"
        print(f"  [refresh-fail] {msg}")
        return {"status": "error", "error": msg, "fetched": out}

    print(f"[refresh-earnings] saved {len(out)} dates to SSM")

    # Also auto-pull macro economic calendar (CPI/FOMC/NFP/PPI/etc.) and
    # merge into /trading-bot/catalyst-calendar, preserving manual entries.
    macro_result = {"saved": False}
    try:
        auto_events = fetch_finnhub_economic_calendar(key, days_ahead=45)
        macro_result = merge_econ_into_catalyst_calendar(auto_events)
    except Exception as e:
        print(f"  [econ-merge-warn] {type(e).__name__}: {e}")
        macro_result = {"saved": False, "error": str(e)}

    return {
        "status":   "ok",
        "mode":     "refresh_earnings",
        "earnings": out,
        "macro":    macro_result,
    }

def summarize_ticker(symbol: str) -> dict:
    """Pull daily bars + quote, compute indicators. Used by morning + midday."""
    bars = get_daily_bars(symbol, limit=60)
    closes = [float(b["c"]) for b in bars]
    last = closes[-1] if closes else None
    return {
        "symbol": symbol,
        "last":   last,
        "ema8":   ema(closes[-30:], 8)  if len(closes) >= 30 else None,
        "ema21":  ema(closes[-40:], 21) if len(closes) >= 40 else None,
        "rsi14":  rsi_14(closes[-30:]),
        "range_5d_pct": (
            round((max(closes[-5:]) - min(closes[-5:])) / min(closes[-5:]) * 100, 2)
            if len(closes) >= 5 else None
        ),
    }

# ---- DynamoDB: spread state + notes ----------------------------------------
#
# Both strategies share one table. Momentum rows get a "STRAT2#" prefix on the
# partition key so credit_spread queries (no prefix) and momentum queries
# (with prefix) never cross-contaminate. The active strategy at request time
# decides which prefix to use, so the helpers below stay strategy-agnostic.

def _pk_prefix() -> str:
    if _ACTIVE_STRATEGY == STRATEGY_NAKED:    return "STRAT3#"
    if _ACTIVE_STRATEGY == STRATEGY_MOMENTUM: return "STRAT2#"
    return ""

def save_open_spread(spread: dict) -> None:
    trade_tbl.put_item(Item=_to_ddb({
        **spread,
        "pk": f"{_pk_prefix()}SPREAD#OPEN",
        "sk": spread["id"],
    }))

def load_open_spreads() -> list[dict]:
    resp = trade_tbl.query(
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": f"{_pk_prefix()}SPREAD#OPEN"},
    )
    return resp.get("Items", [])

def archive_spread(spread: dict, outcome: dict) -> None:
    """Move a spread from OPEN to CLOSED#YYYYMMDD with its closing outcome."""
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    # Build merged payload, then override pk/sk LAST so the new CLOSED partition
    # key wins over the loaded spread's original pk="...SPREAD#OPEN" and sk=<id>.
    # (In Python dict literals, later keys win; **merged must come before pk/sk.)
    merged = {**spread, **outcome, "closed_at": datetime.now(timezone.utc).isoformat()}
    prefix = _pk_prefix()
    trade_tbl.put_item(Item=_to_ddb({
        **merged,
        "pk": f"{prefix}SPREAD#CLOSED#{today}",
        "sk": spread["id"],
    }))
    trade_tbl.delete_item(Key={"pk": f"{prefix}SPREAD#OPEN", "sk": spread["id"]})

def save_note(body: str, vix: float | None, summaries: list[dict]) -> None:
    now = datetime.now(timezone.utc)
    trade_tbl.put_item(Item=_to_ddb({
        "pk":        f"{_pk_prefix()}NOTE#{now.strftime('%Y%m%d')}",
        "sk":        now.isoformat(),
        "body":      body,
        "vix":       str(vix) if vix is not None else "",
        "summaries": json.dumps(summaries, default=str),
        "timestamp": now.isoformat(),
    }))

def log_event(kind: str, symbol: str, detail: dict, reason: str = "") -> None:
    """Generic per-event row in the TRADE#YYYYMMDD partition.
    `detail` is serialized to JSON so nested floats don't need Decimal conversion."""
    now = datetime.now(timezone.utc)
    trade_tbl.put_item(Item=_to_ddb({
        "pk":        f"{_pk_prefix()}TRADE#{now.strftime('%Y%m%d')}",
        "sk":        f"{now.isoformat()}#{symbol}#{kind}",
        "kind":      kind,           # "open" | "close" | "note" | "error"
        "symbol":    symbol,
        "detail":    json.dumps(detail, default=str),
        "reason":    reason,
        "timestamp": now.isoformat(),
        "date":      now.strftime("%Y-%m-%d"),
    }))

def _naked_no_retry_key() -> tuple[str, str]:
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    return ("STRAT3#NO_RETRY", today)

def add_naked_no_retry(ticker: str) -> None:
    """Mark a ticker as exhausted for today's entry attempts.
    Stored in its own DDB row so rescans (which overwrite the scan record)
    don't accidentally clear this block."""
    pk, sk = _naked_no_retry_key()
    try:
        trade_tbl.update_item(
            Key={"pk": pk, "sk": sk},
            UpdateExpression="ADD tickers :t SET #ttl = :ttl",
            ExpressionAttributeNames={"#ttl": "ttl"},
            ExpressionAttributeValues={
                ":t": {ticker.upper()},
                ":ttl": int((datetime.now(timezone.utc).replace(hour=23, minute=59) + timedelta(days=1)).timestamp()),
            },
        )
        print(f"  [no-retry] {ticker}: blocked for rest of today")
    except Exception as e:
        print(f"  [no-retry-warn] {type(e).__name__}: {e}")

def get_naked_no_retry() -> set:
    """Return tickers blocked from entry today."""
    pk, sk = _naked_no_retry_key()
    try:
        resp = trade_tbl.get_item(Key={"pk": pk, "sk": sk})
        return set(resp.get("Item", {}).get("tickers", set()))
    except Exception:
        return set()

# ---- Alerts ----------------------------------------------------------------

def send_alert(subject: str, body: str) -> None:
    if not ALERT_TOPIC_ARN:
        return
    try:
        sns.publish(
            TopicArn=ALERT_TOPIC_ARN,
            Subject=subject[:100],
            Message=body,
        )
    except Exception as e:
        print(f"  [alert-fail] {type(e).__name__}: {e}")

def _alert_short_label(spread: dict) -> str:
    """Short subject-line tag — Weeklies / QQQ / Conservative."""
    if spread.get("strategy") == STRATEGY_NAKED:
        return "Weeklies"
    if spread.get("strategy") == STRATEGY_MOMENTUM:
        return "QQQ"
    return "Conservative"

def _alert_full_label(spread: dict) -> str:
    """Body header — full strategy name with structure type."""
    kind = (spread.get("kind") or "").lower()
    if kind == "squeeze":
        return "QQQ (Squeeze-Cross)"
    if kind == "qqq":
        return "QQQ (0-5 DTE)"
    if spread.get("strategy") == STRATEGY_NAKED:
        return "Weeklies (Naked Options)"
    if spread.get("strategy") == STRATEGY_MOMENTUM:
        return "QQQ (Momentum)"
    return "Conservative (Credit Spread)"

def send_position_alert(event_type: str, spread: dict, outcome: dict | None = None) -> None:
    """Unified SNS alert for position events across all three strategies.
    event_type: 'open' | 'close' | 'tp1' | 'tp2'
    Strategy is detected from the spread schema (strategy field + kind field)."""
    label   = _alert_short_label(spread)   # [Conservative] / [Aggressive] / [Aggressive 2.0]
    full    = _alert_full_label(spread)
    under   = spread.get("underlying") or spread.get("ticker") or "?"
    is_squeeze = (spread.get("kind") or "").lower() == "squeeze"
    is_naked   = spread.get("strategy") == STRATEGY_NAKED
    is_credit  = not is_squeeze and not is_naked

    if event_type == "open":
        if is_credit:
            sh = int(spread.get("short_strike", 0)); lo = int(spread.get("long_strike", 0))
            credit = abs(float(spread.get("credit", 0)))
            subject = f"[{label}] OPEN {under} {sh}/{lo}P @ ${credit:.2f}"
            body    = format_open_email(spread)
        elif is_squeeze:
            qty    = int(spread.get("qty_original") or spread.get("qty") or 1)
            strike = int(float(spread.get("strike", 0)))
            opt    = (spread.get("option_type") or "call")[0].upper()
            prem   = float(spread.get("premium_paid", 0))
            total  = float(spread.get("total_cost") or prem * 100 * qty)
            direction = spread.get("direction", "?")
            subject = f"[{label}] OPEN {under} {direction} ${strike}{opt} x{qty} @ ${prem:.2f}"
            body = (
                f"TradeBot opened a {full} position.\n\n"
                f"Underlying  : {under}\n"
                f"Contract    : BUY {qty}x ${strike}{opt} exp {spread.get('expiry')} ({spread.get('dte')} DTE)\n"
                f"Direction   : {direction}\n"
                f"Premium     : ${prem:.2f}/contract  ({qty} contracts = ${total:.0f} total)\n"
                f"\n-- Exit ladder --\n"
                f"Stop loss        : -{int(SQUEEZE_STOP_PCT*100)}% (price ${spread.get('stop_value','?')})\n"
                f"TP1 (close 50%)  : +{int(SQUEEZE_TP1_GAIN_PCT*100)}% (price ${spread.get('tp1_value','?')}, stop moves to BE)\n"
                f"TP2 (close 25%)  : +{int(SQUEEZE_TP2_GAIN_PCT*100)}% (price ${spread.get('tp2_value','?')}, runner trails 9 EMA)\n"
                f"\nScore  : {spread.get('score','?')}/10\n"
                f"Reason : {spread.get('claude_reason','?')}\n"
            )
        else:  # naked
            qty    = int(spread.get("qty") or 1)
            strike = int(float(spread.get("strike", 0)))
            opt    = (spread.get("option_type") or "call")[0].upper()
            prem   = float(spread.get("premium_paid", 0))
            total  = float(spread.get("total_cost") or prem * 100 * qty)
            direction = spread.get("direction", "?")
            subject = f"[{label}] OPEN {under} {direction} ${strike}{opt} x{qty} @ ${prem:.2f}"
            stop_pct = float(spread.get("stop_loss_pct", 0)) * 100
            body = (
                f"TradeBot opened a {full} position.\n\n"
                f"Underlying  : {under}\n"
                f"Contract    : BUY {qty}x ${strike}{opt} exp {spread.get('expiry')} ({spread.get('dte')} DTE)\n"
                f"Direction   : {direction}\n"
                f"Premium     : ${prem:.2f}/contract  ({qty} contracts = ${total:.0f} total)\n"
                f"Catalyst    : {spread.get('catalyst','?')}\n"
                f"\nStop loss   : -{stop_pct:.0f}% (premium ${spread.get('stop_loss_value_per_contract','?')})\n"
                f"Breakeven   : ${spread.get('breakeven','?')}\n"
                f"Score       : {spread.get('score','?')}/10\n"
                f"Rationale   : {spread.get('rationale','?')}\n"
            )
        send_alert(subject, body)
        tg = f"<b>OPEN {under}</b>  {direction}  ${strike}{opt} x{qty}\n"
        tg += f"@ ${prem:.2f}/contract = ${total:.0f} total\n"
        if not is_credit:
            tg += f"Score: {spread.get('score','?')}/10  |  {spread.get('catalyst') or spread.get('claude_reason') or ''}\n"
            if is_naked:
                tg += f"Stop: -{'%.0f' % (float(spread.get('stop_loss_pct',0))*100)}%  |  Breakeven: ${spread.get('breakeven','?')}"
        send_telegram(tg)

    elif event_type == "close":
        out = outcome or {}
        pnl_dollars = float(out.get("pnl_dollars") or out.get("pnl") or 0)
        pnl_pct     = float(out.get("pnl_pct") or 0)
        reason      = out.get("exit_reason") or ""
        sign = "+" if pnl_dollars >= 0 else "-"
        subject = f"[{label}] CLOSE {under} P&L {sign}${abs(pnl_dollars):.0f} ({pnl_pct:+.1f}%)"
        if is_credit:
            body = format_close_email(spread, out)
        else:
            body = (
                f"TradeBot closed a {full} position.\n\n"
                f"Underlying : {under}\n"
                f"Contract   : {spread.get('contract_symbol', spread.get('short_symbol','?'))}\n"
                f"Opened     : {spread.get('opened_at','?')}\n"
                f"Closed     : {datetime.now(timezone.utc).isoformat()}\n"
                f"P&L        : {sign}${abs(pnl_dollars):.0f}  ({pnl_pct:+.1f}%)\n"
                f"Reason     : {reason}\n"
            )
        send_alert(subject, body)
        tg = f"<b>CLOSE {under}</b>  {sign}${abs(pnl_dollars):.0f}  ({pnl_pct:+.1f}%)\n"
        tg += f"{reason}"
        send_telegram(tg)

    elif event_type in ("tp1", "tp2"):
        out = outcome or {}
        qty_closed = int(out.get("qty_closed") or 0)
        prem       = float(out.get("premium") or 0)
        realized   = float(out.get("realized_pnl") or 0)
        sign = "+" if realized >= 0 else "-"
        tier = "TP1 (close 50%)" if event_type == "tp1" else "TP2 (close 25%, trail rest)"
        subject = f"[{label}] {event_type.upper()} {under} closed {qty_closed}c {sign}${abs(realized):.0f}"
        body = (
            f"TradeBot partial close on a {full} position.\n\n"
            f"Tier        : {tier}\n"
            f"Underlying  : {under}\n"
            f"Contract    : {spread.get('contract_symbol','?')}\n"
            f"Closed      : {qty_closed} contracts @ ${prem:.2f}\n"
            f"Realized    : {sign}${abs(realized):.0f}\n"
            f"Remaining   : {spread.get('qty', '?')} of original {spread.get('qty_original', '?')}\n"
            f"\nPosition continues. "
            + ("Stop now at breakeven on remainder." if event_type == "tp1"
               else "Runner now trailing 9-EMA close (1-min bars).") + "\n"
        )
        send_alert(subject, body)
        tg = f"<b>{event_type.upper()} {under}</b>  {sign}${abs(realized):.0f}\n"
        tg += f"Closed {qty_closed}c @ ${prem:.2f}  |  {tier}"
        send_telegram(tg)

def format_open_email(spread: dict) -> str:
    return (
        "TradeBot opened a put credit spread.\n\n"
        f"Underlying : {spread['underlying']}\n"
        f"Structure  : SELL {spread['short_strike']:.0f}P / BUY {spread['long_strike']:.0f}P\n"
        f"Expiry     : {spread['expiry']} ({spread['dte']} DTE)\n"
        f"Credit     : ${spread['credit']:.2f}\n"
        f"Max profit : ${spread['credit'] * 100:.0f}\n"
        f"Max loss   : ${spread['max_loss']:.0f}\n"
        f"Breakeven  : ${spread['short_strike'] - spread['credit']:.2f}\n\n"
        f"Profit-take GTC placed at ${spread['credit'] * PROFIT_TAKE_PCT:.2f} debit.\n"
    )

def format_close_email(spread: dict, outcome: dict) -> str:
    return (
        f"TradeBot closed a put credit spread ({outcome['exit_reason']}).\n\n"
        f"Underlying : {spread['underlying']}\n"
        f"Structure  : {spread['short_strike']:.0f}P / {spread['long_strike']:.0f}P\n"
        f"Opened     : {spread['opened_at'][:10]} at ${spread['credit']:.2f} credit\n"
        f"Closed     : {outcome.get('debit', 0.0):.2f} debit\n"
        f"P&L        : ${outcome['pnl']:+.2f}\n"
        f"Exit reason: {outcome['exit_reason']}\n"
    )

# ---- Claude: per-mode prompting --------------------------------------------

SYSTEM_MORNING = """You are a disciplined options-income assistant focused on put credit spreads on SPY/QQQ/IWM.

Your job in MORNING mode is ONE paragraph (max 120 words) of market context that the midday scan will reuse. Cover:
- Overall trend reading from EMA-8 vs EMA-21 and RSI-14
- Whether VIX suggests an attractive premium environment (elevated) or not (crushed)
- Any ticker currently looking problematic for put credit spreads (e.g., sharp downtrend)

Output plain prose. No JSON, no markdown, no bullet points. Just the paragraph."""

SYSTEM_MIDDAY = """You are a disciplined options-income trader placing PUT CREDIT SPREADS on SPY/QQQ/IWM.

STRATEGY:
- Only short puts at 16-20 delta (absolute value), paired with long puts $5 lower strike
- 30-45 DTE window
- Minimum credit $0.50
- Prefer neutral-to-bullish setups: RSI 40-60 ideal; EMA-8 >= EMA-21 is a plus
- Avoid opening if the ticker is in a sharp downtrend (EMA-8 << EMA-21 AND RSI < 35)
- VIX elevated = attractive; VIX crushed = skip unless the setup is clearly clean

For EACH ticker in the candidate list, output ONE decision.

OUTPUT STRICTLY JSON in this schema and nothing else:
{
  "decisions": [
    {
      "symbol": "SPY",
      "action": "open" | "skip",
      "short_symbol": "<OCC contract symbol for the short leg>" | null,
      "long_symbol":  "<OCC contract symbol for the long leg>"  | null,
      "credit_target": <float>,
      "reason": "<20 words max>"
    }
  ]
}

Only pick short/long pairs from the 'candidates' payload. If no candidate meets the strategy, action = "skip"."""

SYSTEM_EOD = """You are reviewing open put credit spreads. Rules are mechanical; you are a tiebreaker.

Mechanical close rules (applied BEFORE you see the position):
- 50% profit target -> already handled by broker GTC
- 2x credit stop -> close now
- <=7 DTE -> close now

You only receive positions that passed the mechanical check but have a borderline condition (e.g., 1.7x debit, or 8 DTE). For each, decide close or keep with a one-line reason.

OUTPUT STRICTLY JSON:
{"decisions": [{"spread_id": "<id>", "action": "close" | "keep", "reason": "<20 words>"}]}"""

def _anthropic_client() -> "anthropic.Anthropic":
    return anthropic.Anthropic(api_key=get_secret("/trading-bot/anthropic-key"))

def claude_morning_note(vix: float | None, summaries: list[dict]) -> str:
    client = _anthropic_client()
    prompt = (
        f"VIX: {vix if vix is not None else 'n/a'}\n\n"
        "Ticker snapshots:\n" +
        "\n".join(
            f"  {s['symbol']}: last ${s['last']}, EMA8 {s['ema8']}, EMA21 {s['ema21']}, "
            f"RSI14 {s['rsi14']}, 5d range {s['range_5d_pct']}%"
            for s in summaries
        )
    )
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        system=SYSTEM_MORNING,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text.strip()

def claude_midday_picks(candidates: dict, vix: float | None, context_note: str | None, budget_info: dict) -> dict:
    """
    candidates: {symbol: {"summary": {...}, "pairs": [ {short: {...}, long: {...}} ]}}
    Returns decisions dict per SYSTEM_MIDDAY schema.
    """
    client = _anthropic_client()
    prompt_parts = [
        f"VIX: {vix if vix is not None else 'n/a'}",
        f"Morning context: {context_note or 'none'}",
        f"Open spreads: {budget_info['open']}/{MAX_SPREADS}",
        f"At-risk: ${budget_info['at_risk']:.0f} / ${ACCOUNT_RISK_CAP:.0f}",
        f"Remaining slots: {budget_info['slots']}",
        "",
    ]
    for sym, payload in candidates.items():
        s = payload["summary"]
        prompt_parts.append(f"=== {sym} ===")
        prompt_parts.append(
            f"last ${s['last']}, EMA8 {s['ema8']}, EMA21 {s['ema21']}, RSI14 {s['rsi14']}"
        )
        prompt_parts.append("Candidate pairs (short / long):")
        for p in payload["pairs"][:8]:
            sh, lo = p["short"], p["long"]
            prompt_parts.append(
                f"  {sh['symbol']} short  K={sh['strike']:.0f}  delta={sh['delta']:.3f}  bid/ask={sh['bid']:.2f}/{sh['ask']:.2f}  exp={sh['expiry']}"
            )
            prompt_parts.append(
                f"  {lo['symbol']} long   K={lo['strike']:.0f}  delta={lo['delta']:.3f}  bid/ask={lo['bid']:.2f}/{lo['ask']:.2f}"
            )
            prompt_parts.append(f"  Net credit estimate: ${p['credit_est']:.2f}  (width ${SPREAD_WIDTH})")
            prompt_parts.append("")
        prompt_parts.append("")
    prompt = "\n".join(prompt_parts)

    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=500,
        system=SYSTEM_MIDDAY,
        messages=[
            {"role": "user",      "content": prompt},
            {"role": "assistant", "content": "{"},
        ],
    )
    raw = msg.content[0].text.strip()
    text = raw if raw.startswith("{") else "{" + raw
    return _parse_json(text, fallback={"decisions": []})

def claude_eod_review(borderline: list[dict]) -> dict:
    if not borderline:
        return {"decisions": []}
    client = _anthropic_client()
    body = "\n".join(
        f"- id={s['spread_id']}  {s['underlying']}  {s['short_strike']:.0f}/{s['long_strike']:.0f}P  "
        f"DTE={s['dte']}  credit=${s['credit']:.2f}  current_debit=${s['current_debit']:.2f}  "
        f"loss_mult={s['loss_mult']:.2f}x"
        for s in borderline
    )
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=400,
        system=SYSTEM_EOD,
        messages=[
            {"role": "user",      "content": body},
            {"role": "assistant", "content": "{"},
        ],
    )
    raw = msg.content[0].text.strip()
    text = raw if raw.startswith("{") else "{" + raw
    return _parse_json(text, fallback={"decisions": []})

def _parse_json(text: str, fallback: dict) -> dict:
    # direct
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # strip fences
    fenced = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.MULTILINE)
    try:
        return json.loads(fenced)
    except json.JSONDecodeError:
        pass
    # first object block
    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    print(f"  [parse-fail] {text[:400]!r}")
    return fallback

# ---- Candidate builders (midday) ------------------------------------------

def build_candidates_for_ticker(symbol: str) -> dict:
    """Fetch options chain, filter to 30-45 DTE put contracts, pair strikes $5 apart."""
    today = date.today()
    gte = (today + timedelta(days=DTE_MIN)).isoformat()
    lte = (today + timedelta(days=DTE_MAX)).isoformat()
    contracts = get_options_contracts(symbol, gte, lte, contract_type="put")
    if not contracts:
        return {"summary": summarize_ticker(symbol), "pairs": []}

    # Snapshot everything at once - batched inside the helper
    snaps = get_option_snapshots([c["symbol"] for c in contracts])

    # Group by expiry for pairing
    by_expiry: dict[str, list[dict]] = {}
    for c in contracts:
        snap = snaps.get(c["symbol"], {})
        greeks = snap.get("greeks") or {}
        quote  = snap.get("latestQuote") or {}
        delta  = greeks.get("delta")
        bid    = float(quote.get("bp") or 0)
        ask    = float(quote.get("ap") or 0)
        if delta is None or bid <= 0 or ask <= 0:
            continue
        enriched = {
            "symbol": c["symbol"],
            "strike": float(c["strike_price"]),
            "expiry": c["expiration_date"],
            "delta":  float(delta),
            "bid":    bid,
            "ask":    ask,
        }
        by_expiry.setdefault(c["expiration_date"], []).append(enriched)

    pairs: list[dict] = []
    for expiry, items in by_expiry.items():
        items.sort(key=lambda x: x["strike"])
        strike_map = {it["strike"]: it for it in items}
        # candidate shorts: |delta| in [0.16, 0.20]
        shorts = [it for it in items if SHORT_DELTA_LO <= abs(it["delta"]) <= SHORT_DELTA_HI]
        for sh in shorts:
            lo = strike_map.get(sh["strike"] - SPREAD_WIDTH)
            if not lo:
                continue
            # Net credit estimate at mid: short mid - long mid
            credit_est = ((sh["bid"] + sh["ask"]) / 2) - ((lo["bid"] + lo["ask"]) / 2)
            if credit_est < MIN_CREDIT:
                continue
            pairs.append({"short": sh, "long": lo, "credit_est": round(credit_est, 2)})

    # Sort pairs by credit desc for the top-8 shown to Claude
    pairs.sort(key=lambda p: p["credit_est"], reverse=True)
    return {"summary": summarize_ticker(symbol), "pairs": pairs}

# ---- Budget / risk helpers -------------------------------------------------

def compute_budget() -> dict:
    opens = load_open_spreads()
    at_risk = sum(float(s.get("max_loss", 0)) for s in opens)
    slots = min(MAX_SPREADS - len(opens), 99)
    # Track per-ticker counts + already-open (strike, expiry) pairs so we can
    # block both over-concentration and structural duplicates at entry time.
    per_ticker: dict[str, int] = {}
    open_keys: set[str] = set()
    for s in opens:
        u = s.get("underlying") or s.get("ticker") or ""
        per_ticker[u] = per_ticker.get(u, 0) + 1
        try:
            sh = int(float(s.get("short_strike", 0)))
            lo = int(float(s.get("long_strike", 0)))
            exp = s.get("expiry", "")
            open_keys.add(f"{u}-{sh}-{lo}-{exp}")
        except Exception:
            pass
    return {
        "open":         len(opens),
        "at_risk":      at_risk,
        "slots":        max(slots, 0),
        "cap_left":     max(ACCOUNT_RISK_CAP - at_risk, 0.0),
        "per_ticker":   per_ticker,
        "open_keys":    open_keys,
    }

# ---- Profit-take refresh (morning mode helper) -----------------------------

def _open_orders_by_option_symbol() -> dict[str, list[dict]]:
    """Return currently-open Alpaca orders indexed by each option leg symbol.
    Used to detect whether a spread already has a live profit-take order.
    nested=true asks Alpaca to embed the legs in the parent mleg order.
    """
    try:
        r = http.get(
            f"{ALPACA_BASE}/v2/orders",
            headers=alpaca_headers(),
            params={"status": "open", "nested": "true", "limit": 200},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        orders = r.json()
    except Exception as e:
        print(f"  [orders-fetch-warn] {type(e).__name__}: {e}")
        return {}
    idx: dict[str, list[dict]] = {}
    for o in orders:
        legs = o.get("legs") or []
        # Flat (single-leg) orders carry the symbol on the order itself.
        if not legs and o.get("symbol"):
            idx.setdefault(o["symbol"], []).append(o)
        for leg in legs:
            sym = leg.get("symbol")
            if sym:
                idx.setdefault(sym, []).append(o)
    return idx

def _has_live_profit_take(spread: dict, open_orders_idx: dict[str, list[dict]]) -> bool:
    """A profit-take for this spread exists if there's an open mleg order
    that references BOTH the short and long leg AND is a buy-to-close on the short."""
    candidates = open_orders_idx.get(spread["short_symbol"], [])
    for o in candidates:
        legs = o.get("legs") or []
        syms = {l.get("symbol") for l in legs}
        if spread["short_symbol"] in syms and spread["long_symbol"] in syms:
            # Make sure it's actually the closing direction, not a new opener
            for l in legs:
                if l.get("symbol") == spread["short_symbol"] and l.get("side") == "buy":
                    return True
    return False

def refresh_profit_takes() -> dict:
    """Re-place day-long profit-take orders on every open spread that doesn't
    already have one live. Alpaca paper doesn't honor GTC on multi-leg options,
    so we re-post these each market morning to keep the 50% take always active.
    """
    opens = load_open_spreads()
    if not opens:
        return {"checked": 0, "replaced": 0}

    idx = _open_orders_by_option_symbol()
    replaced = 0
    for s in opens:
        if _has_live_profit_take(s, idx):
            continue
        credit  = float(s.get("credit", 0))
        target  = float(s.get("profit_target", 0)) or round(credit * PROFIT_TAKE_PCT, 2)
        if target <= 0:
            print(f"  [pt-skip] {s['id']}: no valid profit target")
            continue
        close_legs = [
            {"symbol": s["short_symbol"], "side": "buy",  "ratio_qty": "1", "position_intent": "buy_to_close"},
            {"symbol": s["long_symbol"],  "side": "sell", "ratio_qty": "1", "position_intent": "sell_to_close"},
        ]
        try:
            pt = place_mleg_order(close_legs, target, tif="day")
            new_id = pt.get("id", "")
            # Update stored order id so EOD close / dashboard see the current order
            trade_tbl.update_item(
                Key={"pk": "SPREAD#OPEN", "sk": s["sk"]},
                UpdateExpression="SET gtc_order_id = :o",
                ExpressionAttributeValues={":o": new_id},
            )
            replaced += 1
            print(f"  [pt-refresh] {s['id']} -> order {new_id} @ ${target:.2f}")
        except Exception as e:
            print(f"  [pt-refresh-fail] {s['id']}: {type(e).__name__}: {e}")
            log_event("error", s.get("underlying", "BOT"),
                      {"stage": "pt_refresh", "id": s["id"], "error": str(e)})
    return {"checked": len(opens), "replaced": replaced}

# ---- Mode: morning ---------------------------------------------------------

def run_morning() -> dict:
    # Morning is pre-open (08:45 ET). We only build the context note here;
    # profit-take refresh happens in midday mode once the market is live.
    print("[morning] building context note")
    vix = get_vix()
    summaries = [summarize_ticker(sym) for sym in UNIVERSE]
    note = claude_morning_note(vix, summaries)
    save_note(note, vix, summaries)
    print(f"  Note ({len(note)} chars): {note[:120]}...")
    return {"status": "ok", "mode": "morning", "vix": vix, "note": note}

# ---- Mode: midday ----------------------------------------------------------

def _latest_note_body() -> str | None:
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    resp = trade_tbl.query(
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": f"NOTE#{today}"},
        ScanIndexForward=False,
        Limit=1,
    )
    items = resp.get("Items", [])
    return items[0].get("body") if items else None

def _execute_open(decision: dict, pair: dict, underlying: str) -> dict | None:
    sh, lo = pair["short"], pair["long"]
    credit = float(decision.get("credit_target") or pair["credit_est"])
    width  = SPREAD_WIDTH
    max_loss = round((width - credit) * 100, 2)   # per contract

    # Budget guard
    budget = compute_budget()
    if budget["slots"] <= 0 or (budget["at_risk"] + max_loss) > ACCOUNT_RISK_CAP:
        print(f"  [skip] {underlying}: budget full (open={budget['open']}, at_risk=${budget['at_risk']:.0f})")
        return None

    legs = [
        {"symbol": sh["symbol"], "side": "sell", "ratio_qty": "1", "position_intent": "sell_to_open"},
        {"symbol": lo["symbol"], "side": "buy",  "ratio_qty": "1", "position_intent": "buy_to_open"},
    ]
    try:
        order = place_mleg_order(legs, credit, tif="day")
    except Exception as e:
        print(f"  [order-fail] {underlying}: {type(e).__name__}: {e}")
        log_event("error", underlying, {"stage": "open", "error": str(e)}, decision.get("reason", ""))
        return None

    # Poll briefly for fill (paper usually fills instantly)
    filled_price = _wait_for_fill(order["id"], target=credit) or credit

    # Place the profit-take (50% of credit captured) as a day order.
    # Alpaca paper currently 403s tif=gtc on multi-leg options, so we re-place
    # this order at the start of every midday run via refresh_profit_takes().
    profit_price = round(filled_price * PROFIT_TAKE_PCT, 2)
    close_legs = [
        {"symbol": sh["symbol"], "side": "buy",  "ratio_qty": "1", "position_intent": "buy_to_close"},
        {"symbol": lo["symbol"], "side": "sell", "ratio_qty": "1", "position_intent": "sell_to_close"},
    ]
    gtc_id = ""
    try:
        pt = place_mleg_order(close_legs, profit_price, tif="day")
        gtc_id = pt.get("id", "")
    except Exception as e:
        # Non-fatal: position is open, we'll retry the profit-take tomorrow morning.
        print(f"  [profit-take-fail] {underlying}: {type(e).__name__}: {e}")

    spread = {
        "id":            f"{underlying}-{sh['expiry']}-{int(sh['strike'])}-{int(lo['strike'])}-{uuid.uuid4().hex[:6]}",
        "underlying":    underlying,
        "short_symbol":  sh["symbol"],
        "long_symbol":   lo["symbol"],
        "short_strike":  sh["strike"],
        "long_strike":   lo["strike"],
        "expiry":        sh["expiry"],
        "dte":           (date.fromisoformat(sh["expiry"]) - date.today()).days,
        # abs() because Alpaca's mleg fill_avg_price arrives signed
        # (negative on sell-direction credit spreads). Conceptually the
        # credit we received is always a positive dollar amount.
        "credit":        abs(float(filled_price)),
        "max_loss":      max_loss,
        "profit_target": profit_price,
        "gtc_order_id":  gtc_id,
        "open_order_id": order["id"],
        "opened_at":     datetime.now(timezone.utc).isoformat(),
        "status":        "open",
    }
    save_open_spread(spread)
    log_event("open", underlying, spread, decision.get("reason", ""))
    send_position_alert("open", spread)
    print(f"  [open] {underlying} {int(sh['strike'])}/{int(lo['strike'])}P credit ${filled_price:.2f}")
    return spread

def _wait_for_fill(order_id: str, target: float, tries: int = 4, delay_s: float = 1.5) -> float | None:
    for _ in range(tries):
        try:
            r = http.get(f"{ALPACA_BASE}/v2/orders/{order_id}", headers=alpaca_headers(), timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            j = r.json()
            if j.get("status") == "filled":
                fp = j.get("filled_avg_price") or j.get("limit_price") or target
                return float(fp)
        except Exception:
            pass
        time.sleep(delay_s)
    return None

def run_midday() -> dict:
    print("[midday] scanning chains")

    # First, re-post day-long profit-take orders on any open spread that lost
    # its previous order overnight (Alpaca paper doesn't honor GTC on mleg).
    try:
        refresh = refresh_profit_takes()
        print(f"  profit-take refresh: checked={refresh['checked']}, replaced={refresh['replaced']}")
    except Exception as e:
        print(f"  [pt-refresh-warn] {type(e).__name__}: {e}")

    budget = compute_budget()
    print(f"  budget: open={budget['open']}/{MAX_SPREADS}, at_risk=${budget['at_risk']:.0f}, slots={budget['slots']}")
    if budget["slots"] <= 0:
        print("  no slots available, exiting")
        return {"status": "ok", "mode": "midday", "opened": []}

    vix = get_vix()
    note = _latest_note_body()

    candidates: dict[str, dict] = {}
    for sym in UNIVERSE:
        try:
            candidates[sym] = build_candidates_for_ticker(sym)
            pcount = len(candidates[sym]["pairs"])
            print(f"  {sym}: {pcount} candidate pairs")
        except Exception as e:
            print(f"  [chain-fail] {sym}: {type(e).__name__}: {e}")
            candidates[sym] = {"summary": summarize_ticker(sym), "pairs": []}

    # Drop tickers with no viable pairs before prompting
    candidates = {k: v for k, v in candidates.items() if v["pairs"]}
    if not candidates:
        print("  no viable candidates across universe")
        return {"status": "ok", "mode": "midday", "opened": []}

    picks = claude_midday_picks(candidates, vix, note, budget)
    opened = []
    for d in picks.get("decisions", []):
        if d.get("action") != "open":
            continue
        sym = d.get("symbol")
        if sym not in candidates:
            continue
        # Find the pair Claude picked
        pair = next(
            (p for p in candidates[sym]["pairs"]
             if p["short"]["symbol"] == d.get("short_symbol") and p["long"]["symbol"] == d.get("long_symbol")),
            None,
        )
        if not pair:
            print(f"  [skip] {sym}: Claude picked unknown contract pair")
            continue

        # Concentration cap: don't exceed MAX_PER_UNDERLYING per ticker
        if budget["per_ticker"].get(sym, 0) >= MAX_PER_UNDERLYING:
            print(f"  [skip] {sym}: already at {MAX_PER_UNDERLYING}-spread cap for this underlying")
            continue
        # Duplicate detection: same ticker+strikes+expiry already open
        sh_strike = int(pair["short"]["strike"])
        lo_strike = int(pair["long"]["strike"])
        exp = pair["short"]["expiry"]
        key = f"{sym}-{sh_strike}-{lo_strike}-{exp}"
        if key in budget["open_keys"]:
            print(f"  [skip] {sym}: identical spread already open ({sh_strike}P/{lo_strike}P {exp})")
            continue
        spread = _execute_open(d, pair, sym)
        if spread:
            opened.append(spread)
            # Update in-loop budget so a second Claude pick on this ticker
            # in the same midday run is also blocked by the per-ticker cap.
            budget["per_ticker"][sym] = budget["per_ticker"].get(sym, 0) + 1
            budget["open_keys"].add(key)

    print(f"[midday] opened {len(opened)} spread(s)")
    return {"status": "ok", "mode": "midday", "opened": [s["id"] for s in opened]}

# ---- Mode: eod -------------------------------------------------------------

def _current_spread_debit(spread: dict) -> float | None:
    """Estimate current cost to close the spread (net debit, positive = what we pay)."""
    snaps = get_option_snapshots([spread["short_symbol"], spread["long_symbol"]])
    sh = snaps.get(spread["short_symbol"], {})
    lo = snaps.get(spread["long_symbol"], {})
    sh_q, lo_q = sh.get("latestQuote") or {}, lo.get("latestQuote") or {}
    try:
        sh_mid = (float(sh_q.get("bp") or 0) + float(sh_q.get("ap") or 0)) / 2
        lo_mid = (float(lo_q.get("bp") or 0) + float(lo_q.get("ap") or 0)) / 2
        if sh_mid <= 0:
            return None
        return round(sh_mid - lo_mid, 2)  # debit to close = buy short - sell long
    except Exception:
        return None

def _close_spread(spread: dict, reason: str, debit_estimate: float | None) -> dict:
    # Cancel the profit-take GTC first so legs are free to trade
    if spread.get("gtc_order_id"):
        cancel_order(spread["gtc_order_id"])

    close_legs = [
        {"symbol": spread["short_symbol"], "side": "buy",  "ratio_qty": "1", "position_intent": "buy_to_close"},
        {"symbol": spread["long_symbol"],  "side": "sell", "ratio_qty": "1", "position_intent": "sell_to_close"},
    ]
    # If we have a live mid estimate, pay up to a bit over it so we fill.
    # If no quote is available, cap the debit at the spread width (max possible loss)
    # so the order always fills and we don't sit with an uncloseable position.
    if debit_estimate and debit_estimate > 0:
        limit = round(min(debit_estimate * 1.1, SPREAD_WIDTH), 2)
    else:
        limit = SPREAD_WIDTH
    try:
        order = place_mleg_order(close_legs, limit, tif="day")
    except Exception as e:
        print(f"  [close-fail] {spread['id']}: {type(e).__name__}: {e}")
        log_event("error", spread["underlying"], {"stage": "close", "error": str(e), "id": spread["id"]}, reason)
        return {"pnl": 0.0, "debit": limit, "exit_reason": f"{reason} (order error)"}

    # Poll briefly for the actual fill price so P&L reflects reality, not the limit
    filled = _wait_for_fill(order.get("id", ""), target=limit)
    final_debit = round(filled if filled else limit, 2)
    pnl = round((float(spread["credit"]) - final_debit) * 100, 2)
    outcome = {"pnl": pnl, "debit": final_debit, "exit_reason": reason}
    archive_spread(spread, outcome)
    log_event("close", spread["underlying"], {**spread, **outcome}, reason)
    send_position_alert("close", spread, outcome)
    print(f"  [close] {spread['id']} reason={reason} debit=${limit:.2f} pnl=${pnl:+.0f}")
    return outcome

def run_eod() -> dict:
    print("[eod] sweeping open spreads")
    opens = load_open_spreads()
    closed, borderline_for_claude = [], []

    for s in opens:
        # Normalize numeric fields (DynamoDB returns Decimals/strings)
        spread = {
            **s,
            "credit":        float(s["credit"]),
            "short_strike":  float(s["short_strike"]),
            "long_strike":   float(s["long_strike"]),
            "max_loss":      float(s.get("max_loss", 0)),
        }
        exp = date.fromisoformat(spread["expiry"])
        dte = (exp - date.today()).days
        spread["dte"] = dte

        debit = _current_spread_debit(spread)
        if debit is None:
            print(f"  [no-quote] {spread['id']} - skipping")
            continue
        loss_mult = debit / spread["credit"] if spread["credit"] else 0

        # Mechanical rules first
        if dte <= TIME_EXIT_DTE:
            outcome = _close_spread(spread, f"time_exit_dte_{dte}", debit)
            closed.append({**spread, **outcome})
            continue
        if loss_mult >= LOSS_STOP_MULT:
            outcome = _close_spread(spread, f"loss_stop_{loss_mult:.2f}x", debit)
            closed.append({**spread, **outcome})
            continue

        # Borderline window: 1.5x-2x, or 8-10 DTE
        if loss_mult >= 1.5 or dte <= 10:
            borderline_for_claude.append({
                "spread_id":     spread["id"],
                "underlying":    spread["underlying"],
                "short_strike":  spread["short_strike"],
                "long_strike":   spread["long_strike"],
                "dte":           dte,
                "credit":        spread["credit"],
                "current_debit": debit,
                "loss_mult":     loss_mult,
            })

    # Ask Claude about borderline cases
    if borderline_for_claude:
        decisions = claude_eod_review(borderline_for_claude).get("decisions", [])
        id_to_spread = {s["id"]: s for s in opens}
        for d in decisions:
            if d.get("action") != "close":
                continue
            raw = id_to_spread.get(d.get("spread_id"))
            if not raw:
                continue
            spread = {
                **raw,
                "credit":        float(raw["credit"]),
                "short_strike":  float(raw["short_strike"]),
                "long_strike":   float(raw["long_strike"]),
                "max_loss":      float(raw.get("max_loss", 0)),
            }
            debit = _current_spread_debit(spread)
            outcome = _close_spread(spread, f"claude_close:{d.get('reason','')[:40]}", debit)
            closed.append({**spread, **outcome})

    print(f"[eod] closed {len(closed)} spread(s)")
    return {"status": "ok", "mode": "eod", "closed": [c["id"] for c in closed]}

# ---- GET handlers for dashboard --------------------------------------------

def _cors_headers() -> dict:
    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    }

def _recent_trades(limit: int = 30) -> list:
    """Recent activity rows for the active strategy (credit_spread or momentum)."""
    items = []
    today = datetime.now(timezone.utc)
    prefix = _pk_prefix()
    for d in range(7):
        day = today - timedelta(days=d)
        resp = trade_tbl.query(
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": f"{prefix}TRADE#{day.strftime('%Y%m%d')}"},
            ScanIndexForward=False,
            Limit=limit,
        )
        items.extend(resp.get("Items", []))
        if len(items) >= limit:
            break
    items.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return items[:limit]

def _open_spreads_with_marks() -> list:
    """Live mid + P&L for each open position in the active strategy.
    Handles three schemas: credit_spread (mleg), momentum (mleg debit),
    naked (single-leg call/put)."""
    opens = load_open_spreads()
    if not opens:
        return []
    # Collect every option symbol we need a quote for, regardless of schema
    symbols: list[str] = []
    for s in opens:
        if s.get("strategy") == STRATEGY_NAKED or s.get("contract_symbol"):
            symbols.append(s["contract_symbol"])
        else:
            symbols.append(s["short_symbol"])
            symbols.append(s["long_symbol"])
    snaps = get_option_snapshots(symbols)
    out = []
    for s in opens:
        # Naked single-leg path
        if s.get("strategy") == STRATEGY_NAKED or s.get("contract_symbol"):
            q = (snaps.get(s["contract_symbol"], {}) or {}).get("latestQuote") or {}
            try:
                bid = float(q.get("bp") or 0); ask = float(q.get("ap") or 0)
                mid = round((bid + ask) / 2, 2) if (bid > 0 and ask > 0) else None
            except Exception:
                mid = None
            premium_paid = float(s.get("premium_paid", 0) or 0)
            qty = int(s.get("qty", 1) or 1)
            pnl_pct = round((mid - premium_paid) / premium_paid * 100, 1) if (mid is not None and premium_paid > 0) else None
            try:
                dte = (date.fromisoformat(s["expiry"]) - date.today()).days
            except Exception:
                dte = None
            out.append({
                **s,
                "current_value": mid,        # current mid premium per contract
                "pnl_pct":       pnl_pct,
                "dte":           dte,
            })
            continue

        sh_q = (snaps.get(s["short_symbol"], {}) or {}).get("latestQuote") or {}
        lo_q = (snaps.get(s["long_symbol"],  {}) or {}).get("latestQuote") or {}
        try:
            sh_mid = (float(sh_q.get("bp") or 0) + float(sh_q.get("ap") or 0)) / 2
            lo_mid = (float(lo_q.get("bp") or 0) + float(lo_q.get("ap") or 0)) / 2
        except Exception:
            sh_mid, lo_mid = 0, 0

        is_momentum = (s.get("strategy") == STRATEGY_MOMENTUM) or ("debit" in s and "credit" not in s)

        if is_momentum:
            # For debit spreads, current value = long_mid - short_mid (positive = profitable)
            current_value = round(lo_mid - sh_mid, 2) if lo_mid > 0 else None
            # abs() guards against rows that pre-date the credit-sign fix
            debit_basis = abs(float(s.get("debit", 0) or 0))
            pnl_pct = round((current_value - debit_basis) / debit_basis * 100, 1) if (current_value is not None and debit_basis > 0) else None
            dte = (date.fromisoformat(s["expiry"]) - date.today()).days
            out.append({
                **s,
                "debit":         debit_basis,    # normalize to positive for the dashboard
                "current_value": current_value,
                "pnl_pct":       pnl_pct,
                "dte":           dte,
            })
        else:
            # Credit spread: current cost-to-close = short_mid - long_mid
            current_debit = round(sh_mid - lo_mid, 2) if sh_mid > 0 else None
            # abs() guards against rows that pre-date the credit-sign fix
            credit_basis = abs(float(s.get("credit", 0) or 0))
            pnl_pct = round((credit_basis - current_debit) / credit_basis * 100, 1) if (current_debit is not None and credit_basis > 0) else None
            dte = (date.fromisoformat(s["expiry"]) - date.today()).days
            out.append({
                **s,
                "credit":        credit_basis,   # normalize to positive for the dashboard
                "current_debit": current_debit,
                "pnl_pct":       pnl_pct,
                "dte":           dte,
            })
    return out

def _days_between(start_iso, end_iso):
    """Whole-day difference between two ISO timestamps. Returns None on bad input."""
    if not start_iso or not end_iso:
        return None
    try:
        s = datetime.fromisoformat(str(start_iso).replace("Z", "+00:00"))
        e = datetime.fromisoformat(str(end_iso).replace("Z", "+00:00"))
        return max((e - s).days, 0)
    except Exception:
        return None

def _closed_spreads(days_back: int = 90) -> list:
    """Pull closed spreads from the last N days for the active strategy.
    DDB partition keys are date-stamped so we walk day by day."""
    items = []
    today = datetime.now(timezone.utc)
    prefix = _pk_prefix()
    for d in range(days_back):
        day = today - timedelta(days=d)
        resp = trade_tbl.query(
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": f"{prefix}SPREAD#CLOSED#{day.strftime('%Y%m%d')}"},
        )
        items.extend(resp.get("Items", []))
    return items

def _compute_analytics(days_back: int = 90) -> dict:
    """Aggregate closed trades into win-rate / expectancy / per-cohort stats.
    Returns empty-shaped dict if no trades closed yet."""
    closed = _closed_spreads(days_back=days_back)
    empty = {
        "summary":      {"total_trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
                         "avg_win": 0.0, "avg_loss": 0.0, "expectancy": 0.0, "total_pnl": 0.0,
                         "days_back": days_back},
        "by_ticker":    [],
        "by_setup":     [],
        "by_score":     [],
        "recent_trades": [],
    }
    if not closed:
        return empty

    trades = []
    for t in closed:
        pnl = float(t.get("pnl_dollars") or t.get("pnl") or 0)
        ticker = t.get("underlying") or t.get("ticker") or "?"
        entry_price = float(t.get("entry_price") or t.get("premium_paid") or t.get("premium") or 0)
        exit_price  = float(t.get("exit_premium") or t.get("exit_price") or 0)
        pnl_pct     = round((pnl / (entry_price * int(t.get("qty") or t.get("qty_original") or 1) * 100)) * 100, 1) if entry_price > 0 else 0
        trades.append({
            "ticker":          ticker,
            "pnl":             pnl,
            "pnl_pct":         pnl_pct,
            "is_win":          pnl > 0,
            "score":           int(t.get("score") or 0),
            "setup_type":      t.get("setup_type") or "n/a",
            "direction":       t.get("direction") or "n/a",
            "days_held":       _days_between(t.get("opened_at"), t.get("closed_at")),
            "exit_reason":     (t.get("exit_reason") or t.get("reason") or "")[:120],
            "closed_at":       str(t.get("closed_at") or ""),
            "opened_at":       str(t.get("opened_at") or ""),
            "contract_symbol": str(t.get("contract_symbol") or ""),
            "entry_price":     entry_price,
            "exit_price":      exit_price,
            "qty":             int(t.get("qty") or t.get("qty_original") or 1),
        })

    wins   = [tr for tr in trades if tr["is_win"]]
    losses = [tr for tr in trades if not tr["is_win"]]
    win_rate = (len(wins) / len(trades) * 100) if trades else 0.0
    avg_win  = (sum(tr["pnl"] for tr in wins)   / len(wins))   if wins   else 0.0
    avg_loss = (sum(tr["pnl"] for tr in losses) / len(losses)) if losses else 0.0
    expectancy = (win_rate / 100.0 * avg_win) + ((1 - win_rate / 100.0) * avg_loss)
    total_pnl = sum(tr["pnl"] for tr in trades)

    def _bucket_stats(rows: list, key_fn) -> list:
        buckets: dict[str, dict] = {}
        for tr in rows:
            k = key_fn(tr)
            if k is None:
                continue
            b = buckets.setdefault(k, {"trades": 0, "wins": 0, "pnl": 0.0})
            b["trades"] += 1
            if tr["is_win"]:
                b["wins"] += 1
            b["pnl"] += tr["pnl"]
        out = []
        for k, v in buckets.items():
            out.append({
                "key":      k,
                "trades":   v["trades"],
                "wins":     v["wins"],
                "win_rate": round(v["wins"] / v["trades"] * 100, 1) if v["trades"] else 0.0,
                "pnl":      round(v["pnl"], 2),
            })
        out.sort(key=lambda x: x["pnl"], reverse=True)
        return out

    def _score_bucket(tr):
        s = tr["score"]
        if s >= 8: return "high (8-10)"
        if s >= 6: return "mid (6-7)"
        if s > 0:  return "low (<6)"
        return None  # no score = credit_spread or pre-v2 momentum

    by_ticker = _bucket_stats(trades, lambda tr: tr["ticker"])
    by_setup  = _bucket_stats(trades, lambda tr: tr["setup_type"] if tr["setup_type"] != "n/a" else None)
    by_score  = _bucket_stats(trades, _score_bucket)

    # Recent 50 trades, newest first
    trades.sort(key=lambda x: x["closed_at"], reverse=True)
    recent = trades[:50]

    return {
        "summary": {
            "total_trades": len(trades),
            "wins":         len(wins),
            "losses":       len(losses),
            "win_rate":     round(win_rate, 1),
            "avg_win":      round(avg_win, 2),
            "avg_loss":     round(avg_loss, 2),
            "expectancy":   round(expectancy, 2),
            "total_pnl":    round(total_pnl, 2),
            "days_back":    days_back,
        },
        "by_ticker":     by_ticker,
        "by_setup":      by_setup,
        "by_score":      by_score,
        "recent_trades": recent,
    }

def _recent_reflections(limit: int = 10) -> list:
    """Return the last N Weeklies reflection records for the dashboard evolution panel."""
    items = []
    today = datetime.now(timezone.utc)
    for d in range(120):   # scan up to 120 days back
        day = today - timedelta(days=d)
        pk  = f"STRAT3#REFLECT#{day.strftime('%Y%m%d')}"
        try:
            resp = trade_tbl.query(
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": pk},
                ScanIndexForward=False,
            )
            items.extend(resp.get("Items", []))
        except Exception:
            pass
        if len(items) >= limit:
            break
    items.sort(key=lambda x: x.get("sk", ""), reverse=True)
    # Return just the fields the dashboard needs
    out = []
    for r in items[:limit]:
        out.append({
            "date":         r.get("sk", "")[:10],
            "version_from": r.get("version_from", ""),
            "parameter":    r.get("parameter", ""),
            "old_value":    r.get("old_value", ""),
            "new_value":    r.get("new_value", ""),
            "hypothesis":   r.get("hypothesis", ""),
            "confidence":   r.get("confidence", ""),
            "trade_count":  r.get("trade_count", 0),
            "avg_score":    r.get("avg_score", 0),
        })
    return out

def _recent_notes(limit: int = 5) -> list:
    """Recent morning notes for the active strategy."""
    items = []
    today = datetime.now(timezone.utc)
    prefix = _pk_prefix()
    for d in range(7):
        day = today - timedelta(days=d)
        resp = trade_tbl.query(
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": f"{prefix}NOTE#{day.strftime('%Y%m%d')}"},
            ScanIndexForward=False,
            Limit=limit,
        )
        items.extend(resp.get("Items", []))
        if len(items) >= limit:
            break
    items.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return items[:limit]

# ---- Strategy 2: momentum debit spreads (Aggressive) -----------------------
# v2.0 universe = Layer 1 permanent ETFs + Layer 2 dynamic list (Phase C will
# refresh Layer 2 from a Sunday Finviz screen; until then we ship a sensible default).
MOMENTUM_UNIVERSE_LAYER1         = ["QQQ", "SPY", "IWM", "SMH"]
MOMENTUM_UNIVERSE_LAYER2_DEFAULT = ["NVDA", "AAPL", "TSLA", "AMD", "META"]

# Static sector classification - used by the max-2-per-sector rule.
# Add tickers as the dynamic list expands.
MOMENTUM_SECTOR_MAP: dict[str, str] = {
    # Broad ETFs
    "QQQ": "etf_broad", "SPY": "etf_broad", "IWM": "etf_broad",
    # Sector ETFs
    "SMH": "semi", "XLE": "energy", "XLF": "financials", "XLV": "healthcare",
    # Semis
    "NVDA": "semi", "AMD": "semi", "MRVL": "semi", "INTC": "semi", "AVGO": "semi",
    "MU":   "semi", "QCOM": "semi", "TSM": "semi",
    # Mega tech
    "AAPL": "mega_tech", "MSFT": "mega_tech", "GOOGL": "mega_tech",
    "META": "mega_tech", "AMZN": "mega_tech", "NFLX": "mega_tech",
    # EV / auto
    "TSLA": "ev_auto", "RIVN": "ev_auto", "F": "ev_auto", "GM": "ev_auto",
    # Software / data / AI
    "PLTR": "software", "CRM": "software", "ORCL": "software", "NOW": "software",
    "SNOW": "software", "DDOG": "software", "MDB": "software",
    # Financials
    "JPM": "financials", "BAC": "financials", "GS": "financials", "MS": "financials",
    # Default catch-all is computed in get_sector()
}

def get_sector(ticker: str) -> str:
    """Return the sector for a ticker. Unknown tickers default to 'other' so
    they each get their own bucket and don't accidentally cluster."""
    return MOMENTUM_SECTOR_MAP.get(ticker, f"other_{ticker}")

def get_momentum_universe() -> list[str]:
    """Layer 1 permanent + Layer 2 dynamic (from SSM in Phase C; default until then)."""
    layer2 = MOMENTUM_UNIVERSE_LAYER2_DEFAULT
    try:
        # Phase C will populate this SSM param from a Sunday Finviz screen.
        raw = get_secret("/trading-bot/dynamic-tickers")
        loaded = json.loads(raw) if raw else None
        if isinstance(loaded, list) and loaded:
            layer2 = [str(t).upper() for t in loaded if isinstance(t, str)]
    except Exception:
        # Param doesn't exist yet (Phase C not deployed) - use default.
        pass
    # De-dupe in case Layer 2 happens to include a Layer 1 ETF.
    seen, out = set(), []
    for t in MOMENTUM_UNIVERSE_LAYER1 + layer2:
        if t not in seen:
            seen.add(t); out.append(t)
    return out

# Backward-compat alias so existing references resolve while we migrate.
MOMENTUM_UNIVERSE = MOMENTUM_UNIVERSE_LAYER1 + MOMENTUM_UNIVERSE_LAYER2_DEFAULT

SYSTEM_MOMENTUM_PREMARKET = """You are an aggressive momentum options trading assistant for a paper account. You score directional debit-spread setups on a 0-10 scale (5 signals, 2 points each). Only flag score >= 6 as candidates. Be selective - 1-2 strong setups per week beats 5 weak ones.

Scoring rubric (each signal: 0 / 1 / 2 points):
1. EMA alignment
   - 0 pts: 8-EMA below 21-EMA AND price below both
   - 1 pt:  mixed (one above, one below, or price between EMAs)
   - 2 pts: 8-EMA above 21-EMA AND price above both (bullish), or strict opposite (bearish)
2. MACD histogram
   - 0 pts: opposite direction or flat near zero
   - 1 pt:  turning toward your direction but hasn't crossed
   - 2 pts: clearly crossing zero in your direction
3. RSI position + slope
   - 0 pts: extreme zones (>70 or <30) or dead (35-45 for bull, 55-65 for bear)
   - 1 pt:  neutral zone 45-55
   - 2 pts: 55-65 rising (bull) OR 35-45 falling (bear)
4. Volume ratio (vs 20-day avg)
   - 0 pts: below average (<1.0x)
   - 1 pt:  slightly above (1.0-1.3x)
   - 2 pts: surge (>1.3x)
5. Price structure (use BBwidth + recent action)
   - 0 pts: below support / no clear level
   - 1 pt:  near a level but not broken
   - 2 pts: broke above resistance on volume (bull) or below support (bear); or breakout coil (tight BBwidth + directional candle)

Tiers:
- 8-10 = strong setup, suggested_size "$250"
- 6-7  = good setup, suggested_size "$150"
- 4-5  = weak, watch_only
- 0-3  = skip

Hard exclusions (the bot also enforces these in code - just be consistent):
- Earnings rule: if "earnings (Nd)" appears AND N <= 21 (individual stocks only), set proceed_to_options=false. ETFs always pass.
- Skip if RSI is in extreme zones (>70 or <30) - chase risk.

Return JSON only, no preamble. Schema:
{
  "candidates": [
    {
      "ticker": "MRVL",
      "direction": "bullish"|"bearish",
      "score": 8,
      "score_breakdown": {"ema": 2, "macd": 2, "rsi": 2, "volume": 1, "structure": 1},
      "signals": ["8EMA above 21EMA + price above both", "MACD histogram +0.42 crossing", "RSI 61 rising", "vol 1.6x avg"],
      "setup_type": "breakout_coil"|"trend_continuation"|"reversal",
      "proceed_to_options": true,
      "suggested_size": "$250"
    }
  ],
  "watch_only": [{"ticker": "TSLA", "score": 5, "reason": "MACD flat + RSI neutral"}],
  "skip_tickers": {"AAPL": "earnings in 4 days"}
}"""

def momentum_summarize(symbol: str, earnings_map: dict) -> dict:
    """Pull all signals for one ticker. Used by the momentum premarket scan."""
    bars = get_daily_bars(symbol, limit=60)
    closes = [float(b["c"]) for b in bars]
    last = closes[-1] if closes else None
    e8  = ema(closes, 8)
    e21 = ema(closes, 21)
    _macd_line, _sig, macd_hist = macd(closes)
    rsi = rsi_14(closes)
    vol_r = volume_ratio(bars)
    bbw = bb_width_pct(closes)
    atr = atr_14(bars)
    earn = earnings_map.get(symbol)
    days_to_earn = None
    if earn:
        try:
            days_to_earn = (date.fromisoformat(earn) - date.today()).days
        except Exception:
            pass
    return {
        "symbol":       symbol,
        "price":        round(last, 2) if last is not None else None,
        "ema8":         round(e8, 2)  if e8  is not None else None,
        "ema21":        round(e21, 2) if e21 is not None else None,
        "macd_hist":    macd_hist,
        "rsi14":        round(rsi, 1) if rsi is not None else None,
        "vol_ratio":    vol_r,
        "bb_width_pct": bbw,
        "atr14":        atr,
        "earnings":     earn or "n/a",
        "days_to_earn": days_to_earn,
    }

def claude_momentum_scan(summaries: list[dict], regime: dict | None = None) -> dict:
    """Send the universe snapshot to Claude for 0-10 confluence scoring.
    Regime context lets Claude bias appropriately under yellow/red conditions."""
    client = _anthropic_client()
    today = date.today().isoformat()
    lines = [f"Momentum scan for {today}."]
    if regime:
        lines.append(
            f"Regime: {regime.get('regime', '?')} | bias: {regime.get('bias', '?')} | "
            f"new_trades_allowed: {regime.get('new_trades_allowed', '?')}"
        )
        if regime.get("regime") == "yellow":
            lines.append("YELLOW regime constraint: only flag bullish setups, only score 8+ as proceed_to_options.")
        elif regime.get("regime") == "red":
            lines.append("RED regime: still score for visibility, but set proceed_to_options=false on every candidate.")
    lines += ["", "Ticker snapshots (price/indicators):"]
    for s in summaries:
        earn_str = s["earnings"]
        if s["days_to_earn"] is not None:
            earn_str += f" ({s['days_to_earn']}d)"
        lines.append(
            f"  {s['symbol']:5} price ${s['price']}, EMA8 {s['ema8']}, EMA21 {s['ema21']}, "
            f"MACD_hist {s['macd_hist']}, RSI {s['rsi14']}, vol {s['vol_ratio']}x, "
            f"BBwidth {s['bb_width_pct']}%, ATR ${s['atr14']}, earnings {earn_str}"
        )
    prompt = "\n".join(lines)
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1500,   # was 600 — v2.0 needs room for score_breakdown + watch_only + full skip reasons across 14+ tickers
        system=SYSTEM_MOMENTUM_PREMARKET,
        messages=[
            {"role": "user", "content": prompt},
            {"role": "assistant", "content": "{"},
        ],
    )
    raw = msg.content[0].text.strip()
    print(f"  [premarket-raw] stop={getattr(msg, 'stop_reason', '?')} chars={len(raw)} text={raw[:600]}")
    text = raw if raw.startswith("{") else "{" + raw
    parsed = _parse_json(text, fallback={"candidates": [], "watch_only": [], "skip_tickers": {}})
    # Surface stop reason so we can spot truncations without a CloudWatch dive
    parsed.setdefault("_meta", {})["stop_reason"] = getattr(msg, "stop_reason", "?")
    parsed["_meta"]["raw_chars"] = len(raw)
    return parsed

def save_momentum_note(scan_result: dict, summaries: list[dict]) -> None:
    """Persist the day's premarket scan under STRAT2#NOTE# so the dashboard can show it.
    Synthesizes a human-readable 'body' field so the same notes panel that renders
    Conservative morning notes can render this one too."""
    now = datetime.now(timezone.utc)
    candidates = scan_result.get("candidates", []) or []
    skips = scan_result.get("skip_tickers", {}) or {}

    # Build a readable summary block. Show full signals + full skip reasons —
    # truncation hides the WHY behind a skip, which is the whole point.
    if candidates:
        cand_lines = []
        for c in candidates:
            sigs = c.get("signals") or []
            sig_text = "; ".join(sigs) if sigs else ""
            cand_lines.append(
                f"{c.get('ticker','?')} {c.get('direction','?')} (score {c.get('score','?')}/5, {c.get('setup_type','?')})"
                + (f" — {sig_text}" if sig_text else "")
            )
        cand_text = " || ".join(cand_lines)
    else:
        cand_text = "no candidates flagged"

    watch = scan_result.get("watch_only", []) or []
    watch_text = ""
    if watch:
        watch_text = f" • Watch-only ({len(watch)}): " + "; ".join(
            f"{w.get('ticker','?')} score {w.get('score','?')} ({w.get('reason','')})" for w in watch
        )

    skip_text = ""
    if skips:
        skip_text = f" • Skipped {len(skips)}: " + "; ".join(f"{k} — {v}" for k, v in skips.items())

    body = f"Momentum premarket: {cand_text}.{watch_text}{skip_text}"

    trade_tbl.put_item(Item=_to_ddb({
        "pk":         f"STRAT2#NOTE#{now.strftime('%Y%m%d')}",
        "sk":         now.isoformat(),
        "body":       body,
        "scan":       json.dumps(scan_result, default=str),
        "summaries":  json.dumps(summaries, default=str),
        "candidates": json.dumps(candidates, default=str),
        "timestamp":  now.isoformat(),
    }))

def run_momentum_premarket() -> dict:
    """9:00 ET. Pull signals for the 8-ticker universe and have Claude score them."""
    print("[momentum-premarket] scoring universe")
    earnings_map = get_earnings_dates()
    summaries = [momentum_summarize(sym, earnings_map) for sym in get_momentum_universe()]
    for s in summaries:
        print(
            f"  {s['symbol']}: price ${s['price']} EMA8 {s['ema8']} EMA21 {s['ema21']} "
            f"MACD_h {s['macd_hist']} RSI {s['rsi14']} vol {s['vol_ratio']}x BBw {s['bb_width_pct']}%"
        )
    regime = get_active_regime()
    print(f"  regime={regime.get('regime')} new_trades={regime.get('new_trades_allowed')}")
    scan = claude_momentum_scan(summaries, regime=regime)
    scan.setdefault("_meta", {})["regime"] = regime.get("regime")
    save_momentum_note(scan, summaries)
    candidates  = scan.get("candidates", [])
    watch_only  = scan.get("watch_only", [])
    skip_tickers = scan.get("skip_tickers", {})
    proceed = [c for c in candidates if c.get("proceed_to_options")]
    print(f"  scan complete: {len(candidates)} candidates, {len(watch_only)} watch_only, {len(skip_tickers)} skipped, {len(proceed)} proceed_to_options")
    return {
        "status":       "ok",
        "strategy":     STRATEGY_MOMENTUM,
        "mode":         "premarket",
        "candidates":   candidates,
        "watch_only":   watch_only,
        "skip_tickers": skip_tickers,
        "_meta":        scan.get("_meta", {}),
    }

def _current_debit_spread_value(spread: dict) -> float | None:
    """Estimate the spread's current value at mid (positive = credit we'd receive on close).
    Same math for bull-call and bear-put: long_mid - short_mid."""
    snaps = get_option_snapshots([spread["long_symbol"], spread["short_symbol"]])
    long_q  = (snaps.get(spread["long_symbol"],  {}) or {}).get("latestQuote") or {}
    short_q = (snaps.get(spread["short_symbol"], {}) or {}).get("latestQuote") or {}
    try:
        long_mid  = (float(long_q.get("bp")  or 0) + float(long_q.get("ap")  or 0)) / 2
        short_mid = (float(short_q.get("bp") or 0) + float(short_q.get("ap") or 0)) / 2
        if long_mid <= 0:
            return None
        return round(long_mid - short_mid, 2)
    except Exception:
        return None

def _current_underlying_price(symbol: str) -> float | None:
    """Mid of the latest underlying quote, used for the setup-invalidation exit."""
    try:
        q = get_latest_quote(symbol)
        bid = float(q.get("bp") or 0)
        ask = float(q.get("ap") or 0)
        mid = (bid + ask) / 2
        return round(mid, 2) if mid > 0 else None
    except Exception:
        return None

def _evaluate_momentum_exit(spread: dict, current_value: float, current_underlying: float | None, today: "date", mode: str) -> tuple:
    """Decide whether to close an open momentum spread. Returns ('close'|'hold', reason).
    Score-tiered: 8-10 entries get -40% stop / +50% time-gain, 6-7 entries get -30% / +40%."""
    debit = float(spread.get("debit", 0) or 0)
    if debit <= 0:
        return ("hold", "missing debit basis")
    pnl_pct = (current_value - debit) / debit  # 0.50 = +50%

    expiry = date.fromisoformat(spread["expiry"])
    dte = (expiry - today).days

    # Score-aware thresholds (legacy spreads with no score default to high tier)
    score      = spread.get("score")
    stop_pct   = momentum_stop_for_score(score)         # 0.40 or 0.30
    time_gain  = momentum_time_gain_for_score(score)    # 0.50 or 0.40

    # 1. +100% gain (full close during paper trading)
    if pnl_pct >= MOMENTUM_GAIN_TAKE_PCT:
        return ("close", f"+{pnl_pct*100:.0f}% gain (target hit)")

    # 2. +time_gain AND DTE <= 10 (50% for 8-10 score, 40% for 6-7)
    if pnl_pct >= time_gain and dte <= 10:
        return ("close", f"+{pnl_pct*100:.0f}% gain at {dte}DTE (score {score}, time-adjusted)")

    # 3. Score-tiered loss stop (-40% for 8-10, -30% for 6-7)
    if pnl_pct <= -stop_pct:
        return ("close", f"{pnl_pct*100:.0f}% stop loss (score {score} tier)")

    # 4. Time stop: DTE <= 5 AND P&L < +50%
    if dte <= MOMENTUM_TIME_EXIT_DTE and pnl_pct < time_gain:
        return ("close", f"time stop {dte}DTE @ {pnl_pct*100:+.0f}%")

    # 5. Setup invalidation: price went the wrong way past entry
    entry_price = float(spread.get("entry_price", 0) or 0)
    if entry_price > 0 and current_underlying is not None:
        direction = (spread.get("direction") or "").lower()
        if direction == "bullish" and current_underlying < entry_price:
            return ("close", f"price ${current_underlying} below entry ${entry_price} (bull invalidated)")
        if direction == "bearish" and current_underlying > entry_price:
            return ("close", f"price ${current_underlying} above entry ${entry_price} (bear invalidated)")

    # 6. Earnings safety net: earnings landed inside the DTE window
    underlying = spread.get("underlying") or spread.get("ticker") or ""
    earn_str = get_earnings_dates().get(underlying)
    if earn_str:
        try:
            earn_date = date.fromisoformat(earn_str)
            if today <= earn_date <= expiry:
                return ("close", f"earnings {earn_str} inside expiry {spread['expiry']}")
        except Exception:
            pass

    # Preclose tightening: any position with DTE <= 6 and not at +30% closes
    if mode == "preclose" and dte <= 6 and pnl_pct < 0.30:
        return ("close", f"preclose tighten {dte}DTE @ {pnl_pct*100:+.0f}%")

    return ("hold", f"pnl {pnl_pct*100:+.0f}% dte {dte}")

def _close_momentum_spread(spread: dict, reason: str, current_value: float | None) -> dict:
    """Place an mleg close order (SELL long, BUY short) and archive the result."""
    legs = [
        {"symbol": spread["long_symbol"],  "side": "sell", "ratio_qty": "1", "position_intent": "sell_to_close"},
        {"symbol": spread["short_symbol"], "side": "buy",  "ratio_qty": "1", "position_intent": "buy_to_close"},
    ]
    # Aim slightly below current mid so the order fills; floor at $0.01
    if current_value and current_value > 0:
        limit = max(round(current_value * 0.9, 2), 0.01)
    else:
        limit = 0.01

    underlying = spread.get("underlying") or spread.get("ticker") or "BOT"
    try:
        order = place_mleg_order(legs, limit, tif="day")
    except Exception as e:
        print(f"  [close-fail] {spread.get('id')}: {type(e).__name__}: {e}")
        log_event("error", underlying,
                  {"stage": "momentum_close", "error": str(e), "id": spread.get("id")}, reason)
        return {"pnl_dollars": 0, "exit_reason": f"{reason} (order error)"}

    filled = _wait_for_fill(order.get("id", ""), target=limit)
    final_credit = round(float(filled if filled else limit), 2)
    debit = float(spread.get("debit", 0) or 0)
    pnl_dollars = round((final_credit - debit) * 100, 2)
    pnl_pct = ((final_credit - debit) / debit * 100) if debit > 0 else 0.0

    outcome = {
        "exit_credit":  final_credit,
        "pnl_dollars":  pnl_dollars,
        "pnl_pct":      round(pnl_pct, 1),
        "exit_reason":  reason,
    }
    archive_spread(spread, outcome)
    log_event("close", underlying, {**spread, **outcome}, reason)
    print(f"  [close-momentum] {spread.get('id')} {reason} credit=${final_credit:.2f} pnl=${pnl_dollars:+.0f} ({pnl_pct:+.1f}%)")
    return outcome

def _sweep_momentum_exits(mode: str) -> dict:
    """Walk open momentum spreads, evaluate each, close any that trigger."""
    opens = load_open_spreads()  # STRAT2# prefix because _ACTIVE_STRATEGY=momentum
    if not opens:
        return {"checked": 0, "closed": 0, "decisions": []}
    today = date.today()
    closed = 0
    decisions = []
    for s in opens:
        underlying = s.get("underlying") or s.get("ticker")
        if not underlying:
            continue
        value = _current_debit_spread_value(s)
        ul_price = _current_underlying_price(underlying)
        if value is None:
            print(f"  [sweep-skip] {s.get('id')}: no spread quote available")
            continue
        action, reason = _evaluate_momentum_exit(s, value, ul_price, today, mode)
        decisions.append({"id": s.get("id"), "action": action, "reason": reason})
        if action == "close":
            print(f"  [sweep-close] {s.get('id')} - {reason}")
            _close_momentum_spread(s, reason, value)
            closed += 1
    return {"checked": len(opens), "closed": closed, "decisions": decisions}

def run_momentum_postopen() -> dict:
    """9:45 ET. Sweep open positions for any exit trigger right after the open settles."""
    print("[momentum-postopen] sweeping exits")
    try:
        result = _sweep_momentum_exits(mode="postopen")
    except Exception as e:
        print(f"  [postopen-fail] {type(e).__name__}: {e}")
        return {"status": "error", "strategy": STRATEGY_MOMENTUM, "mode": "postopen", "error": str(e)}
    print(f"  checked={result['checked']}, closed={result['closed']}")
    return {"status": "ok", "strategy": STRATEGY_MOMENTUM, "mode": "postopen", **result}

def run_momentum_orb() -> dict:
    """10:30 ET. For score-8+ candidates from this morning's scan, verify
    Opening Range Breakout (price broke first-30-min range on >=1.5x volume)
    and fast-track entries. Skips entirely outside green regime."""
    print("[momentum-orb] checking 8+ candidates for ORB confirmation")

    regime = get_active_regime()
    if regime.get("regime") != "green":
        print(f"  regime={regime.get('regime')} -> skipping ORB (only runs green)")
        return {
            "status":   "ok",
            "strategy": STRATEGY_MOMENTUM,
            "mode":     "orb",
            "reason":   f"regime {regime.get('regime')} - ORB skipped",
            "regime":   regime,
            "confirmed": [],
        }

    scan = _latest_momentum_scan()
    if not scan:
        return {"status": "ok", "strategy": STRATEGY_MOMENTUM, "mode": "orb", "reason": "no scan available", "confirmed": []}

    try:
        candidates = json.loads(scan.get("candidates") or "[]")
    except Exception as e:
        return {"status": "error", "strategy": STRATEGY_MOMENTUM, "mode": "orb", "error": f"scan parse failed: {e}"}

    high_conviction = [c for c in candidates
                       if int(c.get("score") or 0) >= MOMENTUM_SCORE_TIER_HIGH and c.get("proceed_to_options")]
    if not high_conviction:
        print("  no 8+ candidates from morning scan")
        return {"status": "ok", "strategy": STRATEGY_MOMENTUM, "mode": "orb", "reason": "no 8+ candidates", "confirmed": []}

    set_active_strategy(STRATEGY_MOMENTUM)
    confirmed: list[str] = []
    statuses: dict = {}
    for c in high_conviction:
        sym = (c.get("ticker") or "").upper()
        if not sym:
            continue
        orb = compute_orb_status(sym)
        statuses[sym] = orb
        if not orb.get("complete"):
            print(f"  [orb-skip] {sym}: {orb.get('reason')}")
            continue
        direction = (c.get("direction") or "").lower()
        if direction == "bullish" and orb["broke_high"]:
            confirmed.append(sym)
            print(f"  [orb-pass] {sym} bullish: ${orb['current']} > range high ${orb['high']} on {orb['vol_ratio']}x volume")
        elif direction == "bearish" and orb["broke_low"]:
            confirmed.append(sym)
            print(f"  [orb-pass] {sym} bearish: ${orb['current']} < range low ${orb['low']} on {orb['vol_ratio']}x volume")
        else:
            print(f"  [orb-no-break] {sym} {direction}: cur=${orb['current']} hi=${orb['high']} lo=${orb['low']} vol={orb['vol_ratio']}x")

    if not confirmed:
        return {
            "status":     "ok",
            "strategy":   STRATEGY_MOMENTUM,
            "mode":       "orb",
            "reason":     "no ORB confirmations",
            "orb_status": statuses,
            "confirmed":  [],
        }

    # Fast-track: run the midday entry pipeline restricted to confirmed tickers
    print(f"  ORB-confirmed: {confirmed} -> fast-track entry")
    midday_result = run_momentum_midday(_orb_filter=set(confirmed))
    midday_result["mode"]          = "orb"
    midday_result["orb_confirmed"] = confirmed
    midday_result["orb_status"]    = statuses
    return midday_result

def run_momentum_afternoon() -> dict:
    """13:30 ET. Re-score morning's watch_only (4-5) and 6-7 candidates not yet
    entered. Fast-track any that upgraded to proceed_to_options. Skips outside
    green regime or when already at max positions."""
    print("[momentum-afternoon] checking morning candidates for score upgrades")

    regime = get_active_regime()
    if regime.get("regime") != "green":
        print(f"  regime={regime.get('regime')} -> skipping afternoon (only runs green)")
        return {
            "status":   "ok",
            "strategy": STRATEGY_MOMENTUM,
            "mode":     "afternoon",
            "reason":   f"regime {regime.get('regime')} - afternoon skipped",
            "regime":   regime,
            "upgrades": [],
        }

    set_active_strategy(STRATEGY_MOMENTUM)
    opens = load_open_spreads()
    if len(opens) >= MOMENTUM_MAX_POSITIONS:
        print(f"  already at {len(opens)}/{MOMENTUM_MAX_POSITIONS} positions -> no slots")
        return {
            "status":   "ok",
            "strategy": STRATEGY_MOMENTUM,
            "mode":     "afternoon",
            "reason":   "max positions",
            "upgrades": [],
        }

    scan = _latest_momentum_scan()
    if not scan:
        return {"status": "ok", "strategy": STRATEGY_MOMENTUM, "mode": "afternoon", "reason": "no morning scan", "upgrades": []}

    try:
        candidates = json.loads(scan.get("candidates") or "[]")
        watch_only = json.loads(scan.get("watch_only") or "[]") if scan.get("watch_only") else []
    except Exception as e:
        return {"status": "error", "strategy": STRATEGY_MOMENTUM, "mode": "afternoon", "error": f"scan parse failed: {e}"}

    open_tickers = {(s.get("underlying") or s.get("ticker") or "").upper() for s in opens}

    # Tickers worth re-evaluating: watch_only (4-5 morning) + 6-7 not yet entered
    revisit_meta: dict[str, dict] = {}
    for w in (watch_only or []):
        sym = (w.get("ticker") or "").upper()
        if sym and sym not in open_tickers:
            revisit_meta[sym] = {"morning_score": int(w.get("score") or 0), "source": "watch_only"}
    for c in (candidates or []):
        sym = (c.get("ticker") or "").upper()
        if not sym or sym in open_tickers or sym in revisit_meta:
            continue
        morning_score = int(c.get("score") or 0)
        if MOMENTUM_SCORE_MIN_ENTRY <= morning_score < MOMENTUM_SCORE_TIER_HIGH:
            revisit_meta[sym] = {"morning_score": morning_score, "source": "candidate_6_7"}

    if not revisit_meta:
        print("  nothing worth revisiting (no watch_only or 6-7 candidates)")
        return {"status": "ok", "strategy": STRATEGY_MOMENTUM, "mode": "afternoon", "reason": "nothing to revisit", "upgrades": []}

    # Re-pull indicators for these tickers (fresh data 4 hours after morning scan)
    print(f"  re-scoring {len(revisit_meta)} tickers: {sorted(revisit_meta)}")
    earnings_map = get_earnings_dates()
    fresh_summaries = [momentum_summarize(sym, earnings_map) for sym in revisit_meta.keys()]
    afternoon_scan = claude_momentum_scan(fresh_summaries, regime=regime)
    fresh_candidates = afternoon_scan.get("candidates", []) or []

    # Detect upgrades
    upgrades = []
    for fc in fresh_candidates:
        sym = (fc.get("ticker") or "").upper()
        meta = revisit_meta.get(sym)
        if not meta:
            continue
        new_score = int(fc.get("score") or 0)
        morning_score = meta["morning_score"]
        if new_score > morning_score and new_score >= MOMENTUM_SCORE_MIN_ENTRY and fc.get("proceed_to_options"):
            upgrades.append({
                "ticker":         sym,
                "morning_score":  morning_score,
                "current_score":  new_score,
                "source":         meta["source"],
                "direction":      fc.get("direction"),
                "signals":        fc.get("signals", []),
            })
            print(f"  [upgrade] {sym}: {morning_score} -> {new_score} ({fc.get('direction')})")

    if not upgrades:
        print("  no upgrades detected")
        return {
            "status":         "ok",
            "strategy":       STRATEGY_MOMENTUM,
            "mode":           "afternoon",
            "reason":         "no score upgrades",
            "revisited":      sorted(revisit_meta),
            "upgrades":       [],
        }

    # Save the afternoon scan as the latest so the entry pipeline reads it
    save_momentum_note(afternoon_scan, fresh_summaries)

    upgrade_tickers = {u["ticker"] for u in upgrades}
    print(f"  fast-tracking {len(upgrade_tickers)}: {sorted(upgrade_tickers)}")
    result = run_momentum_midday(_orb_filter=upgrade_tickers)
    result["mode"]     = "afternoon"
    result["upgrades"] = upgrades
    return result

SYSTEM_NAKED_EXIT_MANAGER = """You are an active options position manager. You review open naked option positions every 15 minutes and decide whether to hold, partially exit, or fully exit based on real-time momentum, volume, and price action.

Your three outputs:
- HOLD: momentum still intact, thesis still valid, let the trade run
- PARTIAL_EXIT: lock in some profit or cut partial loss — trim the position but stay exposed
- FULL_EXIT: thesis broken, momentum gone, or an excellent exit opportunity — close everything now

Rules you must follow:
- Hard stops are handled by code — you only manage positions that are NOT at stop loss
- Prioritise protecting profit over holding for the absolute peak — a good exit beats a missed exit
- Volume dying while price stalls = warning sign, lean toward PARTIAL_EXIT or FULL_EXIT
- Price below 9-EMA (bull) or above 9-EMA (bear) on 1-min = momentum loss signal
- DTE <= 3: be more aggressive about taking profit — time decay accelerates fast
- If P&L > 60% and any momentum warning signal fires: FULL_EXIT or PARTIAL_EXIT 70%+
- If P&L is negative but above stop, consider whether the thesis still holds

Reply with JSON only: {"action":"HOLD"|"PARTIAL_EXIT"|"FULL_EXIT","exit_pct":0-100,"reason":"<20 words>"}
exit_pct is only used for PARTIAL_EXIT (25-75 typical range). Ignored for HOLD/FULL_EXIT."""

SYSTEM_NAKED_PREMARKET = """You are a catalyst-focused naked-options trading assistant managing single-leg call/put positions for a paper account. The strategy is aggressive: directional plays on high-conviction catalyst setups, 0-28 DTE. Theta destroys you on slow guesses, so a strong catalyst is the dominant factor.

Score each ticker 0-10 across 5 signals with these MAX weights:

1. Catalyst quality (max 4 pts) - the most important signal
   - 0 pts: no catalyst identified
   - 2 pts: minor (analyst note, sector news, conference mention, broad macro for non-ETF)
   - 4 pts: major (earnings within DTE window, FOMC/CPI for SPY/QQQ/IWM, FDA decision, product launch, government contract)

2. Technical trend (max 2 pts)
   - 0 pts: against intended direction (EMA/RSI wrong way)
   - 1 pt: neutral / mixed
   - 2 pts: EMA aligned + RSI supporting direction

3. Options flow (max 2 pts) - if not provided, score 1 (neutral)
   - 0 pts: no unusual activity
   - 1 pt: OI building, mild skew
   - 2 pts: clear sweep or large unusual positioning

4. Volume + momentum (max 1 pt)
   - 0 pts: at or below average
   - 1 pt: above average 2+ days

5. Social + news sentiment (max 1 pt) - if not provided, score 0
   - 0 pts: negative or flat
   - 1 pt: clearly building

Score as a decimal (e.g. 7.4, 8.8) — use the full range, not just round numbers.

Tiers:
- 8.0-10.0 = full size ($1000 max), enter
- 6.0-7.9  = half size ($500 max), enter ONLY with explicit catalyst (no technicals-only) AND DTE >= 7
- 4.0-5.9  = watch_only
- 0-3.9    = skip

Hard rules (the bot will enforce these regardless of your output):
- score < 6 -> not a candidate
- score 6-7 with no identifiable catalyst -> redirect to S2 (set proceed=false, reason "no catalyst, send to S2")
- IV/volatility extreme (BB width > 25% or RSI > 75 / < 25) -> watch_only at most

Earnings play rules (earnings brings volume, movement, and IV expansion — it is one of the BEST catalysts):
- 3-28 days before earnings: normal scoring. Earnings gets full 4 catalyst pts. Play the pre-earnings drift and IV run-up. This is the sweet spot window.
- 1-2 days before earnings: HIGH CONVICTION ONLY. Require score >= 9.0 AND strong technical alignment in the intended direction. Force size="half" regardless of score tier. IV crush is a real risk here if the move is not large enough. Only set proceed_to_options=true if score >= 9.0, otherwise set watch_only.
- Day of / day after earnings: direction is now known, IV has already crushed. Score normally as a fresh directional setup — often a strong entry on the post-earnings trend continuation.
- ETFs (SPY, QQQ, IWM): earnings rules do not apply — score macro catalysts only.

Return JSON only, no preamble. Schema:
{
  "candidates": [
    {
      "ticker": "NVDA",
      "direction": "bullish"|"bearish",
      "score": 8.4,
      "score_breakdown": {"catalyst": 3.8, "technical": 1.8, "flow": 1.4, "volume": 0.9, "social": 0.5},
      "catalyst_description": "earnings 2026-05-20 (13d out)",
      "size": "full"|"half",
      "suggested_dte": 14,
      "rationale": "<25 words>",
      "proceed_to_options": true
    }
  ],
  "watch_only": [{"ticker":"AMD","score":5,"reason":"no catalyst until earnings cycle"}],
  "skip_tickers": {"TSLA":"score 3 - no catalyst, technical neutral"}
}"""

def get_catalysts_for_ticker(ticker: str, all_catalysts: list[dict]) -> list[dict]:
    """Filter the full catalyst list to entries relevant to a given ticker.
    Macro catalysts (CPI/FOMC/NFP/PPI/GDP/RETAIL/ISM) apply to all tickers
    but most strongly to broad-market ETFs. Ticker-specific catalysts (earnings,
    FDA decisions for that ticker) only apply to the ticker itself."""
    macro_types = {"CPI", "PPI", "NFP", "FOMC", "GDP", "RETAIL", "ISM", "MACRO"}
    out = []
    upper_t = ticker.upper()
    for c in all_catalysts:
        c_ticker = (c.get("ticker") or "").upper()
        c_type   = (c.get("type") or "").upper()
        if c_type in macro_types:
            out.append({**c, "applies_as": "macro"})
        elif c_ticker == upper_t:
            out.append({**c, "applies_as": "ticker_specific"})
    # Ticker-specific catalysts are far more important to score; sort them first
    out.sort(key=lambda x: (x.get("applies_as") != "ticker_specific", x.get("days_out", 999)))
    return out

def naked_summarize_ticker(symbol: str, all_catalysts: list[dict]) -> dict:
    """Pull indicators + attach relevant catalysts for a single ticker."""
    bars = get_daily_bars(symbol, limit=60)
    closes = [float(b["c"]) for b in bars]
    last = closes[-1] if closes else None
    e8  = ema(closes, 8)
    e21 = ema(closes, 21)
    _ml, _sig, macd_hist = macd(closes)
    rsi = rsi_14(closes)
    vol_r = volume_ratio(bars)
    bbw = bb_width_pct(closes)
    cats = get_catalysts_for_ticker(symbol, all_catalysts)
    return {
        "symbol":       symbol,
        "price":        round(last, 2) if last is not None else None,
        "ema8":         round(e8, 2)  if e8  is not None else None,
        "ema21":        round(e21, 2) if e21 is not None else None,
        "macd_hist":    macd_hist,
        "rsi14":        round(rsi, 1) if rsi is not None else None,
        "vol_ratio":    vol_r,
        "bb_width_pct": bbw,
        "catalysts":    cats,
    }

def claude_naked_scan(summaries: list[dict], regime: dict | None = None) -> dict:
    """Send the universe + catalyst calendar to Claude for catalyst-weighted scoring."""
    client = _anthropic_client()
    today = date.today().isoformat()
    lines = [f"S3 (naked options) catalyst-weighted scan for {today}."]
    if regime:
        lines.append(
            f"Regime: {regime.get('regime', '?')} | bias: {regime.get('bias', '?')} | "
            f"new_trades_allowed: {regime.get('new_trades_allowed', '?')}"
        )
        if regime.get("regime") == "red":
            lines.append("RED regime: do not flag any candidates as proceed_to_options.")
    lines.append("")
    lines.append("Ticker snapshots (price + indicators + relevant catalysts):")
    for s in summaries:
        # Show ticker-specific catalysts in full; show macro context separately
        ticker_specific = [c for c in (s.get("catalysts") or []) if c.get("applies_as") == "ticker_specific"]
        macro_context   = [c for c in (s.get("catalysts") or []) if c.get("applies_as") == "macro"]
        cat_parts = []
        if ticker_specific:
            cat_parts.append("ticker-specific: " + "; ".join(
                f"{c.get('type','?')} {c.get('description','')[:60]} ({c.get('days_out','?')}d)"
                for c in ticker_specific
            ))
        if macro_context:
            cat_parts.append("macro: " + "; ".join(
                f"{c.get('type','?')}({c.get('days_out','?')}d)"
                for c in macro_context[:6]
            ))
        cat_str = (" | " + " | ".join(cat_parts)) if cat_parts else " | no catalysts"
        lines.append(
            f"  {s['symbol']:5} price ${s['price']}, EMA8 {s['ema8']}, EMA21 {s['ema21']}, "
            f"MACD_hist {s['macd_hist']}, RSI {s['rsi14']}, vol {s['vol_ratio']}x, "
            f"BBwidth {s['bb_width_pct']}%{cat_str}"
        )
    prompt = "\n".join(lines)
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2500,   # 14-ticker universe needs ~2000 tokens for full JSON
        system=SYSTEM_NAKED_PREMARKET,
        messages=[
            {"role": "user", "content": prompt},
            {"role": "assistant", "content": "{"},
        ],
    )
    raw = msg.content[0].text.strip()
    print(f"  [naked-scan-raw] stop={getattr(msg, 'stop_reason', '?')} chars={len(raw)} text={raw[:400]}")
    text = raw if raw.startswith("{") else "{" + raw
    parsed = _parse_json(text, fallback={"candidates": [], "watch_only": [], "skip_tickers": {}})
    parsed.setdefault("_meta", {})["stop_reason"] = getattr(msg, "stop_reason", "?")
    parsed["_meta"]["raw_chars"] = len(raw)
    return parsed

def save_naked_note(scan_result: dict, summaries: list[dict]) -> None:
    """Persist the day's Weeklies scan note as a clean 3-section card."""
    now        = datetime.now(timezone.utc)
    candidates = scan_result.get("candidates", []) or []
    skips      = scan_result.get("skip_tickers", {}) or {}
    watch      = scan_result.get("watch_only", []) or []
    meta       = scan_result.get("_meta", {}) or {}

    regime_str     = meta.get("regime", "unknown")
    bias_str       = meta.get("bias", "")
    vix            = meta.get("vix")
    universe_count = len(summaries)

    # -- REGIME line --
    regime_label = regime_str
    if bias_str and bias_str != "neutral":
        regime_label = f"{regime_str} / {bias_str}"
    vix_part = f"  •  VIX {vix}" if vix else ""
    lines = [f"REGIME: {regime_label}{vix_part}  •  {universe_count} tickers scanned"]

    # -- CANDIDATES section (top Claude picks; execution engine may still skip
    #    individual tickers if earnings gate, no valid contract, or fill fails) --
    if candidates:
        lines.append("")
        lines.append("CANDIDATES:")
        for c in candidates:
            score     = c.get("score", "?")
            direction = (c.get("direction") or "?").lower()
            dte       = c.get("suggested_dte", "?")
            size      = (c.get("size") or "?").lower()
            catalyst  = (c.get("catalyst_description") or "")[:40].strip()
            cat_part  = f"  {catalyst}" if catalyst else ""
            lines.append(
                f"{c.get('ticker','?'):6} {score} — {direction}  {dte}DTE  {size}{cat_part}"
            )

    # -- WATCHING section --
    if watch:
        lines.append("")
        lines.append("WATCHING:")
        for w in watch:
            reason = (w.get("reason") or "")[:50].strip()
            lines.append(f"{w.get('ticker','?'):6} {w.get('score','?')} — {reason}")

    # -- Skip count / fallback --
    if skips:
        lines.append("")
        lines.append(f"{len(skips)} tickers skipped")
    elif not candidates and not watch:
        lines.append("")
        lines.append("No candidates today")

    body = "\n".join(lines)

    trade_tbl.put_item(Item=_to_ddb({
        "pk":         f"STRAT3#NOTE#{now.strftime('%Y%m%d')}",
        "sk":         now.isoformat(),
        "body":       body,
        "scan":       json.dumps(scan_result, default=str),
        "summaries":  json.dumps(summaries, default=str),
        "candidates": json.dumps(candidates, default=str),
        "timestamp":  now.isoformat(),
    }))

def run_naked_premarket() -> dict:
    """S3 catalyst-weighted scoring scan. Pulls catalyst calendar + indicators
    for the naked universe, sends to Claude for 0-10 scoring. Saves note."""
    print("[naked-premarket] catalyst-weighted scan")
    set_active_strategy(STRATEGY_NAKED)
    regime = get_active_regime()
    print(f"  regime={regime.get('regime')} bias={regime.get('bias')}")

    all_catalysts = get_active_catalysts(lookforward_days=28)
    print(f"  active catalysts: {len(all_catalysts)}")

    universe = get_naked_universe()
    summaries = [naked_summarize_ticker(sym, all_catalysts) for sym in universe]

    scan = claude_naked_scan(summaries, regime=regime)
    scan.setdefault("_meta", {})["regime"] = regime.get("regime")
    scan["_meta"]["vix"] = regime.get("inputs", {}).get("vix")
    scan["_meta"]["bias"] = regime.get("bias")
    save_naked_note(scan, summaries)

    cands = scan.get("candidates", []) or []
    watch = scan.get("watch_only", []) or []
    skips = scan.get("skip_tickers", {}) or {}
    proceed = [c for c in cands if c.get("proceed_to_options")]

    # Log gated candidates ONCE here (scan time) so the dashboard shows them
    # without duplicating on every subsequent entry attempt.
    gated = [c for c in cands if not c.get("proceed_to_options")]
    for c in gated:
        sym = (c.get("ticker") or "").upper()
        reason = "earnings within 2 days - gated by scan" if "earn" in (c.get("catalyst_description") or "").lower() else "gated by scan (proceed_to_options=false)"
        log_event("skip", sym,
                  {"stage": "naked_scan_gate", "score": c.get("score"), "reason": reason},
                  c.get("rationale", ""))

    print(f"  scan complete: {len(cands)} candidates ({len(gated)} gated), {len(watch)} watch_only, {len(skips)} skipped, {len(proceed)} proceed")
    return {
        "status":       "ok",
        "strategy":     STRATEGY_NAKED,
        "mode":         "premarket",
        "candidates":   cands,
        "watch_only":   watch,
        "skip_tickers": skips,
        "_meta":        scan.get("_meta", {}),
    }

def compute_naked_budget() -> dict:
    """Per-strategy budget for S3 (uses STRAT3# prefix via _ACTIVE_STRATEGY)."""
    opens = load_open_spreads()
    at_risk = sum(float(s.get("total_cost", 0) or 0) for s in opens)
    return {
        "open":         len(opens),
        "at_risk":      at_risk,
        "slots":        max(NAKED_MAX_POSITIONS - len(opens), 0),
        "cap_left":     max(NAKED_TOTAL_RISK_CAP - at_risk, 0.0),
        "open_tickers": [(s.get("ticker") or s.get("underlying") or "").upper() for s in opens],
    }

def build_naked_contract(symbol: str, direction: str, suggested_dte: int,
                         current_price: float, awr: float | None) -> dict | None:
    """Pull options chain near suggested_dte, filter by delta range + bid-ask
    spread + AWR breakeven check. Returns the best-fit contract or None.

    Two-pass approach:
      Pass 1: use real greeks (delta) when Alpaca returns them.
      Pass 2: when greeks are null (common on paper/indicative feed), use
              OTM% as a delta proxy so we still find valid contracts.
    """
    today = date.today()
    # Widen DTE window to ±10 so weekly expirations don't get missed
    dte_low  = max(NAKED_DTE_MIN, suggested_dte - 10)
    dte_high = min(NAKED_DTE_MAX, suggested_dte + 10)
    gte = (today + timedelta(days=dte_low)).isoformat()
    lte = (today + timedelta(days=dte_high)).isoformat()
    contract_type = "call" if direction == "bullish" else "put"

    contracts = get_options_contracts(symbol, gte, lte, contract_type=contract_type)
    if not contracts:
        print(f"  [contract] {symbol}: no {contract_type}s in {dte_low}-{dte_high} DTE")
        return None

    snaps = get_option_snapshots([c["symbol"] for c in contracts])
    candidates: list[dict] = []
    rejections = {"delta": 0, "bidask": 0, "awr": 0, "missing_quote": 0}
    proxy_candidates: list[dict] = []  # fallback when greeks are null

    for c in contracts:
        snap   = snaps.get(c["symbol"], {})
        greeks = snap.get("greeks") or {}
        quote  = snap.get("latestQuote") or {}
        delta_raw = greeks.get("delta")
        bid = float(quote.get("bp") or 0)
        ask = float(quote.get("ap") or 0)
        ask_size = int(quote.get("as") or 0)   # number of contracts offered at ask
        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else 0

        # Skip contracts with no market makers willing to sell — paper trading
        # won't simulate a fill if nobody is offering. This catches illiquid
        # small-cap option chains (EOSE, BILL, etc.) before we even place an order.
        oi = int(c.get("open_interest") or 0)
        if (ask_size == 0 or oi < 10) and mid > 0:
            rejections["missing_quote"] += 1
            continue

        strike = float(c["strike_price"])
        dte = (date.fromisoformat(c["expiration_date"]) - today).days

        # --- Pass 1: real greeks ---
        if delta_raw is not None and mid > 0:
            abs_delta = abs(float(delta_raw))
            if not (NAKED_DELTA_MIN <= abs_delta <= NAKED_DELTA_MAX):
                rejections["delta"] += 1
            else:
                ba_spread_pct = (ask - bid) / mid
                if ba_spread_pct > NAKED_BIDASK_MAX_PCT:
                    rejections["bidask"] += 1
                else:
                    if direction == "bullish":
                        breakeven = strike + mid
                        move_required = max(breakeven - current_price, 0.01)
                    else:
                        breakeven = strike - mid
                        move_required = max(current_price - breakeven, 0.01)
                    awr_pct = (move_required / awr) if (awr and awr > 0) else None
                    if awr_pct is not None and awr_pct > NAKED_AWR_BREAKEVEN_MAX:
                        rejections["awr"] += 1
                    else:
                        candidates.append({
                            "symbol":        c["symbol"],
                            "strike":        strike,
                            "expiry":        c["expiration_date"],
                            "dte":           dte,
                            "delta":         float(delta_raw),
                            "bid":           bid,
                            "ask":           ask,
                            "mid":           round(mid, 2),
                            "ba_spread_pct": round(ba_spread_pct * 100, 2),
                            "breakeven":     round(breakeven, 2),
                            "move_required": round(move_required, 2),
                            "awr_pct_used":  round(awr_pct * 100, 1) if awr_pct is not None else None,
                            "iv":            float(greeks.get("iv") or 0),
                            "proxy":         False,
                        })
        elif mid > 0:
            # --- Pass 2 fallback: no greeks from Alpaca indicative feed ---
            # Use OTM% as a delta proxy: 0-18% OTM maps to delta ~0.50-0.25
            if direction == "bullish":
                otm_pct = (strike - current_price) / current_price  # positive = OTM for calls
            else:
                otm_pct = (current_price - strike) / current_price  # positive = OTM for puts
            if NAKED_PROXY_OTM_MIN <= otm_pct <= NAKED_PROXY_OTM_MAX:
                ba_spread_pct = (ask - bid) / mid
                if ba_spread_pct <= NAKED_BIDASK_MAX_PCT:
                    # Estimate delta from OTM%: 0% OTM -> 0.50, 18% OTM -> 0.25
                    est_delta = max(0.25, 0.50 - (otm_pct / 0.18) * 0.25)
                    if direction == "bullish":
                        breakeven = strike + mid
                        move_required = max(breakeven - current_price, 0.01)
                    else:
                        breakeven = strike - mid
                        move_required = max(current_price - breakeven, 0.01)
                    awr_pct = (move_required / awr) if (awr and awr > 0) else None
                    if awr_pct is None or awr_pct <= NAKED_AWR_BREAKEVEN_MAX:
                        proxy_candidates.append({
                            "symbol":        c["symbol"],
                            "strike":        strike,
                            "expiry":        c["expiration_date"],
                            "dte":           dte,
                            "delta":         round(est_delta, 2),
                            "bid":           bid,
                            "ask":           ask,
                            "mid":           round(mid, 2),
                            "ba_spread_pct": round(ba_spread_pct * 100, 2),
                            "breakeven":     round(breakeven, 2),
                            "move_required": round(move_required, 2),
                            "awr_pct_used":  round(awr_pct * 100, 1) if awr_pct is not None else None,
                            "iv":            0.0,
                            "proxy":         True,
                        })
            else:
                rejections["missing_quote"] += 1
        else:
            rejections["missing_quote"] += 1

    # Prefer real-greeks candidates; fall back to proxy if none
    final = candidates if candidates else proxy_candidates
    proxy_note = f", proxy_fallback={len(proxy_candidates)}" if not candidates and proxy_candidates else ""
    print(f"  [contract] {symbol}: {len(contracts)} contracts, {len(final)} passed "
          f"(delta={rejections['delta']}, bidask={rejections['bidask']}, "
          f"awr={rejections['awr']}, missing={rejections['missing_quote']}{proxy_note})")
    if not final:
        return None

    # Sort: closest to delta 0.40 (slightly OTM), then closest to suggested_dte
    final.sort(key=lambda x: (abs(abs(x["delta"]) - 0.40), abs(x["dte"] - suggested_dte)))
    return final[0]

def _execute_naked_open(decision: dict, contract: dict, current_price: float, awr: float | None) -> dict | None:
    """Place a single-leg buy-to-open order matching what Claude approved.
    Score-tiered sizing: $1000 (8-10) or $500 (6-7) max premium."""
    underlying = decision["ticker"]
    direction  = decision["direction"]
    score      = float(decision.get("score") or 0)

    score_cap = naked_size_for_score(score)
    if score_cap <= 0:
        print(f"  [skip] {underlying}: score {score} below entry threshold")
        return None

    # Use ask price directly — paper trading won't fill a limit buy below the ask.
    # A prior sanity-cap at mid*1.03 was inadvertently placing orders below the ask;
    # the bid-ask spread filter (20% max) already guards against absurd quotes.
    ask = float(contract.get("ask") or contract["mid"])
    mid = float(contract["mid"])
    entry_price = round(ask, 2)  # limit at the quoted ask; Alpaca paper fills immediately
    cost_per_contract = entry_price * 100
    if cost_per_contract > score_cap:
        print(f"  [skip] {underlying}: cost ${cost_per_contract:.0f}/contract exceeds score-{score} cap ${score_cap:.0f}")
        return None
    qty = max(1, int(score_cap // cost_per_contract))
    total_cost = cost_per_contract * qty

    budget = compute_naked_budget()
    if budget["slots"] <= 0:
        print(f"  [skip] {underlying}: no slots ({budget['open']}/{NAKED_MAX_POSITIONS})")
        return None
    if (budget["at_risk"] + total_cost) > NAKED_TOTAL_RISK_CAP:
        print(f"  [skip] {underlying}: would exceed total risk cap ${NAKED_TOTAL_RISK_CAP}")
        return None

    try:
        order = place_single_leg_order(
            symbol=contract["symbol"],
            side="buy",
            limit_price=entry_price,
            qty=qty,
            tif="day",
            position_intent="buy_to_open",
        )
    except Exception as e:
        print(f"  [naked-order-fail] {underlying}: {type(e).__name__}: {e}")
        log_event("error", underlying, {"stage": "naked_open", "error": str(e)},
                  decision.get("rationale", ""))
        return None

    # First attempt: poll up to 40s at the ask price
    filled = _wait_for_fill(order["id"], target=entry_price, tries=20, delay_s=2.0)
    if filled is None:
        # Alpaca didn't confirm in 40s — cancel and retry once at ask+$0.05
        try:
            cancel_order(order.get("id", ""))
        except Exception as ce:
            print(f"  [naked-cancel-warn] {type(ce).__name__}: {ce}")
        retry_price = round(entry_price + 0.05, 2)
        print(f"  [naked-fill-retry] {underlying}: bumping to ${retry_price:.2f}")
        try:
            order2 = place_single_leg_order(
                symbol=contract["symbol"],
                side="buy",
                limit_price=retry_price,
                qty=qty,
                tif="day",
                position_intent="buy_to_open",
            )
            filled = _wait_for_fill(order2["id"], target=retry_price, tries=15, delay_s=2.0)
            if filled is None:
                cancel_order(order2.get("id", ""))
        except Exception as re2:
            print(f"  [naked-fill-retry-err] {underlying}: {re2}")
            filled = None
    if filled is None:
        print(f"  [naked-fill-timeout] {underlying}: gave up after ask+$0.05 retry")
        log_event("error", underlying,
                  {"stage": "naked_open_no_fill", "order_id": order.get("id"), "contract": contract["symbol"],
                   "tried_prices": [entry_price, round(entry_price + 0.05, 2)]},
                  f"No fill at ask ${entry_price:.2f} or retry ${entry_price+0.05:.2f}")
        send_telegram(
            f"<b>No fill: {underlying}</b>  {contract['symbol']}\n"
            f"Tried ${entry_price:.2f} then ${entry_price+0.05:.2f} — both expired.\n"
            f"Likely illiquid at this strike. Skipping."
        )
        return None
    final_premium = round(abs(float(filled)), 2)
    final_total   = round(final_premium * 100 * qty, 2)

    stop_pct       = naked_stop_for_dte(contract["dte"])
    stop_value_pc  = round(final_premium * (1 - stop_pct), 2)

    spread = {
        "id":              f"N-{underlying}-{contract['expiry']}-{int(contract['strike'])}{('C' if direction=='bullish' else 'P')}-{uuid.uuid4().hex[:6]}",
        "strategy":        STRATEGY_NAKED,
        "kind":            "naked",
        "underlying":      underlying,
        "ticker":          underlying,
        "direction":       direction,
        "option_type":     "call" if direction == "bullish" else "put",
        "contract_symbol": contract["symbol"],
        "strike":          contract["strike"],
        "expiry":          contract["expiry"],
        "dte":             contract["dte"],
        "qty":             qty,
        "premium_paid":    final_premium,
        "total_cost":      final_total,
        "max_loss_dollars": final_total,         # premium paid is the absolute max loss
        "stop_loss_pct":   stop_pct,
        "stop_loss_value_per_contract": stop_value_pc,
        "breakeven":       contract["breakeven"],
        "entry_price":     current_price,
        "awr_at_entry":    awr,
        "delta_at_entry":  contract["delta"],
        "iv_at_entry":     contract["iv"],
        "open_order_id":   order["id"],
        "opened_at":       datetime.now(timezone.utc).isoformat(),
        "status":          "open",
        "score":           score,
        "score_breakdown": decision.get("score_breakdown", {}),
        "catalyst":        decision.get("catalyst_description", ""),
        "rationale":       decision.get("rationale", ""),
    }
    save_open_spread(spread)
    log_event("open", underlying, spread, decision.get("rationale", ""))
    send_position_alert("open", spread)
    print(f"  [open-naked] {underlying} {direction} {contract['symbol']} qty={qty} "
          f"@ ${final_premium:.2f}/c = ${final_total:.0f} stop=-{stop_pct*100:.0f}%")
    return spread

def _get_spy_intraday_move() -> float | None:
    """Return SPY's % change from today's official 9:30 AM open to now.
    Positive = up day, negative = down day.
    Uses the daily bar's open field (always the official 9:30 AM print, DST-safe)
    and the latest quote for current price — no UTC offset arithmetic needed."""
    try:
        bars = get_daily_bars("SPY", limit=2)
        if not bars:
            return None
        today_bar  = bars[-1]
        open_price = float(today_bar.get("o") or today_bar.get("open") or 0)
        if open_price <= 0:
            return None
        # Use latest quote for freshest current price
        try:
            q = get_latest_quote("SPY")
            bid = float(q.get("bp") or 0)
            ask = float(q.get("ap") or 0)
            current = (bid + ask) / 2 if bid > 0 and ask > 0 else 0
        except Exception:
            current = 0
        # Fall back to bar's close if quote unavailable (pre-market / after-hours)
        if current <= 0:
            current = float(today_bar.get("c") or today_bar.get("close") or 0)
        if current <= 0:
            return None
        return round((current - open_price) / open_price * 100, 2)
    except Exception as e:
        print(f"  [spy-intraday-warn] {type(e).__name__}: {e}")
        return None


def run_naked_entry() -> dict:
    """S3 entry attempt. Reads latest catalyst scan, picks proceed candidates,
    builds contracts (delta + AWR + bid-ask filters), executes orders."""
    print("[naked-entry] looking for entries")
    set_active_strategy(STRATEGY_NAKED)

    regime = get_active_regime()
    if not regime.get("new_trades_allowed", True):
        print(f"  regime={regime.get('regime')} -> no new trades")
        return {"status": "ok", "strategy": STRATEGY_NAKED, "mode": "entry", "opened": [],
                "reason": f"regime {regime.get('regime')} blocks new trades"}

    # Live intraday SPY check — the 8:30 AM regime snapshot can miss intraday reversals.
    # If SPY is already down >1.5% from today's open, treat as yellow: block new bullish
    # entries but still allow bearish ones. Down >3% = block all new entries.
    spy_move = _get_spy_intraday_move()
    intraday_bias = "neutral"
    if spy_move is not None:
        print(f"  SPY intraday move: {spy_move:+.2f}%")
        if spy_move <= -3.0:
            print(f"  SPY down {spy_move:.1f}% intraday -> blocking all new entries")
            return {"status": "ok", "strategy": STRATEGY_NAKED, "mode": "entry", "opened": [],
                    "reason": f"SPY down {spy_move:.1f}% intraday - too risky for new entries"}
        elif spy_move <= -1.5:
            intraday_bias = "bearish"
            print(f"  SPY down {spy_move:.1f}% intraday -> bearish bias, blocking bullish entries")
        elif spy_move >= 1.5:
            intraday_bias = "bullish"
            print(f"  SPY up {spy_move:.1f}% intraday -> bullish confirmation")

    # Load latest STRAT3 scan from DDB
    today_utc = datetime.now(timezone.utc)
    scan = None
    for d in range(2):
        day = today_utc - timedelta(days=d)
        resp = trade_tbl.query(
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": f"STRAT3#NOTE#{day.strftime('%Y%m%d')}"},
            ScanIndexForward=False, Limit=1,
        )
        items = resp.get("Items", [])
        if items:
            scan = items[0]; break

    if not scan:
        return {"status": "ok", "strategy": STRATEGY_NAKED, "mode": "entry",
                "opened": [], "reason": "no scan available"}

    try:
        candidates = json.loads(scan.get("candidates") or "[]")
    except Exception as e:
        return {"status": "error", "strategy": STRATEGY_NAKED, "mode": "entry", "error": str(e)}

    # Gated candidates (proceed_to_options=false) are already logged once in
    # run_naked_premarket via save_naked_note. Don't re-log them on every entry
    # attempt — that produces duplicate skip rows in the dashboard activity feed.

    proceed = [c for c in candidates if c.get("proceed_to_options")]
    if not proceed:
        return {"status": "ok", "strategy": STRATEGY_NAKED, "mode": "entry",
                "opened": [], "reason": "no proceed_to_options candidates"}

    budget = compute_naked_budget()
    print(f"  budget: open={budget['open']}/{NAKED_MAX_POSITIONS}, at_risk=${budget['at_risk']:.0f}, slots={budget['slots']}")
    if budget["slots"] <= 0:
        return {"status": "ok", "strategy": STRATEGY_NAKED, "mode": "entry",
                "opened": [], "reason": "max positions"}

    # Load the daily no-retry set — tickers that already failed contract
    # selection or fill this trading day. This survives rescans (which write
    # a fresh scan record) because it lives in its own DDB row.
    no_retry_today = get_naked_no_retry()
    if no_retry_today:
        print(f"  [no-retry] skipping today's exhausted tickers: {sorted(no_retry_today)}")

    opened: list[dict] = []
    skipped: list[dict] = []
    for c in proceed:
        sym = (c.get("ticker") or "").upper()
        if not sym or sym in budget["open_tickers"]:
            skipped.append({"ticker": sym, "reason": "already open or invalid"})
            continue

        # Intraday bias filter: if market is selling off, skip bullish entries;
        # if market is ripping, skip bearish entries.
        # Skip tickers that already failed contract selection or fill today.
        # This check survives rescans (unlike the old scan-record cache).
        if sym in no_retry_today:
            print(f"  [no-retry] {sym}: skipping - exhausted today")
            continue

        c_direction = (c.get("direction") or "bullish").lower()
        if intraday_bias == "bearish" and c_direction == "bullish":
            reason = f"SPY down {spy_move:.1f}% intraday - skipping bullish entry"
            skipped.append({"ticker": sym, "reason": reason})
            print(f"  [intraday-bias] {sym}: {reason}")
            continue
        if intraday_bias == "bullish" and c_direction == "bearish":
            reason = f"SPY up {spy_move:.1f}% intraday - skipping bearish entry"
            skipped.append({"ticker": sym, "reason": reason})
            print(f"  [intraday-bias] {sym}: {reason}")
            continue
        try:
            quote = get_latest_quote(sym)
            current_price = (float(quote.get("bp") or 0) + float(quote.get("ap") or 0)) / 2
        except Exception as e:
            skipped.append({"ticker": sym, "reason": f"quote-fail: {e}"})
            log_event("skip", sym,
                      {"stage": "naked_entry_quote_fail", "score": c.get("score"), "reason": str(e)},
                      c.get("rationale", ""))
            continue
        if current_price <= 0:
            skipped.append({"ticker": sym, "reason": "no live quote"})
            continue

        awr = compute_awr(sym)
        suggested_dte = int(c.get("suggested_dte") or 14)
        contract = build_naked_contract(sym, c["direction"], suggested_dte, current_price, awr)
        if not contract:
            reason = "no contract matched delta/DTE/spread filters"
            skipped.append({"ticker": sym, "reason": reason})
            print(f"  [skip-no-contract] {sym}: {reason}")
            log_event("skip", sym,
                      {"stage": "naked_no_contract", "score": c.get("score"),
                       "direction": c.get("direction"), "suggested_dte": suggested_dte,
                       "reason": reason},
                      c.get("rationale", ""))
            send_telegram(f"<b>SKIP {sym}</b>  score={c.get('score','?')}  {c.get('direction','?')}\nNo contract found (DTE target {suggested_dte}d) - options chain had no match")
            add_naked_no_retry(sym)
            no_retry_today.add(sym)
            continue

        spread = _execute_naked_open(c, contract, current_price, awr)
        if spread:
            opened.append(spread)
            budget["open_tickers"].append(sym)
            budget["slots"] -= 1
            budget["at_risk"] += spread["total_cost"]
        else:
            skipped.append({"ticker": sym, "reason": "execute failed (no fill)"})
            add_naked_no_retry(sym)
            no_retry_today.add(sym)
        if budget["slots"] <= 0:
            break

    return {
        "status":   "ok",
        "strategy": STRATEGY_NAKED,
        "mode":     "entry",
        "opened":   [s["id"] for s in opened],
        "skipped":  skipped,
    }

def _current_naked_value(spread: dict) -> float | None:
    """Current mid price of one contract for the held option. Positive = the
    premium we'd receive on close."""
    syms = [spread["contract_symbol"]]
    snaps = get_option_snapshots(syms)
    snap = snaps.get(spread["contract_symbol"], {}) or {}
    quote = snap.get("latestQuote") or {}
    try:
        bid = float(quote.get("bp") or 0)
        ask = float(quote.get("ap") or 0)
        if bid <= 0 or ask <= 0:
            return None
        return round((bid + ask) / 2, 2)
    except Exception:
        return None

def _current_underlying_price_for_naked(symbol: str) -> float | None:
    try:
        q = get_latest_quote(symbol)
        bid = float(q.get("bp") or 0)
        ask = float(q.get("ap") or 0)
        mid = (bid + ask) / 2
        return round(mid, 2) if mid > 0 else None
    except Exception:
        return None

def _get_exit_bars(symbol: str, n: int = 25) -> list[dict]:
    """Last N 1-minute bars for the underlying — used for exit momentum analysis."""
    start_utc = datetime.now(timezone.utc) - timedelta(minutes=n + 10)
    try:
        bars = get_intraday_bars(symbol, start_utc)
        return bars[-n:] if len(bars) > n else bars
    except Exception:
        return []

def claude_naked_exit_eval(spread: dict, current_value: float,
                            bars_1min: list[dict], regime: dict) -> dict:
    """Ask Claude whether to HOLD, PARTIAL_EXIT, or FULL_EXIT.
    Returns {action, exit_pct, reason}. Defaults to HOLD on any error."""
    premium   = float(spread.get("premium_paid", 0) or 0)
    qty       = int(spread.get("qty", 1) or 1)
    direction = spread.get("direction", "bullish")
    pnl_pct   = round((current_value - premium) / premium * 100, 1) if premium > 0 else 0
    underlying = spread.get("underlying") or spread.get("ticker", "?")
    try:
        dte = (date.fromisoformat(spread["expiry"]) - date.today()).days
    except Exception:
        dte = "?"

    # Compact 1-min data: closes + volumes only (keeps prompt small)
    recent = bars_1min[-20:] if len(bars_1min) >= 20 else bars_1min
    closes  = [round(float(b.get("c") or 0), 2) for b in recent]
    volumes = [int(b.get("v") or 0) for b in recent]

    # Volume trend: recent 5 bars vs prior bars
    if len(volumes) >= 8:
        avg_prior  = sum(volumes[:-5]) / max(len(volumes) - 5, 1)
        avg_recent = sum(volumes[-5:]) / 5
        vol_trend  = ("rising"  if avg_recent > avg_prior * 1.10 else
                      "falling" if avg_recent < avg_prior * 0.85 else "flat")
    else:
        vol_trend = "insufficient data"

    # 9-EMA and VWAP for momentum context
    ema9_val = ema(closes, 9) if len(closes) >= 9 else None
    price_vs_ema = ("above" if closes and ema9_val and closes[-1] > ema9_val else
                    "below" if closes and ema9_val and closes[-1] < ema9_val else "n/a")
    vwap = compute_vwap_today(underlying)
    vwap_str = f"${vwap:.2f}" if vwap else "n/a"

    user_msg = (
        f"POSITION: {underlying} {direction.upper()} {spread.get('option_type','').upper()} "
        f"| {spread.get('contract_symbol')} | qty={qty}\n"
        f"Entry ${premium:.2f}/c | Now ${current_value:.2f} | P&L {pnl_pct:+.1f}% | DTE {dte}\n"
        f"Strike ${spread.get('strike')} | Underlying entry ${float(spread.get('entry_price',0)):.2f}\n"
        f"Rationale: {(spread.get('rationale') or 'n/a')[:100]}\n\n"
        f"1-MIN BARS (last {len(recent)}):\n"
        f"Closes:  {closes}\n"
        f"Volumes: {volumes}\n"
        f"Vol trend: {vol_trend} | Price vs 9-EMA: {price_vs_ema} | VWAP: {vwap_str}\n"
        f"Regime: {regime.get('regime','?')} | Bias: {regime.get('bias','?')}\n\n"
        f"HOLD, PARTIAL_EXIT, or FULL_EXIT?"
    )
    try:
        client = _anthropic_client()
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=80,
            system=SYSTEM_NAKED_EXIT_MANAGER,
            messages=[
                {"role": "user",      "content": user_msg},
                {"role": "assistant", "content": "{"},
            ],
        )
        raw = "{" + (msg.content[0].text if msg.content else "")
        result = _parse_json(raw, fallback={"action": "HOLD", "exit_pct": 100, "reason": "parse-fail"})
        result.setdefault("action", "HOLD")
        result.setdefault("exit_pct", 100)
        result.setdefault("reason", "")
        return result
    except Exception as e:
        print(f"  [exit-eval-fail] {type(e).__name__}: {e}")
        return {"action": "HOLD", "exit_pct": 0, "reason": "claude error - defaulting hold"}

def _partial_close_naked_position(spread: dict, close_qty: int, reason: str,
                                   current_value: float | None) -> dict | None:
    """Close `close_qty` contracts of an open naked position (partial exit).
    Updates DDB qty in place. Returns outcome dict or None on failure."""
    if close_qty <= 0:
        return None
    underlying = spread.get("underlying") or spread.get("ticker") or "BOT"
    if current_value and current_value > 0:
        limit = max(round(current_value * 0.92, 2), 0.01)   # slightly below mid for speed
    else:
        limit = 0.01
    try:
        order = place_single_leg_order(
            symbol=spread["contract_symbol"],
            side="sell",
            limit_price=limit,
            qty=close_qty,
            tif="day",
            position_intent="sell_to_close",
        )
    except Exception as e:
        print(f"  [partial-close-fail] {underlying}: {type(e).__name__}: {e}")
        return None

    filled = _wait_for_fill(order.get("id", ""), target=limit)
    final_premium = round(abs(float(filled if filled else limit)), 2)
    premium_paid  = float(spread.get("premium_paid", 0) or 0)
    pnl_per_c     = final_premium - premium_paid
    pnl_dollars   = round(pnl_per_c * 100 * close_qty, 2)
    pnl_pct       = round(pnl_per_c / premium_paid * 100, 1) if premium_paid > 0 else 0

    # Update remaining qty in DDB (don't archive — position still open)
    remaining = int(spread.get("qty", close_qty) or close_qty) - close_qty
    try:
        trade_tbl.update_item(
            Key={"pk": f"{_pk_prefix()}SPREAD#OPEN", "sk": spread["id"]},
            UpdateExpression="SET qty = :q",
            ExpressionAttributeValues={":q": remaining},
        )
    except Exception as ue:
        print(f"  [partial-qty-update-fail] {type(ue).__name__}: {ue}")

    outcome = {
        "closed_qty":  close_qty,
        "remaining":   remaining,
        "exit_premium": final_premium,
        "pnl_dollars": pnl_dollars,
        "pnl_pct":     pnl_pct,
        "exit_reason": reason,
    }
    log_event("partial_close", underlying, {**spread, **outcome}, reason)
    print(f"  [partial-close] {spread.get('id')} sold {close_qty}c @ ${final_premium:.2f} "
          f"pnl=${pnl_dollars:+.0f} ({pnl_pct:+.1f}%) | {remaining} remaining")
    return outcome

def _evaluate_naked_exit(spread: dict, current_value: float | None,
                         current_underlying: float | None, today: "date") -> tuple:
    """DTE-tiered stop + profit-taking + thesis-failure for one naked option.
    Returns ('close'|'hold', reason)."""
    premium = float(spread.get("premium_paid", 0) or 0)
    if premium <= 0:
        return ("hold", "missing premium basis")
    if current_value is None:
        return ("hold", "no live quote")

    pnl_pct = (current_value - premium) / premium    # 0.50 = +50%
    expiry = date.fromisoformat(spread["expiry"])
    dte = (expiry - today).days

    # 1. Win exits
    if pnl_pct >= 1.00:
        return ("close", f"+{pnl_pct*100:.0f}% gain (full target hit)")
    if pnl_pct >= 0.50 and dte <= 10:
        return ("close", f"+{pnl_pct*100:.0f}% gain at {dte}DTE (time-adjusted)")

    # 2. DTE-tiered loss stop
    stop_pct = naked_stop_for_dte(dte)
    if pnl_pct <= -stop_pct:
        return ("close", f"{pnl_pct*100:.0f}% stop loss ({dte}DTE tier -{stop_pct*100:.0f}%)")

    # 3. Absolute floor
    if pnl_pct <= -NAKED_STOP_FLOOR_PCT:
        return ("close", f"{pnl_pct*100:.0f}% absolute floor (-{NAKED_STOP_FLOOR_PCT*100:.0f}%)")

    # 4. Time stops
    if dte <= 0:
        return ("close", "0DTE end of day - close any remaining")
    if dte <= 1:
        return ("close", f"{dte}DTE - too close to expiry")

    # 5. Thesis-failure: price moved against entry
    entry_price = float(spread.get("entry_price", 0) or 0)
    if entry_price > 0 and current_underlying is not None:
        direction = (spread.get("direction") or "").lower()
        # Use 1.5% buffer to avoid intraday noise triggering close
        if direction == "bullish" and current_underlying < entry_price * 0.985:
            return ("close", f"price ${current_underlying} below entry ${entry_price} (bull invalidated)")
        if direction == "bearish" and current_underlying > entry_price * 1.015:
            return ("close", f"price ${current_underlying} above entry ${entry_price} (bear invalidated)")

    return ("hold", f"pnl {pnl_pct*100:+.0f}% dte {dte}")

def _close_naked_position(spread: dict, reason: str, current_value: float | None) -> dict:
    """Sell-to-close the naked option and archive the result."""
    qty = int(spread.get("qty", 1) or 1)
    if current_value and current_value > 0:
        # Aim slightly below mid to ensure fill on a closing sell
        limit = max(round(current_value * 0.9, 2), 0.01)
    else:
        limit = 0.01

    underlying = spread.get("underlying") or spread.get("ticker") or "BOT"
    try:
        order = place_single_leg_order(
            symbol=spread["contract_symbol"],
            side="sell",
            limit_price=limit,
            qty=qty,
            tif="day",
            position_intent="sell_to_close",
        )
    except Exception as e:
        err_str = str(e)
        # Phantom-position detection: Alpaca returns 422 with "inferred: sell_to_open"
        # when there's no long position to close. Could happen from a never-filled
        # open, an external manual close, or a split-adjusted symbol change.
        # Archive the spread as phantom so we stop retrying every 15 min forever.
        if "position intent mismatch" in err_str or "inferred: sell_to_open" in err_str:
            print(f"  [naked-phantom] {spread.get('id')}: Alpaca shows no position - auto-archiving")
            outcome = {
                "exit_premium":  0,
                "exit_total":    0,
                "pnl_dollars":   0,
                "pnl_pct":       0,
                "exit_reason":   "phantom position - Alpaca had no holding (auto-archived)",
            }
            try:
                archive_spread(spread, outcome)
                log_event("close", underlying, {**spread, **outcome},
                          "phantom-archive after position-intent-mismatch")
            except Exception as ae:
                print(f"  [phantom-archive-fail] {type(ae).__name__}: {ae}")
            return {**outcome, "phantom": True}
        print(f"  [naked-close-fail] {spread.get('id')}: {type(e).__name__}: {e}")
        log_event("error", underlying,
                  {"stage": "naked_close", "error": str(e), "id": spread.get("id")}, reason)
        return {"pnl_dollars": 0, "exit_reason": f"{reason} (order error)", "error": True}

    filled = _wait_for_fill(order.get("id", ""), target=limit)
    final_premium = round(abs(float(filled if filled else limit)), 2)
    premium_paid = float(spread.get("premium_paid", 0) or 0)
    pnl_per_contract = final_premium - premium_paid
    pnl_dollars = round(pnl_per_contract * 100 * qty, 2)
    pnl_pct = (pnl_per_contract / premium_paid * 100) if premium_paid > 0 else 0.0

    outcome = {
        "exit_premium":  final_premium,
        "exit_total":    round(final_premium * 100 * qty, 2),
        "pnl_dollars":   pnl_dollars,
        "pnl_pct":       round(pnl_pct, 1),
        "exit_reason":   reason,
    }
    archive_spread(spread, outcome)
    log_event("close", underlying, {**spread, **outcome}, reason)
    send_position_alert("close", spread, outcome)   # labels itself as Naked or Squeeze via spread.kind
    print(f"  [close-naked] {spread.get('id')} {reason} sold @ ${final_premium:.2f}/c "
          f"pnl=${pnl_dollars:+.0f} ({pnl_pct:+.1f}%)")
    return outcome

def _sweep_naked_exits() -> dict:
    """Two-tier exit sweep for open naked positions.

    Tier 1 (pure code, always runs): hard stop loss, absolute floor, time exits,
    thesis-failure price checks. These fire immediately — no Claude call.

    Tier 2 (Claude active management): for positions that pass the hard gates,
    Claude reviews 1-min momentum, volume trend, VWAP, and 9-EMA to decide
    HOLD | PARTIAL_EXIT | FULL_EXIT. This replaces mechanical profit targets
    with context-aware judgment on every sweep."""
    set_active_strategy(STRATEGY_NAKED)
    opens = load_open_spreads()
    if not opens:
        return {"checked": 0, "closed": 0, "partial": 0, "decisions": []}
    today   = date.today()
    regime  = get_active_regime()
    closed  = 0
    partial = 0
    decisions = []

    # Load earnings calendar once for the whole sweep
    earnings_map = {}
    try:
        earnings_map = get_earnings_dates()
    except Exception as e:
        print(f"  [sweep-earnings-warn] {type(e).__name__}: {e}")

    for s in opens:
        underlying = s.get("underlying") or s.get("ticker")
        if not underlying:
            continue
        value    = _current_naked_value(s)
        ul_price = _current_underlying_price_for_naked(underlying)

        # --- Tier 0: earnings wall exit (Option A) ---
        # If the underlying has earnings today or tomorrow, exit before the binary event.
        # We want the pre-earnings drift gain — we do NOT want to ride through IV crush.
        earn_date_str = earnings_map.get(underlying.upper())
        if earn_date_str:
            try:
                earn_date = date.fromisoformat(earn_date_str)
                days_to_earn = (earn_date - today).days
                if 0 <= days_to_earn <= 1:
                    reason = f"earnings wall: {underlying} reports in {days_to_earn}d ({earn_date_str}) - exiting before binary event"
                    print(f"  [earnings-wall] {s.get('id')} - {reason}")
                    outcome = _close_naked_position(s, reason, value)
                    decisions.append({"id": s.get("id"), "tier": 0, "action": "close",
                                      "reason": reason, "current_value": value})
                    if outcome and not outcome.get("error") and not outcome.get("phantom"):
                        closed += 1
                    elif outcome and outcome.get("phantom"):
                        decisions[-1]["phantom_archived"] = True
                    continue
            except Exception as e:
                print(f"  [earnings-wall-warn] {underlying}: {type(e).__name__}: {e}")

        # --- Tier 1: hard exits (pure code, no Claude) ---
        action, reason = _evaluate_naked_exit(s, value, ul_price, today)
        if action == "close":
            print(f"  [hard-stop] {s.get('id')} - {reason}")
            outcome = _close_naked_position(s, reason, value)
            decisions.append({"id": s.get("id"), "tier": 1, "action": "close",
                               "reason": reason, "current_value": value})
            if outcome and not outcome.get("error") and not outcome.get("phantom"):
                closed += 1
            elif outcome and outcome.get("phantom"):
                decisions[-1]["phantom_archived"] = True
            continue   # don't run Claude eval on a position we just closed

        # --- Tier 2: Claude active management ---
        if value is None:
            decisions.append({"id": s.get("id"), "tier": 2, "action": "hold",
                               "reason": "no live quote for Claude eval"})
            continue
        try:
            bars_1min = _get_exit_bars(underlying, n=25)
            verdict   = claude_naked_exit_eval(s, value, bars_1min, regime)
        except Exception as e:
            print(f"  [claude-exit-warn] {underlying}: {type(e).__name__}: {e}")
            decisions.append({"id": s.get("id"), "tier": 2, "action": "hold",
                               "reason": f"claude-error: {e}"})
            continue

        claude_action = (verdict.get("action") or "HOLD").upper()
        claude_reason = verdict.get("reason", "")
        print(f"  [claude-exit] {s.get('id')} -> {claude_action}: {claude_reason}")
        decisions.append({"id": s.get("id"), "tier": 2, "action": claude_action,
                          "reason": claude_reason, "current_value": value,
                          "pnl_pct": round((value - float(s.get("premium_paid",0) or 0))
                                           / max(float(s.get("premium_paid",1) or 1), 0.01) * 100, 1)})

        if claude_action == "FULL_EXIT":
            outcome = _close_naked_position(s, f"Claude: {claude_reason}", value)
            if outcome and not outcome.get("error") and not outcome.get("phantom"):
                closed += 1

        elif claude_action == "PARTIAL_EXIT":
            exit_pct  = max(25, min(75, int(verdict.get("exit_pct") or 50)))
            total_qty = int(s.get("qty", 1) or 1)
            close_qty = max(1, round(total_qty * exit_pct / 100))
            if close_qty >= total_qty:
                # Partial would close everything — just do a full exit
                outcome = _close_naked_position(s, f"Claude partial->full: {claude_reason}", value)
                if outcome and not outcome.get("error"):
                    closed += 1
            else:
                outcome = _partial_close_naked_position(s, close_qty,
                                                         f"Claude partial {exit_pct}%: {claude_reason}",
                                                         value)
                if outcome:
                    partial += 1
                    decisions[-1]["partial_qty"] = close_qty

        # HOLD: nothing to do

    return {"checked": len(opens), "closed": closed, "partial": partial, "decisions": decisions}

def run_naked_exit_sweep() -> dict:
    """Lightweight intraday exit sweep — runs every 15 min during market hours.
    After closing a position, checks if the reflection trigger threshold is met."""
    print("[naked-exit-sweep] running")
    try:
        result = _sweep_naked_exits()
    except Exception as e:
        print(f"  [exit-sweep-fail] {type(e).__name__}: {e}")
        return {"status": "error", "strategy": STRATEGY_NAKED, "mode": "exit_sweep", "error": str(e)}

    closed_this_sweep = result.get("closed", 0)
    print(f"  checked={result['checked']}, closed={result['closed']}, partial={result.get('partial',0)}")

    # Auto-trigger reflection if enough trades have accumulated since last run
    if closed_this_sweep > 0:
        try:
            since_last = _count_closed_since_last_reflect()
            print(f"  [weeklies-reflect-check] {since_last} trades since last reflection (threshold={WEEKLIES_REFLECT_EVERY})")
            if since_last >= WEEKLIES_REFLECT_EVERY:
                print("  [weeklies-reflect-check] threshold met — triggering reflection")
                reflect_result = run_weeklies_reflect()
                result["reflection_triggered"] = True
                result["reflection"] = reflect_result.get("action", "?")
        except Exception as re:
            print(f"  [weeklies-reflect-auto-warn] {type(re).__name__}: {re}")

    return {"status": "ok", "strategy": STRATEGY_NAKED, "mode": "exit_sweep", **result}

def run_naked_check() -> dict:
    """Strategy 3 diagnostic stub. Verifies: dispatcher routes correctly, 3rd
    account keys load, catalyst calendar reads, AWR computes for one ticker."""
    print("[naked-check] strategy=naked dispatcher hit")
    try:
        acct = get_account()
        equity = acct.get("equity", "?")
        bp = acct.get("buying_power", "?")
        print(f"  account ok, equity=${equity}, buying_power=${bp}")
    except Exception as e:
        print(f"  [naked-account-fail] {type(e).__name__}: {e}")
        return {"status": "error", "strategy": STRATEGY_NAKED, "mode": "naked_check", "error": str(e)}

    # Phase B additions: catalyst calendar + AWR sample
    catalysts = get_active_catalysts(lookforward_days=28)
    print(f"  catalysts (next 28d): {len(catalysts)}")
    awr_sample = {}
    for sym in ("SPY", "QQQ", "NVDA"):
        awr = compute_awr(sym)
        awr_sample[sym] = awr
        print(f"  AWR {sym} (8wk): ${awr}")

    return {
        "status":               "ok",
        "strategy":             STRATEGY_NAKED,
        "mode":                 "naked_check",
        "note":                 "dispatch + keys + catalyst + AWR verified",
        "universe":             get_naked_universe(),
        "account_equity":       acct.get("equity"),
        "account_buying_power": acct.get("buying_power"),
        "catalysts_28d":        catalysts,
        "awr_sample":           awr_sample,
    }

def run_momentum_exit_sweep() -> dict:
    """Lightweight intraday exit sweep — runs every 15 min during market hours.
    Same exit rules as the scheduled postopen/midday/preclose sweeps, no Claude
    calls. Lets the bot react to scale-out / stop-loss triggers within minutes
    instead of waiting for the next big scheduled run."""
    print("[momentum-exit-sweep] running")
    try:
        result = _sweep_momentum_exits(mode="intraday")
    except Exception as e:
        print(f"  [exit-sweep-fail] {type(e).__name__}: {e}")
        return {"status": "error", "strategy": STRATEGY_MOMENTUM, "mode": "exit_sweep", "error": str(e)}
    print(f"  checked={result['checked']}, closed={result['closed']}")
    return {"status": "ok", "strategy": STRATEGY_MOMENTUM, "mode": "exit_sweep", **result}

def run_momentum_preclose() -> dict:
    """15:00 ET. Final sweep with tightened time-stop logic before close."""
    print("[momentum-preclose] final sweep")
    try:
        result = _sweep_momentum_exits(mode="preclose")
    except Exception as e:
        print(f"  [preclose-fail] {type(e).__name__}: {e}")
        return {"status": "error", "strategy": STRATEGY_MOMENTUM, "mode": "preclose", "error": str(e)}
    print(f"  checked={result['checked']}, closed={result['closed']}")
    return {"status": "ok", "strategy": STRATEGY_MOMENTUM, "mode": "preclose", **result}

# --- Momentum entry helpers (used by run_momentum_midday) ------------------

def momentum_spread_width(price: float) -> float:
    """Width tier from spec: $5 ETFs/<$100, $10 mid-price, $20 high-price."""
    if price < 100: return 5.0
    if price < 500: return 10.0
    return 20.0

def _latest_momentum_scan() -> dict | None:
    """Read the most recent STRAT2#NOTE# row (today's premarket scan output)."""
    today_utc = datetime.now(timezone.utc)
    for d in range(2):  # today + yesterday in case midday runs before premarket
        day = today_utc - timedelta(days=d)
        resp = trade_tbl.query(
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": f"STRAT2#NOTE#{day.strftime('%Y%m%d')}"},
            ScanIndexForward=False,
            Limit=1,
        )
        items = resp.get("Items", [])
        if items:
            return items[0]
    return None

def compute_momentum_budget() -> dict:
    opens = load_open_spreads()  # uses STRAT2# prefix because _ACTIVE_STRATEGY=momentum
    at_risk = sum(float(s.get("max_loss_dollars", 0)) for s in opens)
    slots = max(MOMENTUM_MAX_POSITIONS - len(opens), 0)
    return {
        "open":     len(opens),
        "at_risk":  at_risk,
        "slots":    slots,
        "cap_left": max(MOMENTUM_TOTAL_RISK_CAP - at_risk, 0.0),
        "open_tickers": [s.get("ticker", s.get("underlying")) for s in opens],
    }

def build_momentum_pair(symbol: str, direction: str, current_price: float, atr14: float | None, max_dte_override: int | None = None) -> dict | None:
    """Pull the chain (calls for bullish, puts for bearish), find an ATM long
    + ~0.30 delta short pair, return None if nothing passes the gates.
    max_dte_override: when earnings is approaching, cap DTE so the spread
    expires before the announcement (avoids IV-crush exposure)."""
    today = date.today()
    effective_max_dte = MOMENTUM_DTE_MAX if max_dte_override is None else max(MOMENTUM_DTE_MIN, min(MOMENTUM_DTE_MAX, max_dte_override))
    gte = (today + timedelta(days=MOMENTUM_DTE_MIN)).isoformat()
    lte = (today + timedelta(days=effective_max_dte)).isoformat()
    contract_type = "call" if direction == "bullish" else "put"
    contracts = get_options_contracts(symbol, gte, lte, contract_type=contract_type)
    if not contracts:
        return None
    snaps = get_option_snapshots([c["symbol"] for c in contracts])

    by_expiry: dict[str, list[dict]] = {}
    for c in contracts:
        snap = snaps.get(c["symbol"], {})
        greeks = snap.get("greeks") or {}
        quote  = snap.get("latestQuote") or {}
        delta = greeks.get("delta")
        bid = float(quote.get("bp") or 0)
        ask = float(quote.get("ap") or 0)
        if delta is None or bid <= 0 or ask <= 0:
            continue
        by_expiry.setdefault(c["expiration_date"], []).append({
            "symbol": c["symbol"],
            "strike": float(c["strike_price"]),
            "expiry": c["expiration_date"],
            "delta":  float(delta),
            "bid":    bid,
            "ask":    ask,
        })

    width = momentum_spread_width(current_price)
    delta_lo = MOMENTUM_TARGET_DELTA_SHORT - MOMENTUM_DELTA_TOLERANCE
    delta_hi = MOMENTUM_TARGET_DELTA_SHORT + MOMENTUM_DELTA_TOLERANCE

    best = None
    for expiry, items in by_expiry.items():
        items.sort(key=lambda x: x["strike"])
        strike_map = {round(it["strike"], 2): it for it in items}
        # Long leg: strike closest to current price (ATM)
        long_leg = min(items, key=lambda it: abs(it["strike"] - current_price))
        # Short leg: width away in the directional sense
        short_strike = long_leg["strike"] + width if direction == "bullish" else long_leg["strike"] - width
        short_leg = strike_map.get(round(short_strike, 2))
        if not short_leg:
            continue
        if not (delta_lo <= abs(short_leg["delta"]) <= delta_hi):
            continue
        long_mid  = (long_leg["bid"]  + long_leg["ask"])  / 2
        short_mid = (short_leg["bid"] + short_leg["ask"]) / 2
        debit = round(long_mid - short_mid, 2)
        if debit <= 0:
            continue
        max_gain = round(width - debit, 2)
        rr = round(max_gain / debit, 2) if debit > 0 else 0
        # Breakeven: bull call adds debit to long strike; bear put subtracts.
        breakeven = (long_leg["strike"] + debit) if direction == "bullish" else (long_leg["strike"] - debit)
        breakeven_move = abs(breakeven - current_price)
        atr_ok = (atr14 is None) or (breakeven_move <= MOMENTUM_BREAKEVEN_ATR_MAX * atr14)
        candidate = {
            "expiry":         expiry,
            "dte":            (date.fromisoformat(expiry) - today).days,
            "long":           long_leg,
            "short":          short_leg,
            "width":          width,
            "debit_est":      debit,
            "max_gain":       max_gain,
            "rr_ratio":       rr,
            "breakeven":      round(breakeven, 2),
            "breakeven_move": round(breakeven_move, 2),
            "atr_ok":         atr_ok,
            "max_loss_dollars": round(debit * 100, 2),
        }
        # Prefer higher reward/risk; tiebreak on lower DTE (faster decay window for theta-positive direction is irrelevant; we want the move sooner)
        if best is None or rr > best["rr_ratio"] or (rr == best["rr_ratio"] and candidate["dte"] < best["dte"]):
            best = candidate
    return best

SYSTEM_MOMENTUM_MIDDAY = """You decide which momentum debit spread candidates to enter today. You receive pre-screened pairs that already passed delta and width filters. Reject anything that fails the rules below. Return JSON only, no preamble.

Hard rules - reject if any fail:
- reward/risk ratio < 2.0
- max loss dollars > $250 for score 8-10, > $150 for score 6-7
- breakeven move > 1.5x the 14-day ATR (atr_ok = false in the input)
- DTE < 7 unless the score is 9-10 with breakout clearly underway
- account at max positions (5) or budget cap exceeded
- sector concentration: max 2 positions per sector, both must score 8+

Be selective: prefer Layer 1 ETFs (QQQ/SPY/IWM/SMH) over individual stocks while paper-trading. Skip if any single signal feels weak.

Schema:
{"trades": [{"ticker": "QQQ", "direction": "bullish", "buy_strike": 479, "sell_strike": 489, "expiration": "2026-05-09", "dte": 14, "est_debit": 3.20, "max_gain": 6.80, "reward_risk": 2.13, "breakeven": 482.20, "atr_ok": true, "enter_today": true, "max_loss_dollars": 320, "reason": "<20 words>"}], "skips": {"AMD": "rr 1.6 below threshold"}}"""

def claude_momentum_picks(candidate_pairs: list[dict], budget: dict, vix: float | None) -> dict:
    """Send pre-screened pairs to Claude for entry approval."""
    client = _anthropic_client()
    lines = [
        f"VIX: {vix if vix is not None else 'n/a'}",
        f"Open positions: {budget['open']}/{MOMENTUM_MAX_POSITIONS}",
        f"At-risk: ${budget['at_risk']:.0f} / ${MOMENTUM_TOTAL_RISK_CAP:.0f}",
        f"Slots remaining: {budget['slots']}",
        f"Already-open tickers (skip duplicates): {budget['open_tickers']}",
        "",
        "Candidate pairs:",
    ]
    for c in candidate_pairs:
        sec = c.get("sector", "?")
        lines.append(
            f"  {c['ticker']} {c['direction']} score={c['score']}/10 sector={sec} setup={c['setup_type']}"
        )
        lines.append(
            f"    LONG  {c['long_symbol']} K={c['long_strike']:.0f} delta={c['long_delta']:.3f} bid/ask={c['long_bid']:.2f}/{c['long_ask']:.2f}"
        )
        lines.append(
            f"    SHORT {c['short_symbol']} K={c['short_strike']:.0f} delta={c['short_delta']:.3f} bid/ask={c['short_bid']:.2f}/{c['short_ask']:.2f}"
        )
        lines.append(
            f"    width=${c['width']:.0f} debit=${c['debit_est']:.2f} max_gain=${c['max_gain']:.2f} rr={c['rr_ratio']} "
            f"breakeven=${c['breakeven']} move=${c['breakeven_move']} atr_ok={c['atr_ok']} dte={c['dte']} max_loss=${c['max_loss_dollars']}"
        )
        lines.append(f"    signals: {c.get('signals', [])}")
        lines.append("")
    prompt = "\n".join(lines)
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=600,
        system=SYSTEM_MOMENTUM_MIDDAY,
        messages=[
            {"role": "user", "content": prompt},
            {"role": "assistant", "content": "{"},
        ],
    )
    raw = msg.content[0].text.strip()
    text = raw if raw.startswith("{") else "{" + raw
    return _parse_json(text, fallback={"trades": [], "skips": {}})

def _execute_momentum_open(decision: dict, pair: dict, current_price: float, score: int, score_breakdown: dict | None = None) -> dict | None:
    """Place a debit-spread order matching what Claude approved.
    Score-tiered max debit: $250 for 8-10, $150 for 6-7."""
    underlying = decision["ticker"]
    direction  = decision["direction"]
    debit      = float(decision.get("est_debit") or pair["debit_est"])
    max_loss   = round(debit * 100, 2)

    score_cap = momentum_size_for_score(score)
    if score_cap <= 0:
        print(f"  [skip] {underlying}: score {score} below entry threshold")
        return None

    # Final budget check (Claude is advisory; we enforce the hard limit)
    budget = compute_momentum_budget()
    if budget["slots"] <= 0 or (budget["at_risk"] + max_loss) > MOMENTUM_TOTAL_RISK_CAP:
        print(f"  [skip] {underlying}: budget full (open={budget['open']}, at_risk=${budget['at_risk']:.0f})")
        return None
    if max_loss > score_cap * 1.05:
        print(f"  [skip] {underlying}: debit ${max_loss} exceeds score-{score} cap ${score_cap:.0f}")
        return None

    # mleg legs: BUY the long strike, SELL the short strike, both opening
    legs = [
        {"symbol": pair["long"]["symbol"],  "side": "buy",  "ratio_qty": "1", "position_intent": "buy_to_open"},
        {"symbol": pair["short"]["symbol"], "side": "sell", "ratio_qty": "1", "position_intent": "sell_to_open"},
    ]
    try:
        order = place_mleg_order(legs, debit, tif="day")
    except Exception as e:
        print(f"  [order-fail] {underlying}: {type(e).__name__}: {e}")
        log_event("error", underlying, {"stage": "momentum_open", "error": str(e)}, decision.get("reason", ""))
        return None

    # Poll briefly for fill. abs() because mleg fill prices arrive signed.
    filled = _wait_for_fill(order["id"], target=debit) or debit
    final_debit = round(abs(float(filled)), 2)

    spread = {
        "id":              f"M-{underlying}-{pair['expiry']}-{int(pair['long']['strike'])}-{int(pair['short']['strike'])}-{uuid.uuid4().hex[:6]}",
        "strategy":        STRATEGY_MOMENTUM,
        "underlying":      underlying,         # parity with credit-spread schema
        "ticker":          underlying,         # convenient alias
        "direction":       direction,
        "long_symbol":     pair["long"]["symbol"],
        "short_symbol":    pair["short"]["symbol"],
        "long_strike":     pair["long"]["strike"],
        "short_strike":    pair["short"]["strike"],
        "width":           pair["width"],
        "expiry":          pair["expiry"],
        "dte":             pair["dte"],
        "debit":           final_debit,
        "max_loss_dollars": round(final_debit * 100, 2),
        "max_gain_dollars": round((pair["width"] - final_debit) * 100, 2),
        "breakeven":       pair["breakeven"],
        "entry_price":     current_price,            # captured for setup-invalidation exit (Phase 4)
        "open_order_id":   order["id"],
        "opened_at":       datetime.now(timezone.utc).isoformat(),
        "status":          "open",
        "score":           score,
        "score_breakdown": score_breakdown or {},
        "sector":          get_sector(underlying),
        "setup_type":      decision.get("setup_type", ""),
    }
    save_open_spread(spread)  # routes to STRAT2#SPREAD#OPEN via _pk_prefix()
    log_event("open", underlying, spread, decision.get("reason", ""))
    print(f"  [open-momentum] {underlying} {direction} score={score} sector={get_sector(underlying)} {int(pair['long']['strike'])}/{int(pair['short']['strike'])} debit=${final_debit:.2f}")
    return spread

def run_momentum_midday(_orb_filter: set | None = None) -> dict:
    """12:00 ET. Sweep exits first (always), then enter new trades only if
    the regime allows. Yellow regime tightens score floor + halves debit cap.
    _orb_filter: when called from run_momentum_orb(), restricts candidates to
    only those that confirmed an opening-range breakout. None = no filter."""
    print("[momentum-midday] sweep + entries" + (f" (ORB filter: {sorted(_orb_filter)})" if _orb_filter else ""))

    # Step 1: exit sweep on any open positions (runs in any regime)
    try:
        sweep = _sweep_momentum_exits(mode="midday")
        print(f"  exit sweep: checked={sweep['checked']}, closed={sweep['closed']}")
    except Exception as e:
        print(f"  [sweep-warn] {type(e).__name__}: {e}")

    # Step 2: regime gate before any entries
    regime = get_active_regime()
    if not regime.get("new_trades_allowed", True):
        print(f"  regime={regime.get('regime')} -> no new trades ({regime.get('notes')})")
        return {
            "status":   "ok",
            "strategy": STRATEGY_MOMENTUM,
            "mode":     "midday",
            "opened":   [],
            "regime":   regime,
            "reason":   f"regime {regime.get('regime')} blocks new trades",
        }
    is_yellow = regime.get("regime") == "yellow"
    yellow_bias = (regime.get("bias") or "").lower()
    if is_yellow:
        print(f"  regime=yellow -> bullish-only score-8+ at $150 cap (bias={yellow_bias})")

    # Step 3: entries
    scan = _latest_momentum_scan()
    if not scan:
        print("  no premarket scan available - skipping")
        return {"status": "ok", "strategy": STRATEGY_MOMENTUM, "mode": "midday", "opened": [], "reason": "no scan"}

    try:
        candidates = json.loads(scan.get("candidates") or "[]")
        summaries  = json.loads(scan.get("summaries")  or "[]")
    except Exception as e:
        print(f"  [scan-parse-fail] {e}")
        return {"status": "error", "strategy": STRATEGY_MOMENTUM, "mode": "midday", "error": str(e)}

    proceed = [c for c in candidates if c.get("proceed_to_options")]
    if _orb_filter is not None:
        proceed = [c for c in proceed if (c.get("ticker") or "").upper() in _orb_filter]
    if not proceed:
        reason = "no ORB-confirmed candidates" if _orb_filter is not None else "no candidates"
        print(f"  {reason}")
        return {"status": "ok", "strategy": STRATEGY_MOMENTUM, "mode": "midday", "opened": [], "reason": reason}

    budget = compute_momentum_budget()
    print(f"  budget: open={budget['open']}/{MOMENTUM_MAX_POSITIONS}, at_risk=${budget['at_risk']:.0f}, slots={budget['slots']}")
    if budget["slots"] <= 0:
        return {"status": "ok", "strategy": STRATEGY_MOMENTUM, "mode": "midday", "opened": [], "reason": "no slots"}

    sum_by_sym = {s["symbol"]: s for s in summaries}
    etf_set = {"SPY", "QQQ", "IWM", "SMH"}

    # Build current sector counts from already-open positions for the sector rule.
    open_spreads_now = load_open_spreads()
    sector_counts: dict[str, int] = {}
    sector_min_score: dict[str, int] = {}  # min score among existing positions in each sector
    for os in open_spreads_now:
        sec = os.get("sector") or get_sector(os.get("underlying") or os.get("ticker") or "")
        sector_counts[sec] = sector_counts.get(sec, 0) + 1
        try:
            sec_score = int(os.get("score") or 0)
        except Exception:
            sec_score = 0
        sector_min_score[sec] = min(sector_min_score.get(sec, sec_score), sec_score)

    pair_data: list[dict] = []
    for c in proceed:
        sym = c["ticker"]
        if sym in budget["open_tickers"]:
            print(f"  [skip] {sym}: already have an open position")
            continue
        s = sum_by_sym.get(sym)
        if not s or s.get("price") is None:
            print(f"  [skip] {sym}: no price in summary")
            continue

        # Earnings handling: allow approaching earnings but cap the spread DTE
        # so it expires BEFORE the announcement (avoids IV-crush exposure).
        # Only hard-skip if earnings is today or tomorrow.
        days_to_earn = s.get("days_to_earn")
        max_dte_override: int | None = None
        if sym not in etf_set and days_to_earn is not None:
            if days_to_earn < MOMENTUM_EARNINGS_MIN_DAYS:
                print(f"  [skip] {sym}: earnings in {days_to_earn}d (within {MOMENTUM_EARNINGS_MIN_DAYS}d floor)")
                continue
            if days_to_earn <= MOMENTUM_DTE_MAX:
                # Cap DTE so spread expires the day before earnings
                max_dte_override = days_to_earn - 1
                if max_dte_override < MOMENTUM_DTE_MIN:
                    print(f"  [skip] {sym}: earnings in {days_to_earn}d - no safe DTE window (need >={MOMENTUM_DTE_MIN}d)")
                    continue
                print(f"  [earnings-cap] {sym}: capping DTE at {max_dte_override}d to expire before earnings ({days_to_earn}d out)")

        # Score-tier sizing gate (yellow regime tightens to 8+)
        score = int(c.get("score") or 0)
        score_floor = MOMENTUM_SCORE_TIER_HIGH if is_yellow else MOMENTUM_SCORE_MIN_ENTRY
        if score < score_floor:
            print(f"  [skip] {sym}: score {score} < floor {score_floor} (regime {regime.get('regime')})")
            continue

        # Yellow regime: bullish-only entries (per spec)
        if is_yellow and (c.get("direction", "").lower() != "bullish"):
            print(f"  [skip] {sym}: yellow regime allows bullish-only ({c.get('direction')})")
            continue

        # Sector concentration rule:
        #  - Reject 3rd in same sector
        #  - Reject 2nd in same sector unless BOTH this and existing are 8+
        sector = get_sector(sym)
        scnt = sector_counts.get(sector, 0)
        if scnt >= 2:
            print(f"  [skip] {sym}: sector '{sector}' already has 2 positions (max)")
            continue
        if scnt == 1:
            existing_min = sector_min_score.get(sector, 0)
            if score < MOMENTUM_SCORE_TIER_HIGH or existing_min < MOMENTUM_SCORE_TIER_HIGH:
                print(f"  [skip] {sym}: sector '{sector}' add requires both positions score 8+ (this={score}, existing min={existing_min})")
                continue

        pair = build_momentum_pair(sym, c["direction"], float(s["price"]), s.get("atr14"), max_dte_override=max_dte_override)
        if not pair:
            print(f"  [skip] {sym}: no valid pair found in 7-21 DTE chain")
            continue
        if pair["rr_ratio"] < MOMENTUM_REWARD_RISK_MIN:
            print(f"  [skip] {sym}: rr {pair['rr_ratio']} < {MOMENTUM_REWARD_RISK_MIN}")
            continue
        if not pair["atr_ok"]:
            print(f"  [skip] {sym}: breakeven move {pair['breakeven_move']} > 1.5x ATR ({s.get('atr14')})")
            continue
        score_cap = momentum_size_for_score(score)
        if is_yellow:
            # Per spec: yellow regime caps debit at $150 regardless of score
            score_cap = min(score_cap, MOMENTUM_DEBIT_MID)
        if pair["max_loss_dollars"] > score_cap:
            cap_label = f"yellow-${MOMENTUM_DEBIT_MID:.0f}" if is_yellow else f"score-{score} ${score_cap:.0f}"
            print(f"  [skip] {sym}: max loss ${pair['max_loss_dollars']} > {cap_label}")
            continue
        pair_data.append({
            "ticker":         sym,
            "direction":      c["direction"],
            "score":          score,
            "score_breakdown": c.get("score_breakdown", {}),
            "sector":         sector,
            "setup_type":     c.get("setup_type", ""),
            "signals":        c.get("signals", []),
            "long_symbol":    pair["long"]["symbol"],
            "short_symbol":   pair["short"]["symbol"],
            "long_strike":    pair["long"]["strike"],
            "short_strike":   pair["short"]["strike"],
            "long_delta":     pair["long"]["delta"],
            "short_delta":    pair["short"]["delta"],
            "long_bid":       pair["long"]["bid"],
            "long_ask":       pair["long"]["ask"],
            "short_bid":      pair["short"]["bid"],
            "short_ask":      pair["short"]["ask"],
            "width":          pair["width"],
            "expiry":         pair["expiry"],
            "dte":            pair["dte"],
            "debit_est":      pair["debit_est"],
            "max_gain":       pair["max_gain"],
            "rr_ratio":       pair["rr_ratio"],
            "breakeven":      pair["breakeven"],
            "breakeven_move": pair["breakeven_move"],
            "atr_ok":         pair["atr_ok"],
            "max_loss_dollars": pair["max_loss_dollars"],
            "_pair_obj":      pair,             # keep the raw pair for execution
            "_current_price": float(s["price"]),
        })

    if not pair_data:
        print("  no candidates passed pre-screen gates")
        return {"status": "ok", "strategy": STRATEGY_MOMENTUM, "mode": "midday", "opened": [], "reason": "no pairs passed gates"}

    vix = get_vix()
    picks = claude_momentum_picks(pair_data, budget, vix)
    opened = []
    for d in picks.get("trades", []):
        if not d.get("enter_today"):
            continue
        sym = d.get("ticker")
        match = next((p for p in pair_data if p["ticker"] == sym), None)
        if not match:
            print(f"  [skip] {sym}: Claude picked a ticker we didn't pre-screen")
            continue
        spread = _execute_momentum_open(
            d, match["_pair_obj"], match["_current_price"],
            match["score"], match.get("score_breakdown"),
        )
        if spread:
            opened.append(spread)

    print(f"[momentum-midday] opened {len(opened)} spread(s)")
    return {
        "status":   "ok",
        "strategy": STRATEGY_MOMENTUM,
        "mode":     "midday",
        "opened":   [s["id"] for s in opened],
        "skips":    picks.get("skips", {}),
    }

# ---- Daily Suggestion System -----------------------------------------------

def send_telegram(message: str, parse_mode: str = "HTML") -> bool:
    """Send a message to the configured Telegram chat. Returns True on success."""
    try:
        token   = get_secret("/trading-bot/telegram-bot-token")
        chat_id = get_secret("/trading-bot/telegram-chat-id")
        r = http.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": parse_mode,
                  "disable_web_page_preview": True},
            timeout=(5, 15),
        )
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"  [telegram-warn] {type(e).__name__}: {e}")
        return False


def get_stocktwits_trending(top: int = 20) -> list[dict]:
    """Fetch trending symbols from StockTwits public API (no auth required).
    Returns [{ticker, st_bullish_pct, st_total_msgs, st_watchlist}]."""
    try:
        r = http.get(
            "https://api.stocktwits.com/api/2/trending/symbols.json",
            headers={"User-Agent": "trading-bot/1.0"},
            timeout=(5, 15),
        )
        r.raise_for_status()
        symbols = r.json().get("symbols", [])[:top]
        results = []
        for s in symbols:
            ticker = str(s.get("symbol") or "").upper()
            if not ticker or len(ticker) > 5:
                continue
            bull  = int((s.get("messages") or {}).get("bullish") or 0)
            bear  = int((s.get("messages") or {}).get("bearish") or 0)
            total = bull + bear
            bull_pct = round(bull / total * 100, 1) if total > 0 else 50.0
            results.append({
                "ticker":         ticker,
                "st_bullish_pct": bull_pct,
                "st_total_msgs":  total,
                "st_watchlist":   int(s.get("watchlist_count") or 0),
            })
        return results
    except Exception as e:
        print(f"  [stocktwits-warn] {type(e).__name__}: {e}")
        return []


def get_reddit_tickers(subreddits: list | None = None, limit: int = 50) -> dict:
    """Reddit blocks AWS Lambda IPs so this always returns empty.
    Kept as a stub so the caller doesn't need to change."""
    return {}


def get_broad_movers(top: int = 15) -> list[dict]:
    """Fetch last 2 daily bars for all SUGGESTION_SCAN_UNIVERSE tickers in one
    Alpaca call. Returns top N by absolute % change with volume confirmation."""
    try:
        end   = datetime.now(timezone.utc)
        start = end - timedelta(days=6)
        r = http.get(
            f"{ALPACA_DATA_BASE}/v2/stocks/bars",
            headers=alpaca_headers(),
            params={
                "symbols":    ",".join(SUGGESTION_SCAN_UNIVERSE),
                "timeframe":  "1Day",
                "start":      start.isoformat(),
                "end":        end.isoformat(),
                "feed":       "iex",
                "adjustment": "split",
                "limit":      1000,
            },
            timeout=(5, 30),
        )
        r.raise_for_status()
        all_bars = r.json().get("bars", {})
    except Exception as e:
        print(f"  [broad-movers-warn] {type(e).__name__}: {e}")
        return []

    results = []
    for ticker, bars in all_bars.items():
        if len(bars) < 2:
            continue
        bars.sort(key=lambda b: b.get("t", ""))
        prev, last = bars[-2], bars[-1]
        prev_c = float(prev.get("c") or 0)
        last_c = float(last.get("c") or 0)
        last_v = float(last.get("v") or 0)
        prev_v = float(prev.get("v") or 0)
        if prev_c <= 0 or last_c <= 0:
            continue
        pct       = round((last_c - prev_c) / prev_c * 100, 2)
        vol_ratio = round(last_v / prev_v, 2) if prev_v > 0 else 1.0
        results.append({
            "ticker":        ticker,
            "pct_change":    pct,
            "volume_ratio":  vol_ratio,
            "last_close":    last_c,
            "direction_hint": "bullish" if pct >= 0 else "bearish",
        })

    results.sort(key=lambda x: abs(x["pct_change"]), reverse=True)
    confirmed = [r for r in results if r["volume_ratio"] >= 1.5]
    others    = [r for r in results if r["volume_ratio"] < 1.5]
    return (confirmed + others)[:top]


def _merge_suggestion_candidates(
    st: list, reddit: dict, movers: list
) -> list:
    """Deduplicate and rank candidates from all three sources."""
    merged: dict = {}

    for i, item in enumerate(st):
        t = item["ticker"]
        merged[t] = {
            "ticker": t, "st_bullish_pct": item.get("st_bullish_pct", 50),
            "st_total_msgs": item.get("st_total_msgs", 0),
            "st_watchlist": item.get("st_watchlist", 0), "st_rank": i + 1,
            "reddit_mentions": 0, "pct_change": 0.0, "volume_ratio": 1.0,
            "direction_hint": "bullish" if item.get("st_bullish_pct", 50) >= 50 else "bearish",
            "sources": ["stocktwits"],
        }

    for ticker, count in sorted(reddit.items(), key=lambda x: -x[1])[:15]:
        if ticker in merged:
            merged[ticker]["reddit_mentions"] = count
            if "reddit" not in merged[ticker]["sources"]:
                merged[ticker]["sources"].append("reddit")
        else:
            merged[ticker] = {
                "ticker": ticker, "st_bullish_pct": 50, "st_total_msgs": 0,
                "st_watchlist": 0, "st_rank": 999, "reddit_mentions": count,
                "pct_change": 0.0, "volume_ratio": 1.0, "direction_hint": "neutral",
                "sources": ["reddit"],
            }

    for item in movers:
        t = item["ticker"]
        if t in merged:
            merged[t]["pct_change"]     = item["pct_change"]
            merged[t]["volume_ratio"]   = item["volume_ratio"]
            merged[t]["last_close"]     = item.get("last_close", 0)
            merged[t]["direction_hint"] = item["direction_hint"]
            if "movers" not in merged[t]["sources"]:
                merged[t]["sources"].append("movers")
        else:
            merged[t] = {
                "ticker": t, "st_bullish_pct": 50, "st_total_msgs": 0,
                "st_watchlist": 0, "st_rank": 999, "reddit_mentions": 0,
                "pct_change": item["pct_change"], "volume_ratio": item["volume_ratio"],
                "last_close": item.get("last_close", 0),
                "direction_hint": item["direction_hint"], "sources": ["movers"],
            }

    def _priority(c: dict) -> float:
        s  = len(c["sources"]) * 2.0
        s += min(abs(c.get("pct_change", 0)), 10) * 0.3
        s += min(c.get("volume_ratio", 1) - 1, 3) * 0.5
        s += min(c.get("reddit_mentions", 0), 50) * 0.05
        s += (1 - min(c.get("st_rank", 20), 20) / 20) * 1.0
        return s

    return sorted(merged.values(), key=_priority, reverse=True)


def _suggestion_ticker_context(ticker: str) -> dict | None:
    """Build technical + news summary for one candidate. Returns None if
    the ticker has no usable data or price is too low to be optionable.

    Indicators used:
      - 50-day SMA: primary trend direction (above = bullish, below = bearish)
      - MACD 12/26/9: momentum confirmation (histogram sign + slope)
      - EMA 8/21 cross: short-term trend filter
      - Volume ratio: confirms moves aren't low-volume noise
      - ATR-14: volatility context for sizing
      - 5-day % change: recent momentum
    """
    try:
        bars = get_daily_bars(ticker, limit=60)   # need 50+ bars for SMA50
        if not bars or len(bars) < 10:
            return None
        bars.sort(key=lambda b: b.get("t", ""))
        closes  = [float(b.get("c") or 0) for b in bars]
        volumes = [float(b.get("v") or 0) for b in bars]

        current_price = closes[-1]
        if current_price <= 5.0:
            return None

        # --- 50-day SMA (primary trend) ---
        sma50_val = round(sum(closes[-50:]) / 50, 2) if len(closes) >= 50 else None
        price_vs_sma50 = (
            "above" if sma50_val and current_price > sma50_val else
            "below" if sma50_val and current_price < sma50_val else "at"
        )

        # --- MACD 12/26/9 (momentum direction + slope) ---
        macd_line_val, sig_line_val, hist_val = macd(closes)
        # Histogram slope: compare last bar vs previous bar
        hist_prev = None
        if len(closes) >= 36:
            _, _, hist_prev = macd(closes[:-1])
        macd_hist_slope = (
            "rising"  if hist_val is not None and hist_prev is not None and hist_val > hist_prev else
            "falling" if hist_val is not None and hist_prev is not None and hist_val < hist_prev else
            "flat"
        )
        macd_direction = (
            "bullish" if hist_val is not None and hist_val > 0 else
            "bearish" if hist_val is not None and hist_val < 0 else "neutral"
        )

        # --- EMA 8/21 cross (short-term trend) ---
        ema8_val  = ema(closes, 8)  if len(closes) >= 8  else None
        ema21_val = ema(closes, 21) if len(closes) >= 21 else None
        ema_trend = (
            "bullish" if ema8_val and ema21_val and ema8_val > ema21_val else
            "bearish" if ema8_val and ema21_val and ema8_val < ema21_val else "neutral"
        )

        # --- Volume + momentum ---
        avg_vol5  = sum(volumes[-5:]) / 5  if len(volumes) >= 5 else 0
        vol_ratio = round(volumes[-1] / avg_vol5, 2) if avg_vol5 > 0 else 1.0
        pct_5d    = round((closes[-1] - closes[-6]) / closes[-6] * 100, 2) if len(closes) >= 6 else 0.0
        atr_val   = atr_14(bars) if len(bars) >= 15 else None

        # --- News (Finnhub, last 48h) ---
        news = []
        try:
            fh_key = get_secret("/trading-bot/finnhub-key")
            raw    = fetch_finnhub_news(ticker, fh_key, days_back=2, max_items=5)
            news   = [n.get("headline", "") for n in raw if n.get("headline")][:5]
        except Exception:
            pass

        # --- Earnings check ---
        earn_map  = get_earnings_dates()
        earn_date = earn_map.get(ticker)
        days_earn = None
        if earn_date:
            try:
                days_earn = (date.fromisoformat(str(earn_date)) - date.today()).days
            except Exception:
                pass

        # --- Live quote: always use the freshest available price ---
        # Daily bar closes can be hours stale by 8:15am pre-market.
        # get_latest_quote() returns pre-market bid/ask when available,
        # otherwise falls back to the prior close — always fresher than bars.
        live_price = current_price  # fallback to last bar close
        try:
            q = get_latest_quote(ticker)
            bid = float(q.get("bp") or 0)
            ask = float(q.get("ap") or 0)
            if bid > 0 and ask > 0:
                live_price = round((bid + ask) / 2, 2)
        except Exception:
            pass

        # Recompute SMA comparison using live price
        live_vs_sma50 = (
            "above" if sma50_val and live_price > sma50_val else
            "below" if sma50_val and live_price < sma50_val else "at"
        )

        return {
            "current_price":    live_price,
            "sma50":            sma50_val,
            "price_vs_sma50":   live_vs_sma50,
            "macd_histogram":   hist_val,
            "macd_hist_slope":  macd_hist_slope,
            "macd_direction":   macd_direction,
            "ema_trend":        ema_trend,
            "vol_ratio":        vol_ratio,
            "pct_5d":           pct_5d,
            "atr":              round(atr_val, 2) if atr_val else None,
            "news_headlines":   news,
            "days_to_earnings": days_earn,
        }
    except Exception as e:
        print(f"  [ctx-warn] {ticker}: {type(e).__name__}: {e}")
        return None


def build_suggestion_contract(ticker: str, direction: str,
                               budget: float = SUGGESTION_BUDGET) -> dict | None:
    """Find the best-fitting naked call (bullish) or put (bearish) within budget.
    Filters by delta band, DTE band, bid-ask sanity. Prefers delta ~0.42."""
    today          = date.today()
    gte            = (today + timedelta(days=SUGGESTION_DTE_MIN)).isoformat()
    lte            = (today + timedelta(days=SUGGESTION_DTE_MAX)).isoformat()
    contract_type  = "call" if direction == "bullish" else "put"
    max_prem       = budget / 100  # e.g. $5.00 for $500 budget

    contracts = get_options_contracts(ticker, gte, lte, contract_type=contract_type)
    if not contracts:
        return None

    snaps = get_option_snapshots([c["symbol"] for c in contracts])
    best  = None

    for c in contracts:
        snap   = snaps.get(c["symbol"], {}) or {}
        greeks = snap.get("greeks") or {}
        quote  = snap.get("latestQuote") or {}
        delta  = greeks.get("delta")
        iv     = greeks.get("iv") or greeks.get("impliedVolatility")
        bid    = float(quote.get("bp") or 0)
        ask    = float(quote.get("ap") or 0)

        if delta is None or bid <= 0 or ask <= 0:
            continue
        abs_delta = abs(float(delta))
        if not (SUGGESTION_DELTA_MIN <= abs_delta <= SUGGESTION_DELTA_MAX):
            continue

        mid        = (bid + ask) / 2
        spread_pct = (ask - bid) / mid if mid > 0 else 1.0
        if spread_pct > SUGGESTION_BIDASK_MAX_PCT:
            continue
        if mid > max_prem:
            continue  # too expensive for even 1 contract

        dte          = (date.fromisoformat(c["expiration_date"]) - today).days
        qty          = max(1, int(budget // (mid * 100)))
        total        = round(qty * mid * 100, 2)
        target_price = round(mid * (1 + SUGGESTION_TARGET_PCT), 2)
        stop_price   = round(mid * (1 - SUGGESTION_STOP_PCT), 2)
        target_gain  = round((target_price - mid) * 100 * qty, 2)
        stop_loss    = round((mid - stop_price) * 100 * qty, 2)
        rr           = round(target_gain / stop_loss, 2) if stop_loss > 0 else 0.0
        strike       = float(c["strike_price"])
        breakeven    = round(strike + mid, 2) if direction == "bullish" else round(strike - mid, 2)

        candidate = {
            "symbol":       c["symbol"],
            "strike":       strike,
            "expiry":       c["expiration_date"],
            "dte":          dte,
            "delta":        round(abs_delta, 3),
            "iv":           round(float(iv), 3) if iv else None,
            "bid":          bid,
            "ask":          ask,
            "premium_est":  round(mid, 2),
            "spread_pct":   round(spread_pct * 100, 1),
            "qty":          qty,
            "total_cost":   total,
            "target_price": target_price,
            "stop_price":   stop_price,
            "target_gain":  target_gain,
            "stop_loss":    stop_loss,
            "rr_ratio":     rr,
            "breakeven":    breakeven,
            "_dscore":      -abs(abs_delta - 0.42),  # prefer delta ~0.42
        }
        if best is None or candidate["_dscore"] > best["_dscore"]:
            best = candidate

    if best:
        best.pop("_dscore", None)
    return best


SYSTEM_SUGGESTION_ANALYST = """You are a professional options trader ranking naked option candidates for a $500 budget. Goal: find the best calls/puts with the highest probability of a 20%+ gain.

Technical indicators to evaluate:
- Price vs 50-day SMA: primary trend. Below SMA50 = structural bearish, above = bullish.
- MACD histogram: positive/rising = bullish momentum accelerating. Negative/falling = bearish.
- EMA 8/21 cross: short-term trend confirmation.
- Volume ratio >1.5x = institutional participation behind the move.
- 5-day % change: recent price momentum direction.

Evaluation weights: technicals 30% | catalyst/news 25% | risk/reward (DTE, delta, IV) 25% | social sentiment 10% | regime 10%.

MARKET DIRECTION ASYMMETRY — this is critical:
Red market days (SPY down >1% intraday) strongly favor PUTS:
- Fear and selling pressure cascade. Stocks rarely reverse sharply once broad selling starts.
- Bearish setups on red days: award +1.5 points to the score. Lower bar to qualify.
- Bullish setups AGAINST the market: deduct 1 point. Need exceptional individual catalyst to overcome.
- On red days, a put with score 5.5+ is worth taking. A call needs 7.5+ (must be bucking market with very strong specific catalyst).

Green market days (SPY up >1%) are CHOPPIER — profit-taking creates resistance:
- Bullish setups: normal scoring applies.
- Bearish setups: need stronger conviction (deduct 0.5 pt) since trend is against you.
- On green days, a call with score 6.0+ qualifies. A put needs 7.0+ (needs a clear breakdown catalyst).

Flat market (SPY within ±1%): score both directions equally. Best setup wins regardless of direction.

Scoring guide — use the full range AFTER applying market direction adjustment:
- 9-10: Exceptional. Multiple strong signals aligning, clear catalyst, high conviction.
- 7-8: Strong. Most signals agree, good setup, worth trading.
- 5-6: Moderate. Mixed signals or weak catalyst. Marginal.
- 3-4: Weak. Conflicting signals, no clear edge.
- 1-2: Avoid. No setup at all.

Rules:
- Score every candidate objectively with direction adjustment applied.
- Rank your top 3 candidates. Return action=PASS only if ALL candidates score below 4.
- Data may be pre-market or prior close. Evaluate as a forward-looking 1-5 day trade.
- On strongly red days: actively look for the BEST PUT setup, not just the least-bad call.
- Skip a candidate only for: earnings within 3 days, IV > 150%, or genuinely no setup.

Return compact JSON — top 3 picks ranked by score:
{"action":"PICK","picks":[{"rank":1,"ticker":"NVDA","direction":"bearish","score":8.2,"score_breakdown":{"technicals":9,"catalyst":8,"social":7,"risk_reward":8,"regime":9},"rationale":"<2 sentences, specific setup>","key_risks":"<1 sentence>","contract_symbol":"NVDA260603P00215000"},{"rank":2,"ticker":"SMCI","direction":"bearish","score":6.4,"rationale":"<1 sentence>","contract_symbol":"SMCI260529P00032500"},{"rank":3,"ticker":"NKE","direction":"bearish","score":5.8,"rationale":"<1 sentence>","contract_symbol":"NKE260605P00043500"}]}"""


def claude_suggestion_scan(candidates: list, regime: dict, vix: float | None,
                           spy_move: float | None = None) -> dict | None:
    """Score all candidates. Returns the top pick (rank 1) enriched with contract
    data, plus a 'runners_up' list of ranks 2-3. Returns None if action=PASS or
    the top score is below the dynamic threshold."""
    if not candidates:
        return None

    spy_str = f"{spy_move:+.2f}%" if spy_move is not None else "n/a"
    if spy_move is not None and spy_move <= -1.5:
        market_context = f"RED DAY - SPY {spy_str} intraday. Strong bearish bias. Puts favored."
    elif spy_move is not None and spy_move <= -0.8:
        market_context = f"SOFT RED - SPY {spy_str} intraday. Lean bearish."
    elif spy_move is not None and spy_move >= 1.5:
        market_context = f"GREEN DAY - SPY {spy_str} intraday. Bullish momentum but watch profit-taking."
    elif spy_move is not None and spy_move >= 0.8:
        market_context = f"SOFT GREEN - SPY {spy_str} intraday. Lean bullish."
    else:
        market_context = f"FLAT - SPY {spy_str} intraday. Score both directions equally."

    lines = [
        f"DATE: {date.today().isoformat()}",
        f"VIX: {vix if vix is not None else 'n/a'}",
        f"REGIME: {regime.get('regime','?')} bias={regime.get('bias','?')} trades_allowed={regime.get('new_trades_allowed',True)}",
        f"MARKET TODAY: {market_context}",
        f"BUDGET: ${SUGGESTION_BUDGET:.0f}",
        "",
    ]
    for c in candidates:
        cont = c.get("contract") or {}
        lines.append(f"=== {c['ticker']} | {c.get('direction_hint','?').upper()} ===")
        lines.append(
            f"Price ${c.get('current_price','?')} | SMA50 {c.get('price_vs_sma50','?')} "
            f"(SMA50=${c.get('sma50','?')}) | EMA8/21 {c.get('ema_trend','?')}"
        )
        lines.append(
            f"MACD hist={c.get('macd_histogram','?')} ({c.get('macd_hist_slope','?')}) "
            f"| 5d {c.get('pct_5d',0):+.1f}% | Vol {c.get('vol_ratio','?')}x"
        )
        lines.append(
            f"Social: StockTwits {c.get('st_bullish_pct',50):.0f}% bull "
            f"({c.get('st_total_msgs',0)} msgs)"
        )
        earn = c.get("days_to_earnings")
        if earn is not None:
            lines.append(f"Earnings in {earn} days")
        for h in (c.get("news_headlines") or [])[:3]:
            lines.append(f"  News: {h[:120]}")
        if cont:
            lines.append(
                f"Contract: {cont.get('symbol')} K={cont.get('strike')} exp={cont.get('expiry')} "
                f"DTE={cont.get('dte')} delta={cont.get('delta')} IV={cont.get('iv','?')} "
                f"premium=${cont.get('premium_est')} x{cont.get('qty')}c = ${cont.get('total_cost'):.0f} "
                f"target=${cont.get('target_price')} stop=${cont.get('stop_price')} R:R={cont.get('rr_ratio')}"
            )
        lines.append("")

    try:
        client = _anthropic_client()
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1200,
            system=SYSTEM_SUGGESTION_ANALYST,
            messages=[
                {"role": "user",      "content": "\n".join(lines)},
                {"role": "assistant", "content": "{"},
            ],
        )
        raw    = "{" + (msg.content[0].text if msg.content else "")
        result = _parse_json(raw, fallback={"action": "PASS", "picks": []})
        result.setdefault("action", "PASS")

        if result.get("action") != "PICK":
            print("  [claude-suggestion] returned PASS")
            return None

        picks_raw = result.get("picks") or []
        if not picks_raw:
            print("  [claude-suggestion] no picks in response")
            return None

        # Sort by rank to be safe
        picks_raw.sort(key=lambda p: p.get("rank", 99))

        def _enrich(p: dict) -> dict:
            """Attach contract + technical data from the candidates list."""
            ticker = (p.get("ticker") or "").upper()
            match  = next((c for c in candidates if c["ticker"] == ticker), None)
            if match:
                p.update({
                    "contract":         match.get("contract"),
                    "st_bullish_pct":   match.get("st_bullish_pct", 50),
                    "current_price":    match.get("current_price"),
                    "sma50":            match.get("sma50"),
                    "price_vs_sma50":   match.get("price_vs_sma50"),
                    "macd_histogram":   match.get("macd_histogram"),
                    "macd_hist_slope":  match.get("macd_hist_slope"),
                    "macd_direction":   match.get("macd_direction"),
                    "ema_trend":        match.get("ema_trend"),
                    "vol_ratio":        match.get("vol_ratio"),
                    "pct_5d":           match.get("pct_5d"),
                    "news_headlines":   match.get("news_headlines", []),
                    "days_to_earnings": match.get("days_to_earnings"),
                })
            return p

        top     = _enrich(picks_raw[0])
        runners = [_enrich(p) for p in picks_raw[1:3]]

        top_score  = float(top.get("score") or 0)
        top_dir    = (top.get("direction") or "bullish").lower()

        # Dynamic threshold: red days lower the bar for puts, raise it for calls.
        # Green days are choppier — calls need normal conviction, puts need more.
        if spy_move is not None and spy_move <= -1.5:
            min_score = 5.5 if top_dir == "bearish" else 7.5
        elif spy_move is not None and spy_move >= 1.5:
            min_score = SUGGESTION_SCORE_MIN if top_dir == "bullish" else 7.0
        else:
            min_score = SUGGESTION_SCORE_MIN

        print(f"  [claude-suggestion] top={top.get('ticker')} dir={top_dir} score={top_score} "
              f"min_required={min_score} spy={spy_move} runners={[p.get('ticker') for p in runners]}")

        if top_score < min_score:
            print(f"  [claude-suggestion] top score {top_score} below dynamic min {min_score} -> PASS")
            return None

        top["runners_up"] = runners
        return top

    except Exception as e:
        print(f"  [claude-suggestion-fail] {type(e).__name__}: {e}")
        return None


def save_daily_suggestion(pick: dict) -> None:
    # Keep only the fields needed by recap + P&L check to stay under SSM 4096-char limit.
    # runners_up and news_headlines are large and only used at scan time (already sent to Telegram).
    cont = pick.get("contract") or {}
    slim = {
        "ticker":         pick.get("ticker"),
        "direction":      pick.get("direction"),
        "score":          pick.get("score"),
        "rationale":      (pick.get("rationale") or "")[:200],
        "key_risks":      (pick.get("key_risks") or "")[:120],
        "current_price":  pick.get("current_price"),
        "sma50":          pick.get("sma50"),
        "price_vs_sma50": pick.get("price_vs_sma50"),
        "macd_histogram": pick.get("macd_histogram"),
        "macd_direction": pick.get("macd_direction"),
        "macd_hist_slope":pick.get("macd_hist_slope"),
        "ema_trend":      pick.get("ema_trend"),
        "vol_ratio":      pick.get("vol_ratio"),
        "pct_5d":         pick.get("pct_5d"),
        "st_bullish_pct": pick.get("st_bullish_pct"),
        "days_to_earnings":pick.get("days_to_earnings"),
        "contract": {
            "symbol":       cont.get("symbol"),
            "strike":       cont.get("strike"),
            "expiry":       cont.get("expiry"),
            "dte":          cont.get("dte"),
            "delta":        cont.get("delta"),
            "iv":           cont.get("iv"),
            "premium_est":  cont.get("premium_est"),
            "qty":          cont.get("qty"),
            "total_cost":   cont.get("total_cost"),
            "target_price": cont.get("target_price"),
            "stop_price":   cont.get("stop_price"),
            "target_gain":  cont.get("target_gain"),
            "stop_loss":    cont.get("stop_loss"),
            "rr_ratio":     cont.get("rr_ratio"),
            "breakeven":    cont.get("breakeven"),
        },
        "saved_at": date.today().isoformat(),
    }
    payload = slim
    try:
        ssm.put_parameter(
            Name="/trading-bot/daily-suggestion",
            Value=json.dumps(payload, default=str),
            Type="String", Overwrite=True,
        )
        ssm.put_parameter(
            Name="/trading-bot/daily-suggestion-alerts",
            Value=json.dumps({"date": date.today().isoformat(), "sent": []}),
            Type="String", Overwrite=True,
        )
    except Exception as e:
        print(f"  [save-suggestion-warn] {type(e).__name__}: {e}")


def load_daily_suggestion() -> dict | None:
    try:
        resp = ssm.get_parameter(Name="/trading-bot/daily-suggestion", WithDecryption=False)
        data = json.loads(resp["Parameter"]["Value"])
        if data.get("saved_at") == date.today().isoformat():
            return data
    except Exception:
        pass
    return None


def _format_suggestion_telegram(pick: dict) -> str:
    """Build the HTML Telegram message for the morning suggestion."""
    cont      = pick.get("contract") or {}
    ticker    = pick.get("ticker", "?")
    direction = pick.get("direction", "bullish")
    score     = float(pick.get("score") or 0)
    today_str = date.today().strftime("%b %d, %Y")

    cp    = "C" if direction == "bullish" else "P"
    flag  = "+" if direction == "bullish" else "-"

    prem   = cont.get("premium_est", 0)
    qty    = cont.get("qty", 1)
    total  = cont.get("total_cost", 0)
    target = cont.get("target_price", 0)
    stop   = cont.get("stop_price", 0)
    tgain  = cont.get("target_gain", 0)
    tloss  = cont.get("stop_loss", 0)
    rr     = cont.get("rr_ratio", 0)
    sym    = cont.get("symbol", "")
    expiry = cont.get("expiry", "")
    dte    = cont.get("dte", 0)
    delta  = cont.get("delta", 0)
    iv_raw = cont.get("iv")
    iv_pct = f"{round(float(iv_raw) * 100, 1)}%" if iv_raw else "n/a"
    be     = cont.get("breakeven", 0)

    # Human-readable headline: IONQ $67C May 29 EXP
    strike_val = float(cont.get("strike") or 0)
    strike_fmt = str(int(strike_val)) if strike_val == int(strike_val) else str(strike_val)
    try:
        expiry_dt   = datetime.strptime(expiry, "%Y-%m-%d")
        expiry_short = f"{expiry_dt.strftime('%b')} {expiry_dt.day}"
    except Exception:
        expiry_short = expiry
    play_headline = f"{ticker} ${strike_fmt}{cp} {expiry_short} EXP"

    current_px  = pick.get("current_price")
    sma50       = pick.get("sma50")
    vs_sma      = pick.get("price_vs_sma50", "n/a")
    macd_hist   = pick.get("macd_histogram")
    macd_slope  = pick.get("macd_hist_slope", "n/a")
    ema_trend   = pick.get("ema_trend", "n/a")
    vol_ratio   = pick.get("vol_ratio", "n/a")
    st_bull     = pick.get("st_bullish_pct", 50)
    rationale   = pick.get("rationale", "")
    risks       = pick.get("key_risks", "")
    earn_days   = pick.get("days_to_earnings")
    earn_str    = f"Earnings {earn_days}d away" if earn_days is not None else "No near-term earnings"
    pct_5d      = pick.get("pct_5d", 0) or 0

    sb     = pick.get("score_breakdown") or {}
    sb_str = " | ".join(f"{k}:{v}" for k, v in sb.items()) if sb else ""

    # SMA + MACD summary line
    sma_str  = f"${current_px} {'above' if vs_sma == 'above' else 'below'} SMA50 ${sma50}" if sma50 else "SMA50 n/a"
    macd_str = f"MACD hist {macd_hist:+.4f} ({macd_slope})" if macd_hist is not None else "MACD n/a"
    macd_bias = "bullish" if macd_hist and macd_hist > 0 else "bearish" if macd_hist and macd_hist < 0 else "neutral"

    lines = [
        f"<b>OPTIONS PLAY - {today_str}</b>",
        f"<i>Scanned at {datetime.now(ET).strftime('%H:%M ET')}</i>",
        "",
        f"<b>{play_headline}</b>",
        f"<code>{sym}</code>",
        f"Delta {delta:.2f} | IV {iv_pct} | {dte} DTE",
        "",
        "<b>TRADE SETUP ($500 budget)</b>",
        f"Stock price now: <b>${current_px}</b>",
        f"Entry:       <b>${prem:.2f}</b> x {qty}c = <b>${total:.0f}</b>",
        f"Target +20%: ${target:.2f}  (+${tgain:.0f})",
        f"Stop  -20%:  ${stop:.2f}  (-${tloss:.0f})",
        f"Break-even:  ${be:.2f}",
    ]
    if rr:
        lines.append(f"R:R = {rr:.1f}")
    lines += [
        "",
        f"<b>Score: {score:.1f}/10</b>",
    ]
    if sb_str:
        lines.append(f"<i>({sb_str})</i>")
    if rationale:
        lines.append(f"<i>{rationale}</i>")
    lines += [
        "",
        "<b>Technicals:</b>",
        f"Trend:  {sma_str}",
        f"MACD:   {macd_str} -> {macd_bias}",
        f"EMA:    8/21 cross {ema_trend} | 5d {pct_5d:+.1f}% | Vol {vol_ratio}x",
        f"Social: StockTwits {st_bull:.0f}% bullish",
        f"Earn:   {earn_str}",
    ]
    if risks:
        lines.append(f"Risk:   {risks}")

    # --- Runners-up (ranks 2 and 3) -----------------------------------------
    runners = pick.get("runners_up") or []
    if runners:
        lines.append("")
        lines.append("<b>Also considered:</b>")
        for ru in runners[:2]:
            ru_rank  = ru.get("rank", "?")
            ru_tick  = ru.get("ticker", "?")
            ru_dir   = "CALL" if ru.get("direction") == "bullish" else "PUT"
            ru_score = float(ru.get("score") or 0)
            ru_note  = ru.get("rationale") or ""
            ru_sma   = ru.get("price_vs_sma50", "")
            ru_macd  = ru.get("macd_direction", "")
            ru_px    = ru.get("current_price")
            # Build a concise one-liner: ticker direction score | quick context
            tech_note = ""
            if ru_sma and ru_macd:
                tech_note = f"{ru_sma.upper()} SMA50, MACD {ru_macd}"
            elif ru_sma:
                tech_note = f"{ru_sma.upper()} SMA50"
            # Use Claude's rationale if short, otherwise fall back to technicals
            detail = ru_note[:80] if ru_note else tech_note
            px_str = f" ${ru_px}" if ru_px else ""
            lines.append(
                f"#{ru_rank} <b>{ru_tick}</b> {ru_dir}{px_str} "
                f"score {ru_score:.1f} - <i>{detail}</i>"
            )

    lines += [
        "",
        f"<i>Max risk ${total:.0f}. Not financial advice.</i>",
    ]
    return "\n".join(lines)


def run_daily_suggestion(force: bool = False) -> dict:
    """09:45 ET weekdays. Discover trending tickers, build technical context,
    score with Claude, pick one high-conviction play, notify via Telegram + email."""
    # Skip on holidays/weekends — this mode bypasses the global market gate so
    # we guard explicitly here. force=True lets manual test invokes still run.
    if not force and not market_is_open():
        print("[daily-suggestion] market closed today (holiday/weekend) - skipping")
        return {"status": "skipped", "mode": "daily_suggestion", "reason": "market_closed"}

    print("[daily-suggestion] starting morning scan")
    set_active_strategy(STRATEGY_SUGGESTION)

    regime  = get_active_regime()
    vix     = get_vix()
    spy_move = _get_spy_intraday_move()
    print(f"  regime={regime.get('regime','?')} vix={vix} spy_intraday={spy_move}")

    # Determine market bias from live SPY move
    if spy_move is not None and spy_move <= -1.5:
        market_bias = "bearish"
        print(f"  RED DAY (SPY {spy_move:+.1f}%) -> bearish bias, puts favored")
    elif spy_move is not None and spy_move >= 1.5:
        market_bias = "bullish"
        print(f"  GREEN DAY (SPY {spy_move:+.1f}%) -> bullish bias")
    else:
        market_bias = "neutral"

    print("  fetching StockTwits trending")
    st_data = get_stocktwits_trending(top=20)

    print("  fetching Reddit mentions")
    reddit_data = get_reddit_tickers(["wallstreetbets", "options"], limit=50)

    print("  fetching broad movers")
    movers_data = get_broad_movers(top=15)

    candidates = _merge_suggestion_candidates(st_data, reddit_data, movers_data)

    # On red days: inject SPY and QQQ as forced bearish candidates — they are
    # always liquid and the most direct expression of a broad market sell-off.
    if market_bias == "bearish":
        forced_bearish = ["SPY", "QQQ"]
        existing = {c["ticker"] for c in candidates}
        for fb in forced_bearish:
            if fb not in existing:
                candidates.insert(0, {
                    "ticker": fb, "st_bullish_pct": 30, "st_total_msgs": 0,
                    "st_watchlist": 0, "st_rank": 1, "reddit_mentions": 0,
                    "pct_change": spy_move or -1.5, "volume_ratio": 1.5,
                    "direction_hint": "bearish", "sources": ["market_bias"],
                })
        print(f"  injected bearish candidates: {forced_bearish}")

    print(f"  merged {len(candidates)} candidates")

    contexts: list = []
    for cand in candidates[:16]:
        ticker = cand["ticker"]
        ctx    = _suggestion_ticker_context(ticker)
        if not ctx:
            print(f"  [skip] {ticker}: no data or price too low")
            continue
        earn_days = ctx.get("days_to_earnings")
        if earn_days is not None and 0 <= earn_days <= 2:
            print(f"  [skip] {ticker}: earnings in {earn_days}d")
            continue

        # Direction logic: SMA50 is the structural anchor, but override with
        # market bias when the broad market is clearly moving one direction.
        sma_dir  = "bullish" if ctx.get("price_vs_sma50") == "above" else "bearish"
        macd_dir = ctx.get("macd_direction", "neutral")
        ema_dir  = ctx.get("ema_trend", "neutral")

        if market_bias == "bearish":
            # On red days: if the stock shows ANY weakness, go bearish.
            # Only keep bullish if stock is genuinely bucking the market
            # (strong positive MACD AND EMA bullish AND positive 5d).
            stock_bucking = (macd_dir == "bullish" and ema_dir == "bullish"
                             and float(ctx.get("pct_5d") or 0) > 1.0)
            tech_dir = "bullish" if stock_bucking else "bearish"
        elif market_bias == "bullish":
            # On green days: trust SMA50 but don't fight clear bearish breakdowns
            stock_breaking_down = (macd_dir == "bearish" and ema_dir == "bearish"
                                   and float(ctx.get("pct_5d") or 0) < -2.0)
            tech_dir = "bearish" if stock_breaking_down else sma_dir
        else:
            tech_dir = sma_dir  # neutral: use SMA50 as normal

        combined = {**cand, **ctx, "direction_hint": tech_dir,
                    "market_bias": market_bias, "spy_move": spy_move}
        combined["direction_agreement"] = (tech_dir == macd_dir or macd_dir == "neutral")
        contexts.append(combined)

    if not contexts:
        msg = "<b>Daily Options Scan</b>\n\nNo optionable candidates found today."
        send_telegram(msg)
        return {"status": "ok", "mode": "daily_suggestion", "picks": 0, "reason": "no_candidates"}

    valid: list = []
    for ctx in contexts:
        contract = build_suggestion_contract(ctx["ticker"], ctx.get("direction_hint", "bullish"))
        if not contract:
            print(f"  [skip] {ctx['ticker']}: no contract in budget")
            continue
        ctx["contract"] = contract
        valid.append(ctx)
        print(f"  {ctx['ticker']} {ctx.get('direction_hint')} -> {contract['symbol']} ${contract['premium_est']} x{contract['qty']}c")

    if not valid:
        msg = "<b>Daily Options Scan</b>\n\nNo suitable contracts found within $500 budget today."
        send_telegram(msg)
        return {"status": "ok", "mode": "daily_suggestion", "picks": 0, "reason": "no_contracts"}

    print(f"  scoring {len(valid)} candidates with Claude (market_bias={market_bias})")
    pick = claude_suggestion_scan(valid, regime, vix, spy_move=spy_move)

    if not pick:
        bias_note = f" (SPY {spy_move:+.1f}% — {market_bias} day)" if spy_move is not None else ""
        msg = f"<b>Daily Options Scan</b>\n\nNo high-conviction play today{bias_note}. Standing aside."
        send_telegram(msg)
        send_alert("Daily Options Scan - No Play Today", "No high-conviction play found. Standing aside.")
        return {"status": "ok", "mode": "daily_suggestion", "picks": 0, "reason": "below_min_score"}

    print(f"  PICK: {pick.get('ticker')} score={pick.get('score')}")
    save_daily_suggestion(pick)

    tg_msg = _format_suggestion_telegram(pick)
    send_telegram(tg_msg)

    # SNS email: strip HTML tags for plain-text body
    plain = re.sub(r"<[^>]+>", "", tg_msg)
    cont  = pick.get("contract") or {}
    send_alert(
        f"[Daily Play] {pick.get('ticker')} {(pick.get('direction') or '').upper()} "
        f"{cont.get('symbol','')} score={pick.get('score',0)}",
        plain,
    )

    # ---- Auto-execute on the Weeklies (STRAT3) account ----
    # The suggestion pick is already high-conviction and has a valid contract.
    # Re-use the contract found above — no second API call needed.
    # The Weeklies exit sweep, stop-loss, and alerts pick it up automatically.
    set_active_strategy(STRATEGY_NAKED)
    try:
        ticker_up    = (pick.get("ticker") or "").upper()
        naked_budget = compute_naked_budget()
        if naked_budget["slots"] <= 0:
            print(f"  [suggestion-trade] no Weeklies slots ({naked_budget['open']}/{NAKED_MAX_POSITIONS}) - sent to Telegram only")
        elif ticker_up in naked_budget["open_tickers"]:
            print(f"  [suggestion-trade] {ticker_up} already open in Weeklies - sent to Telegram only")
        elif not cont.get("symbol") or not cont.get("premium_est"):
            print(f"  [suggestion-trade] no valid contract in pick - sent to Telegram only")
        else:
            # Translate suggestion contract format -> naked contract format
            naked_contract = {
                "symbol":    cont["symbol"],
                "strike":    cont.get("strike"),
                "expiry":    cont.get("expiry"),
                "dte":       cont.get("dte"),
                "delta":     cont.get("delta"),
                "iv":        cont.get("iv"),
                "mid":       cont.get("premium_est"),   # field name differs
                "breakeven": cont.get("breakeven"),
            }
            current_px = float(pick.get("current_price") or 0)
            if current_px <= 0:
                try:
                    q = get_latest_quote(ticker_up)
                    current_px = (float(q.get("bp") or 0) + float(q.get("ap") or 0)) / 2
                except Exception:
                    pass
            naked_decision = {
                "ticker":               ticker_up,
                "direction":            pick.get("direction", "bullish"),
                "score":                pick.get("score", 7.0),
                "catalyst_description": "Daily Suggestion auto-trade",
                "rationale":            (pick.get("rationale") or "")[:200],
            }
            # Live market check before executing — don't auto-buy calls on a red market
            spy_chg = _get_spy_intraday_move()
            trade_dir = (pick.get("direction") or "bullish").lower()
            if spy_chg is not None and spy_chg <= -3.0:
                print(f"  [suggestion-trade] SPY down {spy_chg:.1f}% - skipping auto-trade (too risky)")
                set_active_strategy(STRATEGY_SUGGESTION)
                return {
                    "status": "ok", "mode": "daily_suggestion",
                    "pick": pick.get("ticker"), "score": pick.get("score"),
                    "contract": cont.get("symbol"),
                    "auto_trade": "skipped - market down too much",
                }
            if spy_chg is not None and spy_chg <= -1.5 and trade_dir == "bullish":
                print(f"  [suggestion-trade] SPY down {spy_chg:.1f}% - skipping bullish auto-trade")
                set_active_strategy(STRATEGY_SUGGESTION)
                return {
                    "status": "ok", "mode": "daily_suggestion",
                    "pick": pick.get("ticker"), "score": pick.get("score"),
                    "contract": cont.get("symbol"),
                    "auto_trade": "skipped - bullish trade blocked on red market",
                }
            print(f"  [suggestion-trade] executing {ticker_up} {pick.get('direction')} on Weeklies account")
            spread = _execute_naked_open(naked_decision, naked_contract, current_px, None)
            if spread:
                print(f"  [suggestion-trade] opened {spread['id']} on Weeklies (STRAT3)")
            else:
                print(f"  [suggestion-trade] execute returned None (no fill, over budget, or order rejected)")
    except Exception as e:
        print(f"  [suggestion-trade-error] {type(e).__name__}: {e}")

    return {
        "status": "ok", "mode": "daily_suggestion",
        "pick": pick.get("ticker"), "score": pick.get("score"),
        "contract": cont.get("symbol"),
    }


def run_suggestion_recap() -> dict:
    """12:30 ET. Always-send midday recap: shows where the underlying moved,
    where the option is now, whether the thesis still holds, and key levels."""
    print("[suggestion-recap] midday recap")
    set_active_strategy(STRATEGY_SUGGESTION)

    pick = load_daily_suggestion()
    if not pick:
        print("  no suggestion for today")
        return {"status": "ok", "mode": "suggestion_recap", "reason": "no_suggestion"}

    cont        = pick.get("contract") or {}
    symbol      = cont.get("symbol", "")
    ticker      = pick.get("ticker", "?")
    direction   = pick.get("direction", "bullish")
    entry_prem  = float(cont.get("premium_est") or 0)
    entry_px    = float(pick.get("current_price") or 0)   # underlying at scan time
    qty         = int(cont.get("qty") or 1)
    target_prem = float(cont.get("target_price") or 0)
    stop_prem   = float(cont.get("stop_price") or 0)

    # Current option price
    current_opt = None
    if symbol:
        snaps = get_option_snapshots([symbol])
        snap  = snaps.get(symbol, {}) or {}
        quote = snap.get("latestQuote") or {}
        bid   = float(quote.get("bp") or 0)
        ask   = float(quote.get("ap") or 0)
        if bid > 0 and ask > 0:
            current_opt = round((bid + ask) / 2, 2)

    # Current underlying price
    current_px = None
    try:
        q = get_latest_quote(ticker)
        b = float(q.get("bp") or 0)
        a = float(q.get("ap") or 0)
        if b > 0 and a > 0:
            current_px = round((b + a) / 2, 2)
    except Exception:
        pass

    now_str = datetime.now(ET).strftime("%H:%M ET")
    today_str = date.today().strftime("%b %d, %Y")

    # P&L calculation
    pnl_pct = round((current_opt - entry_prem) / entry_prem * 100, 1) if current_opt and entry_prem > 0 else None
    pnl_dlr = round((current_opt - entry_prem) * 100 * qty, 2) if current_opt and entry_prem > 0 else None

    # Underlying move
    underlying_pct = round((current_px - entry_px) / entry_px * 100, 2) if current_px and entry_px > 0 else None

    # Thesis status
    if pnl_pct is None:
        thesis_line = "No live quote available"
    elif pnl_pct >= 20:
        thesis_line = "TARGET REACHED (+20%) - consider taking profits"
    elif pnl_pct >= 10:
        thesis_line = "Working well - up 10%+, watch for continuation"
    elif pnl_pct >= -10:
        thesis_line = "In range - thesis intact, give it room"
    elif pnl_pct >= -20:
        thesis_line = "Approaching stop level - monitor closely"
    else:
        thesis_line = "Stop level breached - consider closing"

    dir_word = "above" if direction == "bullish" else "below"
    dir_emoji = "UP" if direction == "bullish" else "DOWN"

    lines = [
        f"<b>MIDDAY RECAP - {today_str}</b>",
        f"<i>{now_str}</i>",
        "",
        f"<b>Morning play: {ticker} {dir_emoji}</b>",
        f"<code>{symbol}</code>",
        "",
        "<b>UNDERLYING</b>",
    ]
    if entry_px:
        lines.append(f"At scan:  ${entry_px:.2f}")
    if current_px is not None:
        arrow = "+" if (underlying_pct or 0) >= 0 else ""
        lines.append(f"Now:      <b>${current_px:.2f}</b> ({arrow}{underlying_pct:.1f}%)")
        # Is the stock moving in your favor?
        favor = (direction == "bullish" and (underlying_pct or 0) > 0) or \
                (direction == "bearish" and (underlying_pct or 0) < 0)
        lines.append(f"Moving:   {'in your favor' if favor else 'against you'}")

    lines += ["", "<b>OPTION</b>"]
    lines.append(f"Entry:    ${entry_prem:.2f} x {qty}c = ${entry_prem * qty * 100:.0f}")
    if current_opt is not None:
        pnl_arrow = "+" if (pnl_pct or 0) >= 0 else ""
        lines.append(f"Now:      <b>${current_opt:.2f}</b> ({pnl_arrow}{pnl_pct:.1f}%)")
        lines.append(f"P&L:      <b>{pnl_arrow}${pnl_dlr:.0f}</b>")
        lines.append(f"Target:   ${target_prem:.2f} (+20%) | Stop: ${stop_prem:.2f} (-20%)")
    else:
        lines.append("(no live quote)")

    lines += [
        "",
        f"<b>STATUS: {thesis_line}</b>",
    ]

    msg = "\n".join(lines)
    send_telegram(msg)
    print(f"  recap sent: {ticker} opt={current_opt} underlying={current_px} pnl={pnl_pct}")

    return {
        "status": "ok", "mode": "suggestion_recap",
        "ticker": ticker, "contract": symbol,
        "option_now": current_opt, "underlying_now": current_px,
        "pnl_pct": pnl_pct, "pnl_dollars": pnl_dlr,
    }


def run_suggestion_pnl_check() -> dict:
    """12:00 and 15:00 ET. Fetch current option price for this morning's
    suggestion and send a Telegram alert if a P&L threshold was crossed."""
    print("[suggestion-pnl-check] checking P&L on daily suggestion")
    set_active_strategy(STRATEGY_SUGGESTION)

    pick = load_daily_suggestion()
    if not pick:
        print("  no suggestion for today")
        return {"status": "ok", "mode": "suggestion_pnl_check", "reason": "no_suggestion"}

    cont   = pick.get("contract") or {}
    symbol = cont.get("symbol", "")
    if not symbol:
        return {"status": "ok", "mode": "suggestion_pnl_check", "reason": "no_contract_symbol"}

    snaps = get_option_snapshots([symbol])
    snap  = snaps.get(symbol, {}) or {}
    quote = snap.get("latestQuote") or {}
    bid   = float(quote.get("bp") or 0)
    ask   = float(quote.get("ap") or 0)
    if bid <= 0 or ask <= 0:
        print(f"  no live quote for {symbol}")
        return {"status": "ok", "mode": "suggestion_pnl_check", "reason": "no_quote"}

    current  = (bid + ask) / 2
    entry    = float(cont.get("premium_est") or 0)
    if entry <= 0:
        return {"status": "ok", "mode": "suggestion_pnl_check", "reason": "no_entry_price"}

    pnl_pct = round((current - entry) / entry * 100, 1)
    qty     = int(cont.get("qty") or 1)
    pnl_dlr = round((current - entry) * 100 * qty, 2)
    ticker  = pick.get("ticker", "?")
    now_str = datetime.now(ET).strftime("%H:%M ET")
    print(f"  {symbol}: entry=${entry:.2f} now=${current:.2f} pnl={pnl_pct:+.1f}% (${pnl_dlr:+.0f})")

    try:
        resp = ssm.get_parameter(Name="/trading-bot/daily-suggestion-alerts", WithDecryption=False)
        alert_state = json.loads(resp["Parameter"]["Value"])
        if alert_state.get("date") != date.today().isoformat():
            alert_state = {"date": date.today().isoformat(), "sent": []}
    except Exception:
        alert_state = {"date": date.today().isoformat(), "sent": []}
    sent = alert_state.get("sent", [])

    def _send_alert_once(key: str, msg: str) -> bool:
        if key in sent:
            return False
        if send_telegram(msg):
            sent.append(key)
            alert_state["sent"] = sent
            try:
                ssm.put_parameter(
                    Name="/trading-bot/daily-suggestion-alerts",
                    Value=json.dumps(alert_state),
                    Type="String", Overwrite=True,
                )
            except Exception:
                pass
            return True
        return False

    triggered = None
    if pnl_pct >= SUGGESTION_PNL_ALERT_UP2 and "up20" not in sent:
        _send_alert_once("up20", (
            f"<b>TARGET HIT - {ticker} +{pnl_pct:.1f}%</b>\n\n"
            f"Entry ${entry:.2f} now ${current:.2f} ({now_str})\n"
            f"P&L: <b>+${pnl_dlr:.0f}</b> on {qty} contract(s)\n\n"
            f"<i>+20% target reached. Consider taking profits.</i>"
        ))
        triggered = "up20"
    elif pnl_pct >= SUGGESTION_PNL_ALERT_UP1 and "up10" not in sent:
        _send_alert_once("up10", (
            f"<b>{ticker} UP {pnl_pct:.1f}%</b>\n\n"
            f"Entry ${entry:.2f} now ${current:.2f} ({now_str})\n"
            f"P&L: <b>+${pnl_dlr:.0f}</b> on {qty} contract(s)\n\n"
            f"<i>Trade moving in your direction (+10%).</i>"
        ))
        triggered = "up10"
    elif pnl_pct <= SUGGESTION_PNL_ALERT_DOWN and "down15" not in sent:
        _send_alert_once("down15", (
            f"<b>STOP WATCH - {ticker} {pnl_pct:.1f}%</b>\n\n"
            f"Entry ${entry:.2f} now ${current:.2f} ({now_str})\n"
            f"P&L: <b>${pnl_dlr:.0f}</b> on {qty} contract(s)\n\n"
            f"<i>Approaching -20% stop. Consider cutting losses.</i>"
        ))
        triggered = "down15"

    # 3 PM ET check (UTC hour >= 19 in summer): archive outcome + trigger reflection
    now_utc = datetime.now(timezone.utc)
    is_eod_check = now_utc.hour >= 19
    if is_eod_check and "archived" not in sent:
        try:
            archive_suggestion_outcome(pick, current)
            sent.append("archived")
            alert_state["sent"] = sent
            ssm.put_parameter(
                Name="/trading-bot/daily-suggestion-alerts",
                Value=json.dumps(alert_state),
                Type="String", Overwrite=True,
            )
            print("  [suggestion-pnl-check] outcome archived")
        except Exception as arc_err:
            print(f"  [suggestion-archive-warn] {arc_err}")

        # Check if we've crossed the SUGGESTION_REFLECT_EVERY threshold
        try:
            since = _count_suggestions_since_last_reflect()
            print(f"  [suggestion-reflect] {since} suggestions since last reflect (threshold={SUGGESTION_REFLECT_EVERY})")
            if since >= SUGGESTION_REFLECT_EVERY:
                run_suggestion_reflect()
        except Exception as ref_err:
            print(f"  [suggestion-reflect-warn] {ref_err}")

    return {
        "status": "ok", "mode": "suggestion_pnl_check",
        "ticker": ticker, "contract": symbol,
        "entry_price": entry, "current_price": round(current, 2),
        "pnl_pct": pnl_pct, "pnl_dollars": pnl_dlr,
        "alert_triggered": triggered,
    }


# ---- Weeklies self-learning (Hermes-style reflection loop) -----------------
#
# Every WEEKLIES_REFLECT_EVERY closed trades (and every Sunday morning),
# Claude reads the last 25 outcomes + current strategy config, identifies
# ONE parameter to change and why, saves the new config to SSM, and logs
# the hypothesis to DDB. Scientific method: one variable per cycle.

SYSTEM_WEEKLIES_REFLECTOR = """You are a systematic trading strategy optimizer reviewing a naked single-leg options strategy. Your job is to produce a clear human-readable report AND identify exactly ONE parameter change.

The strategy: single-leg directional calls/puts on high-momentum stocks. Entries scored 0-10. Position sizing by score. Exits managed by stop-loss, time decay, and AI exit evaluation.

Mutable parameters you can tune:
- score_threshold (int 4-9): minimum score to enter a trade
- delta_min / delta_max (float 0.20-0.65): option delta band
- dte_max (int 7-28): maximum days-to-expiry at entry
- stop_floor_pct (float 0.40-0.75): absolute stop loss as fraction of premium paid
- score_high (int 6-10): score needed for full $1000 sizing vs half $500
- debit_high (float 500-2000): max premium for high-conviction trades
- debit_mid (float 250-1000): max premium for standard trades
- universe_size (int 15-40): how many tickers to scan each morning

Rules:
- Change EXACTLY ONE parameter per reflection. Note others as pending.
- Base decisions on the actual trade data, not general theory.
- If win rate >= 55% and avg P&L > 0, set action to HOLD.

Reply JSON only:
{
  "action": "CHANGE"|"HOLD",
  "what_worked": "<1-2 sentences: which tickers/setups produced wins and why>",
  "what_failed": "<1-2 sentences: which tickers/setups lost and the likely cause>",
  "parameter": "<name or null if HOLD>",
  "old_value": <val or null>,
  "new_value": <val or null>,
  "hypothesis": "<specific expected outcome from this change, e.g. win rate above 50%>",
  "confidence": "high"|"medium"|"low",
  "pending_note": "<next parameter to consider after watching this change>"
}"""


def _score_closed_trade(trade: dict) -> float:
    """Score a single closed Weeklies trade in [-1, +1].
    Composite of P&L outcome vs position size, stop quality, and signal accuracy."""
    try:
        prem     = float(trade.get("premium_paid") or 0)
        if prem <= 0:
            return 0.0
        exit_r   = trade.get("exit_reason", "")
        qty      = int(trade.get("qty") or trade.get("qty_original") or 1)
        score    = int(trade.get("score") or 0)

        # Compute actual P&L pct
        exit_val = trade.get("exit_price") or trade.get("exit_value_per_contract")
        if exit_val is not None:
            pnl_pct = (float(exit_val) - prem) / prem
        else:
            # Try to infer from pnl_dollars
            pnl_dlr = float(trade.get("pnl_dollars") or trade.get("realized_pnl") or 0)
            pnl_pct = pnl_dlr / (prem * 100 * qty) if (prem * qty) > 0 else 0.0

        # Base score from P&L
        if pnl_pct >= 0.30:
            base = 1.0
        elif pnl_pct >= 0.10:
            base = 0.5
        elif pnl_pct >= -0.10:
            base = 0.0
        elif pnl_pct >= -0.30:
            base = -0.5
        else:
            base = -1.0

        # Penalty if stop was hit (means sizing/entry was off)
        if "stop" in exit_r.lower():
            base = min(base, -0.3)

        # Bonus for high-score trades that paid off
        if score >= 8 and pnl_pct > 0.20:
            base = min(base + 0.2, 1.0)

        # Penalty for high-score trades that lost badly
        if score >= 8 and pnl_pct < -0.30:
            base = max(base - 0.2, -1.0)

        return round(base, 3)
    except Exception:
        return 0.0


def _load_recent_closed_weeklies(n: int = 25) -> list[dict]:
    """Load the last N closed Weeklies trades from DDB across all CLOSED partitions."""
    set_active_strategy(STRATEGY_NAKED)
    prefix  = _pk_prefix()   # "STRAT3#"
    trades  = []
    # Scan last 90 days worth of partition keys
    for days_ago in range(0, 90):
        d = (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y%m%d")
        pk = f"{prefix}SPREAD#CLOSED#{d}"
        try:
            resp = trade_tbl.query(
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": pk},
            )
            for item in resp.get("Items", []):
                # Accept kind="naked" or missing kind (older records before kind was added)
                if item.get("kind", "naked") == "naked":
                    trades.append(item)
        except Exception:
            pass
        if len(trades) >= n:
            break
    trades.sort(key=lambda t: t.get("closed_at", ""), reverse=True)
    return trades[:n]


def _count_closed_since_last_reflect() -> int:
    """Count closed Weeklies trades since the last reflection run."""
    try:
        resp = ssm.get_parameter(Name=WEEKLIES_REFLECT_SSM, WithDecryption=False)
        state = json.loads(resp["Parameter"]["Value"])
        last_ts = state.get("last_reflect_at", "")
    except Exception:
        last_ts = ""

    if not last_ts:
        return 9999   # never reflected — trigger immediately

    count = 0
    prefix = "STRAT3#"
    for days_ago in range(0, 30):
        d = (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y%m%d")
        pk = f"{prefix}SPREAD#CLOSED#{d}"
        try:
            resp = trade_tbl.query(
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": pk},
            )
            for item in resp.get("Items", []):
                if item.get("kind", "naked") == "naked" and item.get("closed_at", "") > last_ts:
                    count += 1
        except Exception:
            pass
    return count


def claude_weeklies_reflect(trades: list[dict], config: dict) -> dict:
    """Ask Claude to review recent trade outcomes and propose ONE config change."""
    client = _anthropic_client()

    # Build compact trade summary for the prompt
    trade_lines = []
    scores = [_score_closed_trade(t) for t in trades]
    wins   = sum(1 for s in scores if s > 0)
    losses = sum(1 for s in scores if s < 0)
    avg_s  = round(sum(scores) / len(scores), 3) if scores else 0

    for i, t in enumerate(trades[:25]):
        prem     = float(t.get("premium_paid") or 0)
        score    = t.get("score", "?")
        exit_r   = t.get("exit_reason", "unknown")[:40]
        ticker   = t.get("underlying") or t.get("ticker") or "?"
        direction = t.get("direction", "?")
        dte_entry = t.get("dte_at_entry", "?")
        ts       = round(scores[i], 2) if i < len(scores) else 0
        trade_lines.append(
            f"  {ticker} {direction} score={score} dte={dte_entry} prem=${prem:.2f} "
            f"exit={exit_r} trade_score={ts:+.2f}"
        )

    trades_str = "\n".join(trade_lines)
    config_str = json.dumps({k: v for k, v in config.items() if k != "version"}, indent=2)

    prompt = (
        f"Trades reviewed: {len(trades)} | Wins: {wins} | Losses: {losses} | "
        f"Avg score: {avg_s:+.3f}\n\n"
        f"Recent trade outcomes (newest first):\n{trades_str}\n\n"
        f"Current strategy config:\n{config_str}"
    )

    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        system=SYSTEM_WEEKLIES_REFLECTOR,
        messages=[
            {"role": "user",      "content": prompt},
            {"role": "assistant", "content": "{"},
        ],
    )
    raw = "{" + (msg.content[0].text if msg.content else "")
    return _parse_json(raw, fallback={"action": "HOLD", "hypothesis": "parse-fail"})


def run_weeklies_reflect(force: bool = False) -> dict:
    """Hermes-style reflection cycle for Weeklies strategy.
    Reads last 25 closed trades, asks Claude for ONE config change,
    saves new config to SSM, logs hypothesis to DDB.
    Runs every Sunday morning + after every WEEKLIES_REFLECT_EVERY closed trades.
    Skips silently if no new trades since last run (prevents daily-schedule spam)."""
    set_active_strategy(STRATEGY_NAKED)

    # Guard: skip if no new trades since last reflect (unless forced or never run)
    if not force:
        since_last = _count_closed_since_last_reflect()
        if since_last == 0:
            print("[weeklies-reflect] no new trades since last run — skipping")
            return {"status": "skipped", "reason": "no_new_trades"}

    print("[weeklies-reflect] starting reflection cycle")

    trades = _load_recent_closed_weeklies(n=25)
    print(f"  [weeklies-reflect] loaded {len(trades)} closed trades")

    if len(trades) < 3:
        print("  [weeklies-reflect] fewer than 3 trades — skipping, need more data")
        return {"status": "skipped", "reason": "insufficient_data", "trade_count": len(trades)}

    config = get_weeklies_strategy_config()
    current_version = config.get("version", "v01")
    print(f"  [weeklies-reflect] current config version={current_version}")

    hypothesis = claude_weeklies_reflect(trades, config)
    action     = (hypothesis.get("action") or "HOLD").upper()
    print(f"  [weeklies-reflect] Claude says: {action} — {hypothesis.get('hypothesis','')}")

    if action != "CHANGE":
        # No change — log the hold and update reflect state
        log_event("reflect_hold", "WEEKLIES", {
            "version": current_version,
            "trade_count": len(trades),
            "hypothesis": hypothesis.get("hypothesis", ""),
        })
        _update_reflect_state(current_version, len(trades))
        send_telegram(
            f"<b>Weeklies review - no change ({current_version})</b>\n"
            f"Based on {len(trades)} closed trades\n\n"
            f"<b>What worked:</b>\n{hypothesis.get('what_worked','')}\n\n"
            f"<b>What failed:</b>\n{hypothesis.get('what_failed','')}\n\n"
            f"Strategy performing well enough - no parameter change this cycle."
        )
        return {"status": "ok", "action": "hold", "version": current_version,
                "trade_count": len(trades), "hypothesis": hypothesis}

    # Apply the change
    param     = hypothesis.get("parameter", "")
    old_val   = hypothesis.get("old_value")
    new_val   = hypothesis.get("new_value")

    if not param or param not in config or new_val is None:
        print(f"  [weeklies-reflect] invalid hypothesis param={param!r} — aborting")
        return {"status": "error", "reason": "invalid_parameter", "hypothesis": hypothesis}

    # Archive old config to DDB before overwriting
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    trade_tbl.put_item(Item=_to_ddb({
        "pk":               f"STRAT3#REFLECT#{today}",
        "sk":               datetime.now(timezone.utc).isoformat(),
        "version_from":     current_version,
        "parameter":        param,
        "old_value":        str(old_val),
        "new_value":        str(new_val),
        "hypothesis":       hypothesis.get("hypothesis", ""),
        "confidence":       hypothesis.get("confidence", ""),
        "pending_note":     hypothesis.get("pending_note", ""),
        "trade_count":      len(trades),
        "avg_score":        round(sum(_score_closed_trade(t) for t in trades) / max(len(trades), 1), 3),
    }))

    # Bump version and apply change
    try:
        v_num  = int(current_version.replace("v", "")) + 1
        new_version = f"v{v_num:02d}"
    except Exception:
        new_version = "v02"

    new_config = {**config, param: new_val, "version": new_version}
    _save_weeklies_strategy_config(new_config)
    _update_reflect_state(new_version, len(trades))

    print(f"  [weeklies-reflect] {current_version} -> {new_version}: {param} {old_val} -> {new_val}")
    hyp_text = hypothesis.get("hypothesis", "")
    conf_text = hypothesis.get("confidence", "")
    next_text = hypothesis.get("pending_note", "(none)")
    send_alert(
        f"[Weeklies] Strategy updated {current_version} -> {new_version}",
        f"Parameter changed: {param}\n"
        f"Old: {old_val}  New: {new_val}\n\n"
        f"Hypothesis: {hyp_text}\n\n"
        f"Confidence: {conf_text}\n"
        f"Based on {len(trades)} closed trades.\n\n"
        f"Next to try: {next_text}",
    )
    what_worked = hypothesis.get("what_worked", "")
    what_failed = hypothesis.get("what_failed", "")
    send_telegram(
        f"<b>Weeklies strategy update: {current_version} -> {new_version}</b>\n"
        f"Based on {len(trades)} closed trades\n\n"
        f"<b>What worked:</b>\n{what_worked}\n\n"
        f"<b>What failed:</b>\n{what_failed}\n\n"
        f"<b>Fix applied:</b> {param} changed {old_val} -> {new_val}\n"
        f"{hyp_text}\n\n"
        f"Confidence: {conf_text}\n"
        f"Up next: {next_text}"
    )

    return {
        "status":    "ok",
        "action":    "changed",
        "version":   new_version,
        "parameter": param,
        "old_value": old_val,
        "new_value": new_val,
        "hypothesis": hypothesis,
        "trade_count": len(trades),
    }


def _update_reflect_state(version: str, trade_count: int) -> None:
    """Save last-reflect metadata to SSM so we know when the next trigger fires."""
    try:
        state = {
            "last_reflect_at": datetime.now(timezone.utc).isoformat(),
            "last_version":    version,
            "last_trade_count": trade_count,
        }
        ssm.put_parameter(
            Name=WEEKLIES_REFLECT_SSM,
            Value=json.dumps(state),
            Type="String",
            Overwrite=True,
        )
    except Exception as e:
        print(f"  [reflect-state-warn] {type(e).__name__}: {e}")


# ---- Suggestion self-learning (track record + reflection) ------------------

def get_suggestion_strategy_config() -> dict:
    """Load suggestion config from SSM; fall back to hardcoded defaults."""
    try:
        resp = ssm.get_parameter(Name=SUGGESTION_CONFIG_SSM, WithDecryption=False)
        return json.loads(resp["Parameter"]["Value"])
    except Exception:
        return {
            "version":        "v01",
            "min_score":      SUGGESTION_SCORE_MIN,
            "delta_min":      SUGGESTION_DELTA_MIN,
            "delta_max":      SUGGESTION_DELTA_MAX,
            "dte_min":        SUGGESTION_DTE_MIN,
            "dte_max":        SUGGESTION_DTE_MAX,
            "stop_pct":       SUGGESTION_STOP_PCT,
            "target_pct":     SUGGESTION_TARGET_PCT,
            "bidask_max_pct": SUGGESTION_BIDASK_MAX_PCT,
        }

def _save_suggestion_strategy_config(config: dict) -> None:
    ssm.put_parameter(
        Name=SUGGESTION_CONFIG_SSM,
        Value=json.dumps(config, default=str),
        Type="String", Overwrite=True,
    )

def archive_suggestion_outcome(pick: dict, final_price: float) -> None:
    """Write today's suggestion result to DDB for the reflection track record."""
    today = date.today().isoformat()
    cont  = pick.get("contract") or {}
    entry = float(cont.get("premium_est") or 0)
    qty   = int(cont.get("qty") or 1)
    pnl_pct = round((final_price - entry) / entry * 100, 1) if entry > 0 else 0
    pnl_dlr = round((final_price - entry) * 100 * qty, 2)  if entry > 0 else 0
    trade_tbl.put_item(Item=_to_ddb({
        "pk":              f"SUGGESTION#CLOSED#{today.replace('-','')}",
        "sk":              pick.get("ticker", "UNKNOWN"),
        "date":            today,
        "ticker":          pick.get("ticker"),
        "direction":       pick.get("direction"),
        "score":           pick.get("score"),
        "rationale":       (pick.get("rationale") or "")[:300],
        "entry_price":     entry,
        "exit_price":      round(final_price, 2),
        "pnl_pct":         pnl_pct,
        "pnl_dollars":     pnl_dlr,
        "contract_symbol": cont.get("symbol"),
        "strike":          cont.get("strike"),
        "dte_at_entry":    cont.get("dte"),
        "delta":           cont.get("delta"),
        "iv":              cont.get("iv"),
        "archived_at":     datetime.now(timezone.utc).isoformat(),
    }))
    print(f"  [suggestion-archive] {pick.get('ticker')} {pnl_pct:+.1f}% (${pnl_dlr:+.0f}) archived")

def _load_suggestion_history(n: int = 20) -> list[dict]:
    """Load last N archived suggestion outcomes from DDB."""
    records = []
    for days_ago in range(0, 90):
        d = (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y%m%d")
        pk = f"SUGGESTION#CLOSED#{d}"
        try:
            resp = trade_tbl.query(
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": pk},
            )
            records.extend(resp.get("Items", []))
        except Exception:
            pass
        if len(records) >= n:
            break
    records.sort(key=lambda r: r.get("archived_at", ""), reverse=True)
    return records[:n]

def _count_suggestions_since_last_reflect() -> int:
    """Count archived suggestion outcomes since the last reflection run."""
    try:
        resp = ssm.get_parameter(Name=SUGGESTION_REFLECT_SSM, WithDecryption=False)
        last_ts = json.loads(resp["Parameter"]["Value"]).get("last_reflect_at", "")
    except Exception:
        last_ts = ""
    if not last_ts:
        return 9999
    count = 0
    for days_ago in range(0, 90):
        d = (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y%m%d")
        pk = f"SUGGESTION#CLOSED#{d}"
        try:
            resp = trade_tbl.query(
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": pk},
            )
            for item in resp.get("Items", []):
                if item.get("archived_at", "") > last_ts:
                    count += 1
        except Exception:
            pass
    return count

def _update_suggestion_reflect_state(version: str, count: int) -> None:
    try:
        ssm.put_parameter(
            Name=SUGGESTION_REFLECT_SSM,
            Value=json.dumps({
                "last_reflect_at":  datetime.now(timezone.utc).isoformat(),
                "last_version":     version,
                "last_pick_count":  count,
            }),
            Type="String", Overwrite=True,
        )
    except Exception as e:
        print(f"  [suggestion-reflect-state-warn] {type(e).__name__}: {e}")

SYSTEM_SUGGESTION_REFLECTOR = """You are optimizing a daily options suggestion system. Every trading day it picks ONE naked call/put for a trader to consider. You review the track record and suggest ONE parameter change.

The system scans 35+ tickers, scores each candidate 0-10, picks the top-scoring contract in delta 0.35-0.55, DTE 7-21.

Tunable parameters:
- min_score (float 5-9): minimum score to suggest a trade
- delta_min / delta_max (float 0.25-0.65): contract delta band
- dte_min / dte_max (int 3-30): days-to-expiry at suggestion
- stop_pct (float 0.10-0.40): suggested stop loss %
- target_pct (float 0.10-0.50): suggested profit target %
- bidask_max_pct (float 0.08-0.25): maximum bid-ask spread % of mid

Rules:
- Change EXACTLY ONE parameter. Note others as pending_note.
- Base decisions on the actual pick history, not general theory.
- If win rate >= 55% and avg P&L > 0, set action to HOLD.
- Win = exit_price > entry_price (any positive P&L). Loss = negative.

Reply JSON only:
{
  "action": "CHANGE"|"HOLD",
  "what_worked": "<1-2 sentences: which tickers/setups/directions produced wins>",
  "what_failed": "<1-2 sentences: which setups lost and the likely cause>",
  "parameter": "<name or null>",
  "old_value": <number or null>,
  "new_value": <number or null>,
  "hypothesis": "<specific expected outcome>",
  "confidence": "high"|"medium"|"low",
  "pending_note": "<next parameter to consider after watching this change>"
}"""

def run_suggestion_reflect() -> dict:
    """Review suggestion track record, propose ONE config change, send Telegram report."""
    print("[suggestion-reflect] starting")
    since = _count_suggestions_since_last_reflect()
    if since == 0:
        print("  no new suggestions since last reflect — skipping")
        return {"status": "skipped", "reason": "no_new_picks"}

    history = _load_suggestion_history(n=20)
    print(f"  loaded {len(history)} suggestion records")
    if len(history) < 3:
        print("  fewer than 3 records — need more data")
        return {"status": "skipped", "reason": "insufficient_data", "count": len(history)}

    config  = get_suggestion_strategy_config()
    version = config.get("version", "v01")

    wins    = sum(1 for r in history if float(r.get("pnl_pct") or 0) > 0)
    losses  = sum(1 for r in history if float(r.get("pnl_pct") or 0) < 0)
    avg_pnl = round(sum(float(r.get("pnl_pct") or 0) for r in history) / len(history), 1)

    lines = []
    for r in history[:20]:
        lines.append(
            f"  {r.get('date','')} {r.get('ticker','?')} {r.get('direction','?')} "
            f"score={r.get('score','?')} delta={r.get('delta','?')} "
            f"dte={r.get('dte_at_entry','?')} entry=${float(r.get('entry_price') or 0):.2f} "
            f"exit=${float(r.get('exit_price') or 0):.2f} pnl={float(r.get('pnl_pct') or 0):+.1f}%"
        )
    prompt = (
        f"Picks reviewed: {len(history)} | Wins: {wins} | Losses: {losses} | "
        f"Avg P&L: {avg_pnl:+.1f}%\n\n"
        f"Recent outcomes (newest first):\n" + "\n".join(lines) +
        f"\n\nCurrent config:\n{json.dumps({k:v for k,v in config.items() if k!='version'}, indent=2)}"
    )

    client = _anthropic_client()
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=350,
        system=SYSTEM_SUGGESTION_REFLECTOR,
        messages=[
            {"role": "user",      "content": prompt},
            {"role": "assistant", "content": "{"},
        ],
    )
    raw  = "{" + (msg.content[0].text if msg.content else "")
    hyp  = _parse_json(raw, fallback={"action": "HOLD", "hypothesis": "parse-fail"})
    action = (hyp.get("action") or "HOLD").upper()

    what_worked = hyp.get("what_worked", "")
    what_failed = hyp.get("what_failed", "")

    if action != "CHANGE":
        _update_suggestion_reflect_state(version, len(history))
        tg = (
            f"<b>Daily Pick review - no change ({version})</b>\n"
            f"Based on {len(history)} picks | {wins}W / {losses}L | avg {avg_pnl:+.1f}%\n\n"
            f"<b>What worked:</b>\n{what_worked}\n\n"
            f"<b>What failed:</b>\n{what_failed}\n\n"
            f"Strategy holding well - no parameter change this cycle."
        )
        send_telegram(tg)
        return {"status": "ok", "action": "hold", "version": version}

    param   = hyp.get("parameter", "")
    old_val = hyp.get("old_value")
    new_val = hyp.get("new_value")

    if not param or param not in config or new_val is None:
        print(f"  [suggestion-reflect] invalid param={param!r} — aborting")
        _update_suggestion_reflect_state(version, len(history))
        return {"status": "error", "reason": "invalid_parameter", "hypothesis": hyp}

    try:
        v_num = int(version.replace("v", "")) + 1
        new_version = f"v{v_num:02d}"
    except Exception:
        new_version = "v02"

    new_config = {**config, param: new_val, "version": new_version}
    _save_suggestion_strategy_config(new_config)
    _update_suggestion_reflect_state(new_version, len(history))

    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    trade_tbl.put_item(Item=_to_ddb({
        "pk":          f"SUGGESTION#REFLECT#{today}",
        "sk":          datetime.now(timezone.utc).isoformat(),
        "version_from": version,
        "parameter":   param,
        "old_value":   str(old_val),
        "new_value":   str(new_val),
        "hypothesis":  hyp.get("hypothesis", ""),
        "confidence":  hyp.get("confidence", ""),
        "pick_count":  len(history),
        "wins":        wins,
        "losses":      losses,
        "avg_pnl_pct": avg_pnl,
    }))

    print(f"  [suggestion-reflect] {version} -> {new_version}: {param} {old_val} -> {new_val}")
    send_alert(
        f"[Daily Pick] Strategy updated {version} -> {new_version}",
        f"Parameter changed: {param}\nOld: {old_val}  New: {new_val}\n\n"
        f"Hypothesis: {hyp.get('hypothesis','')}\n"
        f"Confidence: {hyp.get('confidence','')}\n"
        f"Based on {len(history)} picks ({wins}W/{losses}L, avg {avg_pnl:+.1f}%)\n\n"
        f"Next: {hyp.get('pending_note','(none)')}",
    )
    send_telegram(
        f"<b>Daily Pick strategy update: {version} -> {new_version}</b>\n"
        f"Based on {len(history)} picks | {wins}W / {losses}L | avg {avg_pnl:+.1f}%\n\n"
        f"<b>What worked:</b>\n{what_worked}\n\n"
        f"<b>What failed:</b>\n{what_failed}\n\n"
        f"<b>Fix applied:</b> {param} changed {old_val} -> {new_val}\n"
        f"{hyp.get('hypothesis','')}\n\n"
        f"Confidence: {hyp.get('confidence','')}  |  Next: {hyp.get('pending_note','(none)')}"
    )
    return {"status": "ok", "action": "changed", "version": new_version,
            "parameter": param, "old_value": old_val, "new_value": new_val}


# ---- Main handler ----------------------------------------------------------

def handler(event, context):
    # Dashboard GET requests
    http_method = (event.get("requestContext", {}).get("http", {}) or {}).get("method")
    if http_method in ("GET", "OPTIONS"):
        if http_method == "OPTIONS":
            return {"statusCode": 204, "headers": _cors_headers(), "body": ""}
        params = event.get("queryStringParameters") or {}
        action = params.get("action")
        # Strategy param scopes the read to that strategy's DDB partition.
        # Default = credit_spread for backward compatibility with existing dashboard.
        set_active_strategy(params.get("strategy", STRATEGY_CREDIT_SPREAD))
        try:
            if action == "trades":
                limit = min(int(params.get("limit", 30)), 100)
                return {"statusCode": 200, "headers": _cors_headers(), "body": json.dumps(_recent_trades(limit), default=str)}
            if action == "spreads":
                return {"statusCode": 200, "headers": _cors_headers(), "body": json.dumps(_open_spreads_with_marks(), default=str)}
            if action == "notes":
                limit = min(int(params.get("limit", 5)), 20)
                return {"statusCode": 200, "headers": _cors_headers(), "body": json.dumps(_recent_notes(limit), default=str)}
            if action == "analytics":
                days = min(int(params.get("days", 90)), 365)
                return {"statusCode": 200, "headers": _cors_headers(), "body": json.dumps(_compute_analytics(days), default=str)}
            if action == "reflections":
                return {"statusCode": 200, "headers": _cors_headers(), "body": json.dumps(_recent_reflections(), default=str)}
        except Exception as e:
            return {"statusCode": 500, "headers": _cors_headers(), "body": json.dumps({"error": str(e)})}
        return {"statusCode": 400, "headers": _cors_headers(), "body": json.dumps({"error": "unknown action"})}

    # Scheduled run
    mode     = (event or {}).get("mode", "midday").lower()
    strategy = set_active_strategy((event or {}).get("strategy", STRATEGY_CREDIT_SPREAD))
    now_et   = datetime.now(ET)
    print(f"[{now_et.strftime('%Y-%m-%d %H:%M ET')}] Bot triggered - strategy={strategy} mode={mode}")

    # Maintenance modes don't care about strategy or market hours.
    if mode == "refresh_earnings":
        try:
            return run_refresh_earnings()
        except Exception as e:
            print(f"[refresh-earnings-fatal] {type(e).__name__}: {e}")
            return {"status": "error", "mode": mode, "error": str(e)}

    if mode == "regime_check":
        try:
            return run_regime_check()
        except Exception as e:
            print(f"[regime-check-fatal] {type(e).__name__}: {e}")
            return {"status": "error", "mode": mode, "error": str(e)}

    if mode == "refresh_universe":
        try:
            return run_refresh_universe()
        except Exception as e:
            print(f"[refresh-universe-fatal] {type(e).__name__}: {e}")
            return {"status": "error", "mode": mode, "error": str(e)}

    if mode == "reconcile":
        try:
            return run_reconciliation()
        except Exception as e:
            print(f"[reconcile-fatal] {type(e).__name__}: {e}")
            return {"status": "error", "mode": mode, "error": str(e)}

    # Compute force flag early so suggestion modes can use it
    force = bool((event or {}).get("force"))

    if mode == "daily_suggestion":
        try:
            return run_daily_suggestion(force=force)
        except Exception as e:
            print(f"[suggestion-fatal] {type(e).__name__}: {e}")
            return {"status": "error", "mode": mode, "error": str(e)}

    # Suggestion recap and P&L check are read-only informational messages —
    # exempt from the market gate so they always fire even on holidays.
    if strategy == STRATEGY_SUGGESTION:
        try:
            if mode == "suggestion_recap":     return run_suggestion_recap()
            if mode == "suggestion_pnl_check": return run_suggestion_pnl_check()
            return run_suggestion_pnl_check()
        except Exception as e:
            print(f"[suggestion-fatal] {type(e).__name__}: {e}")
            return {"status": "error", "mode": mode, "error": str(e)}

    # Pre-market modes (morning, premarket) run before the open; everything
    # else needs the market live.
    # Pass {"force": true} on a manual invoke to bypass this guard for testing
    # (the trade itself will still fail at Alpaca if the market is genuinely closed).
    pre_market_modes = {"morning", "premarket", "qqq_morning", "weeklies_reflect"}
    if mode not in pre_market_modes and not market_is_open() and not force:
        print("Market closed - skipping")
        return {"status": "market_closed", "strategy": strategy, "mode": mode}
    if force:
        print("  [force] bypassing market-open check (testing)")

    try:
        if strategy == STRATEGY_SUGGESTION:
            # fallthrough (already handled above)
            pass
        if strategy == STRATEGY_MOMENTUM:
            # ---- QQQ 0-5 DTE Directional Strategy ----
            # Replaces the disabled Squeeze-Cross. Every scheduled 5-min tick
            # routes here. run_qqq_tick() sweeps exits always and scans for
            # new entries only during the defined entry windows.
            if mode in ("premarket", "qqq_morning"): return run_qqq_morning()
            if mode == "qqq_scan":   return run_qqq_scan()    # manual test
            if mode == "qqq_tick":   return run_qqq_tick()    # manual test
            # All EventBridge ticks (every 5 min) route to qqq_tick:
            return run_qqq_tick()
        if strategy == STRATEGY_NAKED:
            if mode == "naked_check":      return run_naked_check()
            if mode == "premarket":        return run_naked_premarket()
            if mode == "entry":            return run_naked_entry()
            if mode == "exit_sweep":       return run_naked_exit_sweep()
            if mode == "weeklies_reflect": return run_weeklies_reflect()
            print(f"Unknown naked mode '{mode}', defaulting to naked_check")
            return run_naked_check()
        # Default: credit spread strategy (preserves prior behavior exactly)
        if mode == "morning":
            return run_morning()
        if mode == "midday":
            return run_midday()
        if mode == "eod":
            return run_eod()
        print(f"Unknown mode '{mode}', defaulting to midday")
        return run_midday()
    except Exception as e:
        # Any top-level failure: log + alert so we know on the phone.
        # Capturing the traceback into the activity log so we can see the
        # exact line that blew up without having to dig through CloudWatch.
        tb = traceback.format_exc()
        print(f"[fatal] {type(e).__name__}: {e}\n{tb}")
        try:
            log_event("error", "BOT", {
                "stage": f"mode_{mode}",
                "error": str(e),
                "type":  type(e).__name__,
                "trace": tb[-1500:],   # last ~30 frames is plenty
            })
        except Exception as log_e:
            # If even logging fails, at least leave a CloudWatch breadcrumb.
            print(f"[fatal-log-failed] {type(log_e).__name__}: {log_e}")
        send_alert(
            f"TradeBot ERROR in {mode} mode",
            f"Exception: {type(e).__name__}: {e}\n\n{tb}\n\n"
            f"Check CloudWatch logs for /aws/lambda/trading-bot-runner.",
        )
        raise
