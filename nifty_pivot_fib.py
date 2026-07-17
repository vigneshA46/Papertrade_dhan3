import time
import pytz
import requests
from datetime import datetime,date, time as dtime
from datetime import timedelta
from dotenv import load_dotenv
import os
from dhanhq import MarketFeed
from dhanhq import DhanContext, dhanhq
from dhan_token import get_access_token
from candle_builder import OneMinuteCandleBuilder
from find_security import load_fno_master, find_option_security
import threading
from dispatcher import subscribe
from queue import Queue
from signal_emitter import emit_signal

import asyncio
from find_instrument import FindInstrument
import pandas as pd



# =========================
# CONFIG
# =========================
trade_log_queue = Queue()
def trade_log_worker():
    while True:
        payload = trade_log_queue.get()
        try:
            requests.post(TRADE_LOG_URL, json=payload, timeout=2)
        except Exception as e:
            print("TRADE EVENT LOG ERROR:", e)
        finally:
            trade_log_queue.task_done()


ATM = None 
TRADE_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/event"
EVENT_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/paperlogger"

COMMON_ID = "3ff84201-7e4d-4e8d-8308-8241b1bca093"
SYMBOL = "NIFTY"
OPTION_SELECTION_LTP = 90

MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)



load_dotenv()

STRATEGY_NAME = "nifty_pivot_fib"


CE_TARGET_POINTS = 50
PE_TARGET_POINTS = 50

IST = pytz.timezone("Asia/Kolkata")

TRADE_START = dtime(0, 20)
TRADE_END   = dtime(15, 20)

TARGET_POINTS = 50
LOTSIZE = 65

strategy_id = "3ff84201-7e4d-4e8d-8308-8241b1bca093"

today = datetime.now(IST).strftime("%Y-%m-%d")

telemetry = {
    "strategy_id": COMMON_ID,
    "run_id": COMMON_ID,
    "status": "ACTIVE",
    "pnl": 0,
    "pnl_percentage": 0,
    "ce_ltp": 0,
    "pe_ltp": 0,
    "ce_pnl": 0,
    "pe_pnl": 0
}


# ======================================
# NSE HOLIDAYS (Till 31-Dec-2026)
# ======================================

NSE_HOLIDAYS = {
    date(2026, 1, 15),
    date(2026, 1, 26),
    date(2026, 3, 3),
    date(2026, 3, 26),
    date(2026, 3, 31),
    date(2026, 4, 3),
    date(2026, 4, 14),
    date(2026, 5, 1),
    date(2026, 5, 28),
    date(2026, 6, 26),
    date(2026, 9, 14),
    date(2026, 10, 2),
    date(2026, 10, 20),
    date(2026, 11, 10),
    date(2026, 11, 24),
    date(2026, 12, 25),
}

# =========================
# LOGIN
# =========================

access_token = get_access_token()
CLIENT_ID = os.getenv("CLIENT_ID")
dhan_context = DhanContext(CLIENT_ID, access_token)
dhan = dhanhq(dhan_context)


fno_df=load_fno_master()

loop = asyncio.new_event_loop()

def start_loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=start_loop, daemon=True).start()

def run_async(coro):
    try:
        if asyncio.iscoroutine(coro):
            asyncio.run_coroutine_threadsafe(coro, loop)
        else:
            print("❌ Not coroutine:", coro)
    except Exception as e:
        print("WS error: ", e)

def get_today_deployments():
    url = f"https://algoapi.dreamintraders.in/api/deployments/today/{strategy_id}"

    try:
        response = requests.get(url, timeout=10)

        # Raise error if status not 200
        response.raise_for_status()

        data = response.json()

        # 👉 store in variable (this is what you asked)
        user_deployments = data

        return user_deployments

    except requests.exceptions.RequestException as e:
        print("API Error:", e)
        return None

def group_users_by_broker(deployments):
    grouped = {}

    if not deployments:
        return grouped

    for d in deployments:

        if d["type"] == "paper":
            continue
        broker = d.get("broker_name")

        if not broker:
            continue

        if broker not in grouped:
            grouped[broker] = []

        grouped[broker].append(d)

    return grouped


def build_payload(name, side, token , reason,event_type,ltp,pnl,cum_pnl,lot,users):

    if name == "CE":
        row = AngelCE
    else:
        row = AngelPE

    expiry_date = ce_row["SM_EXPIRY_DATE"]

    day = expiry_date.strftime("%d")
    month = expiry_date.strftime("%b").upper()
    year = expiry_date.strftime("%y")

    symbol = f"NIFTY{day}{month}{year}{ATM}{name}"
    expiry = expiry_date.strftime("%Y-%m-%d")

    return {
        "strategy_id": COMMON_ID,
        "users": users,
        "option": name,
        "side": side,
        "quantity": lot*LOTSIZE,
        "security_id": token,
        "token": int(row["token"]),
        "event_type": event_type,
        "leg_name": name,
        "symbol": symbol,
        "exchange": "NFO",
        "expiry":expiry,
        "strike": ATM,
        "price":ltp,
        "pnl":pnl,
        "cum_pnl":cum_pnl,
        "zebusymbol": "NIFTY",
        "is_ce": True if name == "CE" else False,
        "is_fno": True,
        "antsymbol": "NIFTY",
        "reason":reason
    }


def telemetry_broadcaster():
    while True:
        try:
            # 🔥 COPY to avoid mutation issues
            payload = telemetry.copy()

            # 🔥 optional: sanitize (prevents TypeError)
            def safe_number(x):
                try:
                    return float(x)
                except:
                    return 0

            payload = {k: safe_number(v) if k in ["pnl","ce_pnl","pe_pnl","ce_ltp","pe_ltp","pnl_percentage"] else v
                for k, v in payload.items()}


            res = requests.post(
                "https://algoapi.dreamintraders.in/api/telemetry",
                json=payload,
                timeout=0.5   # 🔥 keep it LOW
            )

            # optional debug
            if res.status_code != 200:
                print("Telemetry failed:", res.status_code)

        except Exception as e:
            print("Telemetry error:", e)

        time.sleep(1)

t = threading.Thread(target=telemetry_broadcaster, daemon=True)
t.start()


def logtradeleg(strategyid, leg, symbol, strike_price, date, token):
    url = "https://algoapi.dreamintraders.in/api/tradelegs/create"
    
    payload = {
        "strategy_id": strategyid,
        "leg": leg,
        "symbol": symbol,
        "strike_price": strike_price,
        "date": date,
        "token":str(token)
    }

    try:
        response = requests.post(url, json=payload)

        if response.status_code == 200 or response.status_code == 201:
            print("✅ Trade leg logged successfully")
            return response.json()
        else:
            print(f"❌ Failed to log trade leg: {response.status_code}")
            print(response.text)
            return None

    except Exception as e:
        print(f"⚠️ Error while calling API: {e}")
        return None




def log_trade_event(
    event_type,
    leg_name,
    token,
    symbol,
    side,
    lot,
    price,
    reason,
    pnl,
    cum_pnl
        ):
    payload = {
        "run_id": COMMON_ID,
        "strategy_id": COMMON_ID,
        "trade_id": COMMON_ID,

        "event_type": event_type,
        "leg_name": leg_name,
        "token": int(token),
        "symbol": symbol,

        "side": side,
        "lots": lot,
        "quantity": lot * LOTSIZE,

        "price": float(price),  # 🔥 safety

        "reason": reason,
        "deployed_by": COMMON_ID,
        "pnl": str(pnl),
        "cum_pnl": str(cum_pnl),
    }

    # 🔥 NON-BLOCKING
    trade_log_queue.put(payload)



def wait_for_start():
    print("⏳ Waiting for market...")
    while True:
        if datetime.now(IST).time() >= TRADE_START:
            print("✅ Market Started")
            return
        time.sleep(1)


def calculate_atm(price, step=50):
    return int(round(price / step) * step)

def get_next_expiry():
    """
    Returns current/next NIFTY expiry date
    directly from Dhan expiry list API
    """

    expiries = dhan.expiry_list(
        under_security_id=13,
        under_exchange_segment="IDX_I"
    )

    expiry_list = expiries["data"]

    # first expiry is always nearest expiry
    next_expiry = expiry_list["data"][0]

    return next_expiry

def select_option_contracts(oc, max_ltp=OPTION_SELECTION_LTP):
    """
    Selects the CE and PE contracts whose LTP is
    closest to max_ltp without exceeding it.

    Returns:
        ce_strike, ce_security_id, pe_strike, pe_security_id
    """

    ce_candidate = None
    pe_candidate = None

    option_chain = oc["data"]["data"]["oc"]

    for strike, contracts in option_chain.items():

        strike = int(float(strike))

        # ---------------- CE ----------------
        ce = contracts.get("ce", {})
        ce_ltp = ce.get("last_price", 0)
        ce_sid = ce.get("security_id", 0)

        if (
            ce_sid != 0
            and ce_ltp > 0
            and ce_ltp <= max_ltp
        ):
            if ce_candidate is None or ce_ltp > ce_candidate["ltp"]:
                ce_candidate = {
                    "strike": strike,
                    "security_id": ce_sid,
                    "ltp": ce_ltp,
                }

        # ---------------- PE ----------------
        pe = contracts.get("pe", {})
        pe_ltp = pe.get("last_price", 0)
        pe_sid = pe.get("security_id", 0)

        if (
            pe_sid != 0
            and pe_ltp > 0
            and pe_ltp <= max_ltp
        ):
            if pe_candidate is None or pe_ltp > pe_candidate["ltp"]:
                pe_candidate = {
                    "strike": strike,
                    "security_id": pe_sid,
                    "ltp": pe_ltp,
                }

    if ce_candidate is None:
        raise Exception("No valid CE contract found.")

    if pe_candidate is None:
        raise Exception("No valid PE contract found.")

    return (
        ce_candidate["strike"],
        ce_candidate["security_id"],
        pe_candidate["strike"],
        pe_candidate["security_id"],
    )

def init_state():
    return {
        "marked": None,
        "position": False,
        "trading_disabled": False,

        "entry_price": None,
        "entry_time": None,

        "lot": 1,
        "pnl": 0.0,
        "symbol": None,

        "rearm_required": False,
        "moment": 0.0,

        # EMA
        "candles": [],
        "ema9": None,
        "pivot": None,
        
        "r1": None,
        "r2": None,
        "r3": None,
        "s1": None,
        "s2": None,
        "s3": None,

        # Strategy State
        "signal_state": "IDLE",      # IDLE -> WAITING_RETEST -> IN_POSITION
        "signal_candle": None,       # Candle which closed above EMA
        "target": None,              # Fibonacci target
        "stoploss": None,            # Dynamic EMA SL
    }

def load_history(security_id, candle_count=10):

    start_time, end_time = get_market_history_window(
        candle_count=candle_count,
        interval=5
    )

    print("\n========== HISTORY WINDOW ==========")
    print("From :", start_time)
    print("To   :", end_time)
    print("====================================\n")

    data = dhan.intraday_minute_data(
        security_id=str(security_id),
        exchange_segment="NSE_FNO",
        instrument_type="OPTIDX",
        from_date=start_time.strftime("%Y-%m-%d %H:%M:%S"),
        to_date=end_time.strftime("%Y-%m-%d %H:%M:%S"),
        interval=5
    )

    raw = data.get("data", {})

    opens = raw.get("open", [])
    highs = raw.get("high", [])
    lows = raw.get("low", [])
    closes = raw.get("close", [])
    volumes = raw.get("volume", [])
    timestamps = raw.get("timestamp", [])

    candles = []

    for i in range(len(timestamps)):

        ts = datetime.fromtimestamp(timestamps[i], IST)

        candles.append({
            "timestamp": timestamps[i],
            "datetime": ts,
            "open": float(opens[i]),
            "high": float(highs[i]),
            "low": float(lows[i]),
            "close": float(closes[i]),
            "volume": float(volumes[i])
        })

    print(f"Loaded {len(candles)} historical candles")

    return candles[-candle_count:]

def update_ema(state, candle):

    multiplier = 2 / (9 + 1)

    previous_ema = state["ema9"]

    close = candle["close"]

    new_ema = (
        (close - previous_ema) * multiplier
    ) + previous_ema

    state["ema9"] = new_ema

    state["candles"].append(candle)

    if len(state["candles"]) > 200:
        state["candles"].pop(0)

    return new_ema

def detect_buy_signal(state, candle):
    """
    Detects a fresh bullish setup.

    Condition:
    Candle closes above EMA9.
    """

    if state["position"]:
        return

    if state["signal_state"] != "IDLE":
        return

    if candle["close"] > state["ema9"]:

        state["signal_state"] = "WAITING_RETEST"

        state["signal_candle"] = candle

        print(
            f"✅ Signal Generated @ {candle['datetime']} "
            f"Close={candle['close']:.2f} "
            f"EMA={state['ema9']:.2f}"
        )

def check_retest_entry(state, ltp):
    """
    Executes entry when price retraces
    to EMA after a valid signal.
    """

    if state["signal_state"] != "WAITING_RETEST":
        return False

    ema = state["ema9"]

    # Price touched EMA
    if ltp <= ema:

        state["position"] = True

        state["signal_state"] = "IN_POSITION"

        state["entry_price"] = ltp

        state["entry_time"] = datetime.now(IST)

        state["stoploss"] = ema - 5

        state["target"] = get_fibonacci_target(
            state,
            ltp
        )

        print("\n===============================")
        print("BUY EXECUTED")
        print(f"Entry     : {ltp:.2f}")
        print(f"EMA       : {ema:.2f}")
        print(f"StopLoss  : {state['stoploss']:.2f}")
        print(f"Target    : {state['target']:.2f}")
        print("===============================\n")

        return True

    return False

def exit_position(state, reason, exit_price):
    """
    Closes the current position and
    prepares the state for next setup.
    """

    pnl = (exit_price - state["entry_price"]) * LOTSIZE * state["lot"]

    print("\n===============================")
    print("EXIT")
    print(f"Reason     : {reason}")
    print(f"Entry      : {state['entry_price']:.2f}")
    print(f"Exit       : {exit_price:.2f}")
    print(f"PnL        : {pnl:.2f}")
    print("===============================\n")

    state["position"] = False
    state["signal_state"] = "IDLE"

    state["entry_price"] = None
    state["entry_time"] = None

    state["target"] = None
    state["stoploss"] = None

    state["signal_candle"] = None

    #state["lot"] += 1

def manage_open_position(state, ltp):
    """
    Monitors an open trade.
    """

    if not state["position"]:
        return

    # ---------- Target ----------

    if ltp >= state["target"]:

        exit_position(
            state,
            "TARGET",
            ltp
        )

        return

    # ---------- Stoploss ----------

    if ltp <= state["stoploss"]:

        exit_position(
            state,
            "STOPLOSS",
            ltp
        )

        return
        
def calculate_ema(closes, period=9):

    if len(closes) < period:
        return None

    multiplier = 2 / (period + 1)

    ema = sum(closes[:period]) / period

    for close in closes[period:]:
        ema = ((close - ema) * multiplier) + ema

    return ema

def is_market_holiday(check_date):
    """
    Returns True if the given date is
    a weekend or NSE holiday.
    """

    if isinstance(check_date, datetime):
        check_date = check_date.date()

    # Saturday = 5, Sunday = 6
    if check_date.weekday() >= 5:
        return True

    return check_date in NSE_HOLIDAYS

def get_previous_trading_day(current_date):
    """
    Returns the previous market trading day.
    """

    if isinstance(current_date, datetime):
        current_date = current_date.date()

    current_date -= timedelta(days=1)

    while is_market_holiday(current_date):
        current_date -= timedelta(days=1)

    return current_date

def get_required_market_minutes(candle_count=10, interval=5):
    """
    Converts candle count into
    market trading minutes.
    """

    return candle_count * interval

def count_market_minutes_back(end_time, minutes):
    """
    Walk backwards through MARKET trading minutes only.
    Skips weekends, NSE holidays and non-market hours.
    """

    current = end_time
    remaining = minutes

    while remaining > 0:

        market_open = current.replace(
            hour=9,
            minute=15,
            second=0,
            microsecond=0
        )

        available = int(
            (current - market_open).total_seconds() / 60
        )

        if available >= remaining:
            return current - timedelta(minutes=remaining)

        remaining -= available

        prev_day = get_previous_trading_day(current)

        current = IST.localize(
            datetime.combine(prev_day, MARKET_CLOSE)
        )

    return current

def get_last_market_time():
    """
    Returns the latest valid market timestamp.

    Handles:
    - Before market open
    - During market
    - After market
    - Weekends
    - NSE holidays
    """

    now = datetime.now(IST)

    # Holiday / Weekend
    if is_market_holiday(now):

        prev_day = get_previous_trading_day(now)

        return IST.localize(
            datetime.combine(prev_day, MARKET_CLOSE)
        )

    market_open = now.replace(
        hour=9,
        minute=15,
        second=0,
        microsecond=0
    )

    market_close = now.replace(
        hour=15,
        minute=30,
        second=0,
        microsecond=0
    )

    # Before market opens
    if now < market_open:

        prev_day = get_previous_trading_day(now)

        return IST.localize(
            datetime.combine(prev_day, MARKET_CLOSE)
        )

    # During market
    if market_open <= now <= market_close:
        return now.replace(second=0, microsecond=0)

    # After market closes
    return market_close

def get_market_history_window(candle_count=10, interval=5):
    """
    Returns the history window required
    to fetch the last completed market candles.
    """

    end_time = get_last_market_time()

    required_minutes = candle_count * interval

    start_time = count_market_minutes_back(
        end_time,
        required_minutes
    )

    return start_time, end_time
""" 
def get_previous_day_ohlc(security_id):

    previous_day = get_previous_trading_day(datetime.now(IST))

    # Build NAIVE datetimes (Dhan expects this)
    start_time = datetime.combine(previous_day, MARKET_OPEN)
    end_time = datetime.combine(previous_day, MARKET_CLOSE)

    print("\n========== PREVIOUS DAY OHLC ==========")
    print("From :", start_time)
    print("To   :", end_time)
    print("=======================================\n")

    data = dhan.intraday_minute_data(
        security_id=str(security_id),
        exchange_segment="NSE_FNO",
        instrument_type="OPTIDX",
        from_date=start_time.strftime("%Y-%m-%d %H:%M"),
        to_date=end_time.strftime("%Y-%m-%d %H:%M"),
        interval=5
    )

    print("Previous day data")
    print(data)

    raw = data.get("data")

    if not isinstance(raw, dict):
        print("Historical API Error")
        print(data)
        return None

    highs = raw.get("high", [])
    lows = raw.get("low", [])
    closes = raw.get("close", [])

    if not highs:
        return None

    return {
        "high": max(highs),
        "low": min(lows),
        "close": closes[-1]
    }

"""

def get_previous_day_ohlc(security_id):
    """
    Fetches previous trading day's OHLC from 5-minute candles.
    This is much more reliable than requesting a single day's window.
    """

    today = datetime.now(IST).date()
    previous_day = get_previous_trading_day(today)

    # Fetch last 3 calendar days
    from_date = previous_day - timedelta(days=2)

    start = datetime.combine(from_date, MARKET_OPEN)
    end = datetime.combine(today, MARKET_CLOSE)

    print("\n========== FETCHING PREVIOUS DAY DATA ==========")
    print("From :", start)
    print("To   :", end)
    print("===============================================\n")

    data = dhan.intraday_minute_data(
        security_id=str(security_id),
        exchange_segment="NSE_FNO",
        instrument_type="OPTIDX",
        from_date=start.strftime("%Y-%m-%d %H:%M:%S"),
        to_date=end.strftime("%Y-%m-%d %H:%M:%S"),
        interval=5
    )

    if data.get("status") != "success":
        print(data)
        return None

    raw = data["data"]

    highs = raw["high"]
    lows = raw["low"]
    closes = raw["close"]
    timestamps = raw["timestamp"]

    previous_day_high = []
    previous_day_low = []
    previous_day_close = []

    for i in range(len(timestamps)):

        candle_time = datetime.fromtimestamp(
            timestamps[i],
            IST
        )

        if candle_time.date() == previous_day:

            previous_day_high.append(float(highs[i]))
            previous_day_low.append(float(lows[i]))
            previous_day_close.append(float(closes[i]))

    if len(previous_day_close) == 0:

        print("No previous day candles found.")
        return None

    ohlc = {

        "high": max(previous_day_high),

        "low": min(previous_day_low),

        "close": previous_day_close[-1]

    }

    return ohlc


def calculate_fibonacci_pivot(ohlc):
    """
    Calculates Daily Fibonacci Pivot Levels.
    """

    high = float(ohlc["high"])
    low = float(ohlc["low"])
    close = float(ohlc["close"])

    pivot = (high + low + close) / 3
    rng = high - low

    return {
        "pivot": pivot,

        "r1": pivot + (rng * 0.382),
        "r2": pivot + (rng * 0.618),
        "r3": pivot + rng,

        "s1": pivot - (rng * 0.382),
        "s2": pivot - (rng * 0.618),
        "s3": pivot - rng,
    }

def get_fibonacci_target(state, entry_price):
    """
    Returns the immediate next Fibonacci resistance
    above the entry price.
    """

    levels = [
        state["pivot"],
        state["r1"],
        state["r2"],
        state["r3"]
    ]

    for level in levels:
        if entry_price < level:
            return level

    return state["r3"]

def initialize_fibonacci_pivot(state, security_id):

    ohlc = get_previous_day_ohlc(security_id)

    if ohlc is None:
        print("Unable to calculate Fibonacci Pivot")
        return

    levels = calculate_fibonacci_pivot(ohlc)

    state.update(levels)

    print("\n========== FIBONACCI LEVELS ==========")

    print("Previous Day OHLC")
    print("--------------------------------------")
    print(f"High  : {ohlc['high']:.2f}")
    print(f"Low   : {ohlc['low']:.2f}")
    print(f"Close : {ohlc['close']:.2f}")

    print()

    print(f"Pivot : {levels['pivot']:.2f}")

    print(f"R1    : {levels['r1']:.2f}")
    print(f"R2    : {levels['r2']:.2f}")
    print(f"R3    : {levels['r3']:.2f}")

    print()

    print(f"S1    : {levels['s1']:.2f}")
    print(f"S2    : {levels['s2']:.2f}")
    print(f"S3    : {levels['s3']:.2f}")

    print("======================================\n")

# =========================
# START
# =========================

wait_for_start()

next_expiry = get_next_expiry()

print("Next expiry:", next_expiry)

oc = dhan.option_chain(
    under_security_id=13,                       # Nifty
    under_exchange_segment="IDX_I",
    expiry=str(next_expiry)
)


ce_strike, ce_security_id, pe_strike, pe_security_id = select_option_contracts(oc)

print("CE Strike      :", ce_strike)
print("CE Security ID :", ce_security_id)

print("PE Strike      :", pe_strike)
print("PE Security ID :", pe_security_id)

ce_state = init_state()
pe_state = init_state()

# Log CE leg
logtradeleg(
    COMMON_ID,
    "CE",
    f"NIFTY CE {ce_strike}",
    str(ce_strike),
    str(today),
    str(ce_security_id)
)

# Log PE leg
logtradeleg(
    COMMON_ID,
    "PE",
    f"NIFTY PE {pe_strike}",
    str(pe_strike),
    str(today),
    str(pe_security_id)
)


""" 
mindata = dhan.intraday_minute_data(
    security_id="44642",
    exchange_segment="NSE_FNO",
    instrument_type="OPTIDX",
    from_date="2026-07-01 09:15:00",
    to_date="2026-07-01 10:30:00",
    interval=5
    )

print(mindata)
 
"""

ce_state["candles"] = load_history(
    ce_security_id,
    candle_count=10
)

print("\nCE Historical Candles\n")

for candle in ce_state["candles"]:
    print(candle)

pe_state["candles"] = load_history(
    pe_security_id,
    candle_count=10
)

print("\nPE Historical Candles\n")

for candle in pe_state["candles"]:
    print(candle)
 
ce_state["ema9"] = calculate_ema(
    [c["close"] for c in ce_state["candles"]],
    period=9
)

print("CE Fibonacci")

initialize_fibonacci_pivot(
    ce_state,
    ce_security_id
)

pe_state["ema9"] = calculate_ema(
    [c["close"] for c in pe_state["candles"]],
    period=9
)

print("PE Fibonacci")

initialize_fibonacci_pivot(
    pe_state,
    pe_security_id
)

ce_last_candle = ce_state["candles"][-1]
pe_last_candle = pe_state["candles"][-1]

print("CE Strike", ce_strike)
print(
    f"CE EMA9 ({ce_last_candle['datetime'].strftime('%d-%m-%Y %H:%M')}) : "
    f"{ce_state['ema9']:.2f}"
)


print("PE Strike", pe_strike)
print(
    f"PE EMA9 ({pe_last_candle['datetime'].strftime('%d-%m-%Y %H:%M')}) : "
    f"{pe_state['ema9']:.2f}"
)

 