import time
import pytz
import requests
from datetime import datetime, time as dtime
from dotenv import load_dotenv
import os
from dhanhq import MarketFeed
from dhanhq import DhanContext, dhanhq
from dhan_token import get_access_token
from candle_builder import OneMinuteCandleBuilder
from find_security import load_fno_master, find_option_security
import threading
from signal_emitter import emit_signal
from dispatcher import subscribe
from queue import Queue
import asyncio
from find_instrument import FindInstrument
import option_chain_manager

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

COMMON_ID = "24924d3f-492b-4f03-8ee3-20111b275fdf"
SYMBOL = "NIFTY"

load_dotenv()

STRATEGY_NAME = "NIFTY_OPTION_BUYING_50_reentry"
client_id = os.getenv("CLIENT_ID")
access_token = get_access_token()


IST = pytz.timezone("Asia/Kolkata")

TRADE_START = dtime(9, 16)
TRADE_END   = dtime(15, 14)

CE_TARGET_POINTS = 50
TARGET_POINTS = 50
PE_TARGET_POINTS = 50
LOTSIZE = 65


today = datetime.now(IST).strftime("%Y-%m-%d")


# =========================
# LOGIN
# =========================

dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)
fno_df = load_fno_master()

strategy_id = "24924d3f-492b-4f03-8ee3-20111b275fdf"

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

def build_payload(name, side, token , reason,event_type,ltp,pnl,cum_pnl,lot,users,  strike):

    strike = int(float(strike))
    
    if name == "CE":
        row = AngelCE
    else:
        row = AngelPE

    expiry_date = ce_row["SM_EXPIRY_DATE"]

    day = expiry_date.strftime("%d")
    month = expiry_date.strftime("%b").upper()
    year = expiry_date.strftime("%y")

    symbol = f"NIFTY{day}{month}{year}{strike}{name}"
    expiry = expiry_date.strftime("%Y-%m-%d")

    print("Building payload with symbol:", symbol)
    print("Payload details - Name:", name, "Side:", side, "Token:", token, "Reason:", reason, "Event Type:", event_type, "LTP:", ltp, "PnL:", pnl, "Cum PnL:", cum_pnl, "Lot:", lot, "Strike:", strike)


    return {
        "strategy_id": COMMON_ID,
        "users": users,
        "option": name,
        "side": side,
        "quantity": lot * LOTSIZE,
        "security_id": token,
        "token": int(row["token"]),
        "event_type": event_type,
        "leg_name": name,
        "symbol": str(symbol),
        "exchange": "NFO",
        "expiry":expiry,
        "strike": str(strike),
        "price":ltp,
        "pnl":pnl,
        "cum_pnl":cum_pnl,
        "zebusymbol": "NIFTY",
        "is_ce": True if name == "CE" else False,
        "is_fno": True,
        "antsymbol": "NIFTY",
        "reason":reason
    }


# =========================
# HELPERS
# =========================

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



def get_first_candle_mark(security_id):

    today = datetime.now(IST).strftime("%Y-%m-%d")
   

    idx= dhan.intraday_minute_data(
        security_id=security_id,
        exchange_segment="NSE_FNO",
        instrument_type="OPTIDX",
        from_date=today,
        to_date=today
    )
    print("Today :",type(today),today)

    data = idx.get("data", {})
    closes = data.get("close", [])
    timestamps = data.get("timestamp", [])

    for i in range(len(timestamps)):
        ts = datetime.fromtimestamp(timestamps[i], IST)  

        if ts.hour == 9 and ts.minute == 15:
            mark = float(closes[i])
            print(f"📍 HIST MARK {security_id} @ {mark}")
            return mark

    print("❌ 09:15 candle not found")
    return None



def log_event(leg_name, token, action, price, remark=""):
    payload = {
        "run_id": COMMON_ID,
        "strategy_id": COMMON_ID,
        "leg_name": leg_name,
        "token": int(token),
        "symbol": SYMBOL,
        "action": action,
        "price": price,
        "log_type": "TRADE_EVENT",
        "remark": remark
    }

    try:
        requests.post(EVENT_LOG_URL, json=payload, timeout=3)
    except Exception as e:
        print("EVENT LOG ERROR:", e)


def log_trade_event(
    event_type,   # ENTRY / EXIT
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

        "trade_id": COMMON_ID,         # 🔥 VERY IMPORTANT
        "event_type": event_type,     # ENTRY / EXIT

        "leg_name": leg_name,
        "token": int(token),
        "symbol": symbol,

        "side": side,
        "lots": lot,
        "quantity": lot * LOTSIZE,

        "price": price,

        "reason": reason,
        "deployed_by": COMMON_ID,

        "pnl": str(pnl),
        "cum_pnl":str(cum_pnl)
    }
   
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

telemetry = {
    "strategy_id": COMMON_ID,
    "run_id": COMMON_ID,
    "status": "ACTIVE",
    "pnl": 0.0,
    "pnl_percentage": 0.0,
    "ce_ltp": 0.0,
    "pe_ltp": 0.0,
    "ce_pnl": 0.0,
    "pe_pnl": 0.0
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



next_expiry = get_next_expiry()


def update_rsi(state, candle, period=14):
    """
    Updates RSI using Wilder's smoothing.

    Requires:
        state["avg_gain"]
        state["avg_loss"]
        state["rsi14"]
        state["candles"]

    Returns:
        Latest RSI
    """

    if len(state["candles"]) == 0:
        return None

    previous_close = state["candles"][-1]["close"]
    current_close = candle["close"]

    change = current_close - previous_close

    gain = max(change, 0)
    loss = max(-change, 0)

    avg_gain = (
        (state["avg_gain"] * (period - 1)) + gain
    ) / period

    avg_loss = (
        (state["avg_loss"] * (period - 1)) + loss
    ) / period

    state["avg_gain"] = avg_gain
    state["avg_loss"] = avg_loss

    if avg_loss == 0:
        rsi = 100
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

    state["rsi14"] = rsi

    return rsi


def calculate_rsi(closes, period=14):
    """
    Calculates initial RSI using Wilder's method.

    Returns:
        rsi, avg_gain, avg_loss
    """

    if len(closes) < period + 1:
        return None, None, None

    gains = []
    losses = []

    for i in range(1, period + 1):
        change = closes[i] - closes[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    # Prevent divide-by-zero
    if avg_loss == 0:
        rsi = 100
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

    return rsi, avg_gain, avg_loss



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

def get_market_history_window(candle_count=10, interval=1):
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
        interval=1
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


def init_state():
    return {
        "marked": None,
        "position": False,
        "trading_disabled": False,
        "entry_price": None,
        "entry_time": None,
        "lot": 2,
        "pnl": 0.0,
        "symbol": None,
        "rearm_required": False,
        "moment":0.0,
        "strike":None
    }



# =========================
# CALLBACKS
# =========================


def on_message(msg):

    if msg.get("type") != "Quote Data":
        return
    
    token = str(msg["security_id"])
    ltp = float(msg.get("LTP", 0))

    builder = builders.get(token)

    if not builder:
        return

    candle = builder.process_tick(msg)

    token = str(msg["security_id"])

    # store LTP
    if token == CE_ID:
        #tick_wise_handler("CE", token, ce_state, ltp)
        telemetry["ce_ltp"] = float(ltp or 0)

    if token == PE_ID:
        #tick_wise_handler("PE", token, pe_state, ltp)
        telemetry["pe_ltp"] = float(ltp or 0)  

    # =========================
    # RUN UNIVERSAL EXIT (TICK LEVEL)
    # =========================
    if "ce_ltp" in telemetry and "pe_ltp" in telemetry:

        # for target and zz
        universal_exit_check(telemetry["ce_ltp"], telemetry["pe_ltp"])

    # =========================
    # CANDLE LOGIC
    # =========================
    if candle:

        if token == CE_ID:
            print("50 reentry CE",token)
            print(candle)
            handle_leg("CE", token, candle, ce_state, ltp)

        if token == PE_ID:
            print("50 reentry PE",token)
            print(candle)
            handle_leg("PE", token, candle, pe_state, ltp)

    # =========================
    # TELEMETRY (REAL-TIME PnL)
    # =========================
    ce_running = 0
    pe_running = 0

    if ce_state["position"]:
        ce_running = (telemetry["ce_ltp"] - ce_state["entry_price"]) * LOTSIZE * ce_state["lot"]

    if pe_state["position"]:
        pe_running = (telemetry["pe_ltp"] - pe_state["entry_price"]) * LOTSIZE * pe_state["lot"]

    telemetry["ce_pnl"] = ce_state["pnl"] + ce_running
    telemetry["pe_pnl"] = pe_state["pnl"] + pe_running
    telemetry["pnl"] = telemetry["ce_pnl"] + telemetry["pe_pnl"]




# =====================
# START WS 
# =====================


TOKENS = [CE_ID , PE_ID]

def on_tick(token, msg):

    if token not in TOKENS:
        return  

    on_message(msg)

for t in TOKENS:
    subscribe(t, on_tick)