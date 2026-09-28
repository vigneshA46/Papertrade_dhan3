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

CE_TARGET_POINTS = 10
PE_TARGET_POINTS = 10
TARGET_POINTS = 10

CE_SL_POINTS = 10
PE_SL_POINTS = 10

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


def check_target_stop(leg_name, token, state, ltp):

    if not state["position"]:
        return

    ltp = float(ltp)

    target = state["target_price"]
    stop = state["stop_price"]

    # TARGET
    if ltp >= target:

        exit_trade(
            leg_name,
            token,
            state,
            ltp,
            "TARGET_10_POINTS"
        )

        return

    # STOP LOSS
    if ltp <= stop:

        exit_trade(
            leg_name,
            token,
            state,
            ltp,
            "STOPLOSS_10_POINTS"
        )

        return

def process_rsi_signal(state, candle):
    """
    Process completed 5-minute candle RSI.

    Logic:
        RSI <= 30
            -> activate oversold state

        Later RSI > 30
            -> prepare entry on next candle
    """

    rsi = update_rsi(state, candle)

    if rsi is None:
        return None

    candle_time = candle["datetime"]

    print(
        f"[{state.get('leg_name')}] "
        f"Candle: {candle_time} | "
        f"Close: {candle['close']} | "
        f"RSI: {rsi:.2f}"
    )

    # -------------------------------------------------
    # RSI <= 30
    # -------------------------------------------------
    if rsi <= 30:

        state["rsi_oversold"] = True

        # If RSI goes back below 30 before entry,
        # keep waiting for the eventual recovery above 30.
        state["entry_pending"] = False

        print(
            f"🔵 {state.get('leg_name')} "
            f"RSI <= 30 -> OVERSOLD"
        )

        return None

    # -------------------------------------------------
    # RSI > 30 after previously being <= 30
    # -------------------------------------------------
    if rsi > 30 and state["rsi_oversold"]:

        state["entry_pending"] = True
        state["rsi_oversold"] = False
        state["last_signal_candle_time"] = candle_time

        print(
            f"🟢 {state.get('leg_name')} "
            f"RSI crossed above 30"
        )

        print(
            f"⏳ {state.get('leg_name')} "
            f"ENTRY PENDING -> NEXT 5-MIN CANDLE OPEN"
        )

        return "ENTRY_PENDING"

    return None

def enter_trade(leg_name, token, state, price):
    if state["position"]:
        return

    price = float(price)

    state["position"] = True
    state["entry_price"] = price
    state["entry_time"] = datetime.now(IST)

    state["target_price"] = price + TARGET_POINTS

    if leg_name == "CE":
        sl_points = CE_SL_POINTS
    else:
        sl_points = PE_SL_POINTS

    state["stop_price"] = price - sl_points

    state["entry_pending"] = False

    print("\n==============================")
    print(f"🚀 {leg_name} ENTRY")
    print("Token       :", token)
    print("Entry Price :", price)
    print("Target      :", state["target_price"])
    print("Stop Loss   :", state["stop_price"])
    print("==============================\n")

    # Trade log
    log_trade_event(
        event_type="ENTRY",
        leg_name=leg_name,
        token=token,
        symbol=state.get("symbol", SYMBOL),
        side="BUY",
        lot=state["lot"],
        price=price,
        reason="RSI_RECOVERY_ABOVE_30",
        pnl=0,
        cum_pnl=state["pnl"]
    )

def exit_trade(leg_name, token, state, price, reason):
    if not state["position"]:
        return

    price = float(price)

    entry_price = float(state["entry_price"])

    pnl = (
        (price - entry_price)
        * LOTSIZE
        * state["lot"]
    )

    state["pnl"] += pnl

    print("\n==============================")
    print(f"🔴 {leg_name} EXIT")
    print("Entry       :", entry_price)
    print("Exit        :", price)
    print("Reason      :", reason)
    print("Trade PnL   :", pnl)
    print("Cum PnL     :", state["pnl"])
    print("==============================\n")

    log_trade_event(
        event_type="EXIT",
        leg_name=leg_name,
        token=token,
        symbol=state.get("symbol", SYMBOL),
        side="SELL",
        lot=state["lot"],
        price=price,
        reason=reason,
        pnl=pnl,
        cum_pnl=state["pnl"]
    )

    # Reset position only.
    # RSI setup starts fresh after exit.
    state["position"] = False
    state["entry_price"] = None
    state["entry_time"] = None
    state["target_price"] = None
    state["stop_price"] = None

    state["entry_pending"] = False
    state["rsi_oversold"] = False

def handle_leg(leg_name, token, candle, state, ltp):

    # ---------------------------------------------
    # Completed 5-minute candle
    # ---------------------------------------------
    if candle:

        signal = process_rsi_signal(
            state,
            candle
        )

        if signal == "ENTRY_PENDING":

            print(
                f"⏳ {leg_name}: "
                f"Waiting for NEXT 5-minute candle"
            )

def check_pending_entry(leg_name, token, state, msg, ltp):

    if not state["entry_pending"]:
        return

    if state["position"]:
        state["entry_pending"] = False
        return

    ltt = msg.get("LTT")

    if not ltt:
        return

    try:
        candle_time = datetime.strptime(
            ltt,
            "%H:%M:%S"
        ).time()

    except Exception:
        return

    # ------------------------------------------------
    # 5-minute candle boundaries
    #
    # 09:15
    # 09:20
    # 09:25
    # 09:30 ...
    # ------------------------------------------------

    if candle_time.minute % 5 != 0:
        return

    print(
        f"🚀 {leg_name}: "
        f"NEXT 5-MIN CANDLE STARTED"
    )

    print(
        f"🚀 {leg_name}: "
        f"ENTRY AT FIRST TICK = {ltp}"
    )

    enter_trade(
        leg_name,
        token,
        state,
        ltp
    )

def init_state():
    return {
        # Position
        "position": False,
        "entry_price": None,
        "entry_time": None,
        "lot": 2,
        "pnl": 0.0,

        # Instrument
        "symbol": None,
        "strike": None,
        "leg_name": None,
        "token": None,

        # RSI
        "rsi14": None,
        "avg_gain": None,
        "avg_loss": None,

        # RSI signal state
        "rsi_oversold": False,
        "entry_pending": False,

        # Prevent duplicate processing
        "last_signal_candle_time": None,

        # Trade control
        "target_price": None,
        "stop_price": None,

        # Existing compatibility fields
        "trading_disabled": False,
        "rearm_required": False,
        "moment": 0.0
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
        check_pending_entry(
        "CE",
        token,
        ce_state,
        msg,
        ltp
    )
        telemetry["ce_ltp"] = float(ltp or 0)

    if token == PE_ID:
        #tick_wise_handler("PE", token, pe_state, ltp)
        check_pending_entry(
        "PE",
        token,
        pe_state,
        msg,
        ltp
    )
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



threading.Thread(target=trade_log_worker, daemon=True).start()


wait_for_start()
next_expiry = get_next_expiry()

print("Next expiry:", next_expiry)

# =========================
# INDEX FIRST CANDLE
# =========================
idx = dhan.intraday_minute_data(
    security_id=13,
    exchange_segment="IDX_I",
    instrument_type="INDEX",
    from_date=today,
    to_date=today
)

data = idx.get("data", {})

opens = data.get("open", [])
highs = data.get("high", [])
lows = data.get("low", [])
closes = data.get("close", [])
volumes = data.get("volume", [])
timestamps = data.get("timestamp", [])

opening_candles = []


for i in range(len(timestamps)):
    ts = datetime.fromtimestamp(timestamps[i], IST) 

    if ts.hour == 9 and 15 <= ts.minute <= 17:
        candle = {
            "timestamp": timestamps[i],
            "open": opens[i],
            "high": highs[i],
            "low": lows[i],
            "close": closes[i],
            "volume": volumes[i]
        }
        opening_candles.append(candle)

print("Opening candles:", opening_candles)

if opening_candles:
    atm_price = float(opening_candles[0]["close"])  
    ATM = calculate_atm(atm_price)
    print("📌 ATM:", ATM)

else:
    print("Waiting for 9:17 candle...")


# =========================
# OPTION SELECTION
# =========================

# =========================
# OPTION CHAIN
# =========================

atm = ATM



oc = dhan.option_chain(
    under_security_id=13,                       # Nifty
    under_exchange_segment="IDX_I",
    expiry=str(next_expiry)
)

option_data = oc["data"]["data"]["oc"]



target = 210

best_ce = None
best_pe = None

best_ce_ltp = float("inf")
best_pe_ltp = float("inf")


for strike, strike_data in option_data.items():

    strike = float(strike)

    # ================= CE =================
    # ONLY ATM OR ITM CE
    if strike <= atm and "ce" in strike_data:

        ce_ltp = strike_data["ce"]["last_price"]

        if ce_ltp >= target and ce_ltp < best_ce_ltp:

            best_ce_ltp = ce_ltp

            best_ce = {
                "strike": strike,
                "ltp": ce_ltp,
                "security_id": strike_data["ce"]["security_id"]
                }

    # ================= PE =================
    # ONLY ATM OR ITM PE
    # ================= PE =================
    
    if strike >= atm and "pe" in strike_data:

        pe_ltp = strike_data["pe"]["last_price"]

        if pe_ltp >= target and pe_ltp < best_pe_ltp:

            best_pe_ltp = pe_ltp

            best_pe = {
                "strike": strike,
                "ltp": pe_ltp,
                "security_id": strike_data["pe"]["security_id"]
            }    # FINAL VALUES



ce_strike = best_ce["strike"]
CE_ID = str(best_ce["security_id"])

pe_strike = best_pe["strike"]
PE_ID = str(best_pe["security_id"])

ce_security_id = CE_ID
pe_security_id = PE_ID


ce_state = init_state()
pe_state = init_state()

builders = {
    CE_ID: FiveMinuteCandleBuilder(),
    PE_ID: FiveMinuteCandleBuilder()
}


finder=FindInstrument()

ce_row = find_option_security(fno_df, ce_strike, "CE", today, "NIFTY")
pe_row = find_option_security(fno_df, pe_strike, "PE", today, "NIFTY")

AngelCE = finder.get_option("NIFTY" , int(ce_strike) , "CE")
AngelPE = finder.get_option("NIFTY" , int(pe_strike) , "PE")

#print("angel tokens" , AngelCE , AngelPE)

ce_state["leg_name"] = "CE"
ce_state["token"] = CE_ID
ce_state["strike"] = ce_strike

pe_state["leg_name"] = "PE"
pe_state["token"] = PE_ID
pe_state["strike"] = pe_strike



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


ce_state["candles"] = load_history(
    ce_security_id,
    candle_count=200
)


pe_state["candles"] = load_history(
    pe_security_id,
    candle_count=200
)

ema_candles = ce_state["candles"]

current_minute = datetime.now(IST).replace(
    second=0,
    microsecond=0
)

last_candle_time = ema_candles[-1]["datetime"].replace(
    second=0,
    microsecond=0
)

print("current minute:", current_minute)
print("last candle time:", last_candle_time)

if current_minute == last_candle_time:
    print("MATCH - removing last candle")
    ema_candles = ema_candles[:-1]
else:
    print("NO MATCH - keeping last candle")

ce_state["live_rsi14"], ce_state["avg_gain"], ce_state["avg_loss"] = calculate_rsi(
    [c["close"] for c in ema_candles],
    period=14
)


current_minute = datetime.now(IST).replace(
    second=0,
    microsecond=0
)

last_candle_time = peema_candles[-1]["datetime"].replace(
    second=0,
    microsecond=0
)

print("current minute:", current_minute)
print("last candle time:", last_candle_time)

if current_minute == last_candle_time:
    print("MATCH - removing last candle")
    peema_candles = peema_candles[:-1]
else:
    print("NO MATCH - keeping last candle")


pe_state["live_rsi14"], pe_state["avg_gain"], pe_state["avg_loss"] = calculate_rsi(
    [c["close"] for c in peema_candles],
    period=14
)


instruments = [
    (MarketFeed.NSE_FNO, CE_ID, MarketFeed.Quote),
    (MarketFeed.NSE_FNO, PE_ID, MarketFeed.Quote)
]

feed = MarketFeed(dhan_context, instruments, "v2")

TOKENS = [
  str(ce_security_id) , str(pe_security_id)
]



# =====================
# START WS 
# =====================


"""
def on_tick(token, msg):

    if token not in TOKENS:
        return  

    on_message(msg)

for t in TOKENS:
    subscribe(t, on_tick)
"""

while True:
    try:

        feed.run_forever()
        msg = feed.get_data()


        if msg:
            on_message(msg)

    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()
 
  