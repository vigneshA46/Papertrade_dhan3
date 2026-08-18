import time
import pytz
import requests
from datetime import datetime,date, time as dtime
from dotenv import load_dotenv
import os
from dhanhq import MarketFeed
from dhanhq import DhanContext, dhanhq
from datetime import timedelta
from dhan_token import get_access_token
from candle_builder import FiveMinuteCandleBuilder , OneMinuteCandleBuilder
from find_security import load_fno_master, find_option_security
import threading
from dispatcher import subscribe
from queue import Queue
from signal_emitter import emit_signal
#from tests.test_order import get_today_deployments, group_users_by_broker
import asyncio
from find_instrument import FindInstrument
import pandas as pd

from io import StringIO

# =========================
# CONFIG
# =========================


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

load_dotenv()

TRADE_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/event"
EVENT_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/paperlogger"

MCX_MASTER_URL = "https://api.dhan.co/v2/instrument/MCX_COMM"
INTRADAY_URL = "https://api.dhan.co/v2/charts/intraday"



STRATEGY_NAME = "MCX CRUDE OIL OPTION BUYING"


access_token = get_access_token()
client_id = os.getenv("CLIENT_ID")


HEADERS = {
    "Content-Type": "application/json",
    "access-token": access_token
}

UNDERLYING_SYMBOL = "CRUDEOIL"
STRIKE_STEP = 50
INTERVAL_1M = "1"
INTERVAL_15M = "15"

IST = pytz.timezone("Asia/Kolkata")

COMMON_ID = "617126ad-4197-4272-a08f-cc2ad43b3859"
SYMBOL = "CRUDEOIL"

TRADE_START = dtime(15, 31)
TRADE_END   = dtime(22, 30)

LOTSIZE = 100

today = datetime.now(IST).strftime("%Y-%m-%d")
#today = "2026-05-18"


def calculate_rsi(closes, period=14):
    """
    Calculates RSI-14 using Wilder's smoothing method.

    Returns:
        rsi, avg_gain, avg_loss
    """

    if len(closes) < period + 1:
        return None, None, None

    gains = []
    losses = []

    # ---------------------------------
    # Calculate gains and losses
    # ---------------------------------
    for i in range(1, len(closes)):

        change = closes[i] - closes[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0.0)

        else:
            gains.append(0.0)
            losses.append(abs(change))

    # ---------------------------------
    # Initial Wilder average
    # First 14 changes
    # ---------------------------------
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    # ---------------------------------
    # Wilder smoothing
    # Remaining changes
    # ---------------------------------
    for i in range(period, len(gains)):

        avg_gain = (
            (avg_gain * (period - 1)) + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1)) + losses[i]
        ) / period

    # ---------------------------------
    # Calculate final RSI
    # ---------------------------------
    if avg_loss == 0:
        rsi = 100.0

    else:
        rs = avg_gain / avg_loss

        rsi = 100.0 - (
            100.0 / (1.0 + rs)
        )

    return rsi, avg_gain, avg_loss

def update_rsi(state, candle, period=14):

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
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))

    state["rsi14"] = rsi

    return rsi


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
        hour=22,
        minute=50,
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
        exchange_segment="MCX_COMM",
        instrument_type="OPTFUT",
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


# =========================
# LOGIN
# =========================

dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)

def load_fno_master() -> pd.DataFrame:
    print("...downloading FNO master")

    r = requests.get(MCX_MASTER_URL, headers={"access-token": access_token})
    r.raise_for_status()

    # ✅ Use header from API (IMPORTANT)
    df = pd.read_csv(StringIO(r.text), low_memory=False)

    # ✅ Drop unwanted column
    if "Unnamed: 31" in df.columns:
        df = df.drop(columns=["Unnamed: 31"])

    # ✅ Type conversions
    df["STRIKE_PRICE"] = pd.to_numeric(df["STRIKE_PRICE"], errors="coerce")
    df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")

    return df

strategy_id = "617126ad-4197-4272-a08f-cc2ad43b3859"
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

    symbol = f"CRUDEOIL{day}{month}{year}{ATM}{name}"
    expiry = expiry_date.strftime("%Y-%m-%d")

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
        "symbol": symbol,
        "exchange": "MCX",
        "expiry":expiry,
        "strike": ATM,
        "price":ltp,
        "pnl":pnl,
        "cum_pnl":cum_pnl,
        "zebusymbol": "CRUDEOIL",
        "is_ce": True if name == "CE" else False,
        "is_fno": True,
        "antsymbol": "CRUDEOIL",
        "reason":reason
    }



def wait_for_start():
    print("⏳ Waiting for market...")
    while True:
        if datetime.now(IST).time() >= TRADE_START:
            print("✅ Market Started")
            return
        time.sleep(1)


def calculate_atm(price, step=50):
    return int(round(price / step) * step)

# =====================================================
# STEP 3: FETCH INTRADAY DATA
# =====================================================


def fetch_intraday(security_id, instrument, interval, trade_date):
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": "MCX_COMM",
        "instrument": instrument,
        "interval": "15",
        "fromDate": f"{trade_date} {TRADE_START}",
        "toDate": f"{trade_date} {TRADE_END}"
    }

    r = requests.post(INTRADAY_URL, headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"]
    })

    dt = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt.dt.tz_convert(IST)

    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)

    #print(df)

    return df

# =====================================================
# STEP 4: GET 3:15–3:30 FUT CANDLE
# =====================================================

def get_315_candle(df):
    candle = df[
        df["datetime"].dt.strftime("%H:%M:%S") == "15:30:00"
    ]

    if candle.empty:
        raise ValueError("❌ 3:15–3:30 candle not found")

    return candle.iloc[0]



def get_first_candle_mark_rest(security_id, access_token):

    url = "https://api.dhan.co/v2/charts/intraday"

    headers = {
        "access-token": access_token,
        "Content-Type": "application/json"
    }

    payload = {
        "securityId": int(security_id),
        "exchangeSegment": "MCX_COMM",
        "instrument": "OPTFUT",
        "interval": "1",
        "fromDate": today,
        "toDate": today
    }

    response = requests.post(url, json=payload, headers=headers)

    #print("RAW RESPONSE:", response.text) 

    if response.status_code != 200:
        print("❌ API FAILED")
        return None

    res = response.json()
    if "data" in res:
            data = res["data"]
    else:
            data = res

    #data = res.get("data", {})
    closes = data.get("close", [])
    timestamps = data.get("timestamp", [])

    mark = None

    for i in range(len(timestamps)):
        ts = datetime.fromtimestamp(timestamps[i], IST)

        if ts.hour == 15 and ts.minute == 30:
            mark = float(closes[i])

    if mark is not None:
        print(f"📍 15:30 MARK @ {mark}")
        return mark

    print("❌ 15:30 candle not found")
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

def calculate_live_rsi(state, current_ltp, period=14):

    if state["avg_gain"] is None or state["avg_loss"] is None:
        return None

    previous_close = state["candles"][-1]["close"]

    change = current_ltp - previous_close

    gain = max(change, 0)
    loss = max(-change, 0)

    live_avg_gain = (
        (state["avg_gain"] * (period - 1)) + gain
    ) / period

    live_avg_loss = (
        (state["avg_loss"] * (period - 1)) + loss
    ) / period

    if live_avg_loss == 0:
        live_rsi = 100.0
    else:
        rs = live_avg_gain / live_avg_loss
        live_rsi = 100.0 - (
            100.0 / (1.0 + rs)
        )

    state["live_rsi14"] = live_rsi

    return live_rsi


def init_state():
    return {
        "marked": None,
        "position": False,
        "trading_disabled": False,
        "buffer": None,
        "entry_price": None,
        "entry_time": None,
        "lot": 1,
        "pnl": 0.0,
        "symbol": None,
        "rearm_required": False,
        "candles": [],
        "rsi14": None,        # last completed candle RSI
        "live_rsi14": None,   # current forming candle RSI
        "avg_gain": None,
        "avg_loss": None,
    }


# =========================
# START 
# =========================
wait_for_start()

print("\n🚀 CRUDEOIL OPTION BUYING STARTED\n")

threading.Thread(target=trade_log_worker, daemon=True).start()

# =========================
# MAIN
# =========================

def find_current_month_future(df, today):

    r = requests.get(MCX_MASTER_URL, headers={"access-token": access_token})
    r.raise_for_status()

    # ✅ Use header from API (IMPORTANT)
    df = pd.read_csv(StringIO(r.text), low_memory=False)
    
    df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")
    
    trade_date = pd.to_datetime(today)

    fut = df[
        (df["INSTRUMENT"] == "FUTCOM") &
        (df["UNDERLYING_SYMBOL"] == SYMBOL) &
        (df["SM_EXPIRY_DATE"] >= trade_date)
    ]

    if fut.empty:
        raise ValueError("❌ No CRUDEOIL future found")

    return fut.sort_values("SM_EXPIRY_DATE").iloc[0]



def find_option_security(df , strike , option_type, today, target_symbol):

    trade_date = pd.to_datetime(today)
    df=df.copy()

    df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")
    df["STRIKE_PRICE"] = pd.to_numeric(df["STRIKE_PRICE"], errors="coerce")
    #print("COLUMNS:", fno_df.columns.tolist())

    opt = df[
        (df["INSTRUMENT"] == "OPTFUT") & 
        (df["UNDERLYING_SYMBOL"] == SYMBOL) &
        (df["STRIKE_PRICE"] == strike) &  
        (df["OPTION_TYPE"] == option_type) &   
        (df["SM_EXPIRY_DATE"] >= trade_date)
    ]

    #print("OPT",opt)

    if opt.empty:
        raise ValueError(f"❌ No {option_type} found for strike {strike}")
        

    return opt.sort_values("SM_EXPIRY_DATE").iloc[0]

fno_df = load_fno_master()
#print(fno_df.iloc[0])

#today_date = datetime.now().date()

currentfut = find_current_month_future(fno_df, today)

currfuttoken = str(currentfut["SECURITY_ID"])

print("future token", currfuttoken)

fut_df = fetch_intraday(
        currfuttoken,
        instrument="FUTCOM",
        interval=INTERVAL_15M,
        trade_date=today
    )

candle_315 = get_315_candle(fut_df)
marked_price = candle_315["close"]

ATM = calculate_atm(marked_price)
print("ATM strike price", ATM)

ce_row = find_option_security(fno_df, ATM, "CE", today, "CRUDEOIL")
pe_row = find_option_security(fno_df, ATM, "PE", today, "CRUDEOIL")


finder = FindInstrument()

AngelCE = finder.get_mcx_option("CRUDEOIL" , int(ATM) , "CE")
AngelPE = finder.get_mcx_option("CRUDEOIL" , int(ATM) , "PE")

CE_TOKEN = str(ce_row["SECURITY_ID"])
PE_TOKEN = str(pe_row["SECURITY_ID"])

builders = {
    str(CE_TOKEN): OneMinuteCandleBuilder(),
    str(PE_TOKEN): OneMinuteCandleBuilder()
}


""" # Log CE leg
logtradeleg(
    COMMON_ID,
    "CE",
    f"CRUDEOIL CE {ATM}",
    ATM,
    str(today),
    CE_TOKEN
)


logtradeleg(
    COMMON_ID,
    "PE",
    f"CRUDEOIL PE {ATM}",
    ATM,
    str(today),
    PE_TOKEN
)
 """
print("trade leg logged")


print("CE TOKEN", CE_TOKEN)
print("PE TOKEN", PE_TOKEN)



# =========================
# GLOBAL STATE
# =========================



realized_pnl = 0
restricted_mode = False
target_hit = False


def on_message(msg):

    global combined_pnl 

    if msg.get("type") != "Quote Data":
        return

    token = str(msg["security_id"])
    ltp = float(msg.get("LTP", 0)or 0)

    builder = builders.get(token)

    if not builder:
        return

    candle = builder.process_tick(msg)

    token = str(msg["security_id"])


    # =========================
    # Entry +8 Breakout
    # =========================

    if token == CE_ID:
        state = ce_state
        leg_name = "CE"
        live_rsi = calculate_live_rsi(
           ce_state,
            ltp
        )   
        #print("CE LIVE RSI:", live_rsi)

    elif token == PE_ID:
        state = pe_state
        leg_name = "PE"
    else:
        state = None
    
    # =========================
    # CANDLE LOGIC
    # =========================
    if candle:

        if token == CE_ID:
            #update_rsi(ce_state , candle)
            #ce_state["candles"].append(candle)
            print("RSI", ce_state["live_rsi14"])

            #print(
            #    f"CE RSI14: {ce_state['rsi14']:.2f} "
            #   )


def load_history(security_id, candle_count=300):

    start_time, end_time = get_market_history_window(
        candle_count=candle_count,
        interval=1
    )

    print("\n========== HISTORY WINDOW ==========")
    print("From :", start_time)
    print("To   :", end_time)
    print("====================================\n")

    data = dhan.intraday_minute_data(
        security_id=str(security_id),
        exchange_segment="MCX_COMM",
        instrument_type="OPTFUT",
        from_date=start_time.strftime("%Y-%m-%d %H:%M:%S"),
        to_date=end_time.strftime("%Y-%m-%d %H:%M:%S"),
        interval=1
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

    #print(f"Loaded {len(candles)} historical candles")

    return candles[-candle_count:]






today = datetime.now(IST).strftime("%Y-%m-%d")


ATM = calculate_atm(marked_price)
print("ATM strike price :", ATM)

ce_row = find_option_security(fno_df, ATM, "CE", today, "CRUDEOIL")
pe_row = find_option_security(fno_df, ATM, "PE", today, "CRUDEOIL")

CE_ID = str(ce_row["SECURITY_ID"])
PE_ID = str(pe_row["SECURITY_ID"])   


print("security ids")
print(CE_ID, PE_ID)




# =========================
# STATE
# =========================

ce_state = init_state()
pe_state = init_state()

combined_pnl = 0

ce_state["candles"] = load_history(
    CE_ID,
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


print("\n========== EMA CANDLES ==========")

for candle in ema_candles:
    print(
    "Timestamp:",
    candle["datetime"],
    "| Close:",
    candle["close"]
    )

print("=================================\n")



ce_state["rsi14"], ce_state["avg_gain"], ce_state["avg_loss"] = calculate_rsi(
    [c["close"] for c in ema_candles],
    period=14
)

print(
    f"CE RSI14: {ce_state['rsi14']:.2f} "
    f"| Candle: {ce_state['candles'][-1]['datetime']}"
)

instruments = [
    (MarketFeed.MCX, str(CE_ID), MarketFeed.Quote),
    (MarketFeed.MCX, str(PE_ID), MarketFeed.Quote)
    ]


feed = MarketFeed(dhan_context, instruments, "v2")

while True:
    try:
        feed.run_forever()
        data = feed.get_data()

        if data:
                
            on_message(data)

    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()
