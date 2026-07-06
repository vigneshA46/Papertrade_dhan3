import time
import pytz
import requests
from datetime import datetime, time as dtime
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
        "ema9": None
    }



def get_ema_bootstrap_window(minutes=51):

    now = datetime.now(IST)

    end_time = now.replace(second=0, microsecond=0)

    start_time = end_time - timedelta(minutes=minutes)

    print("EMA Bootstrap Window:")
    print(start_time, end_time)

    return start_time, end_time



def load_history(security_id):

    start_time, end_time = get_ema_bootstrap_window()

    data = dhan.intraday_minute_data(
        security_id=security_id,
        exchange_segment="NSE_FNO",
        instrument_type="OPTIDX",
        from_date=start_time.strftime("%Y-%m-%d"),
        to_date=end_time.strftime("%Y-%m-%d"),
        interval=5
    )

    print("Raw intraday data:")
    print(data)

    candles = []

    raw = data.get("data", {})

    opens = raw.get("open", [])
    highs = raw.get("high", [])
    lows = raw.get("low", [])
    closes = raw.get("close", [])
    volumes = raw.get("volume", [])
    timestamps = raw.get("timestamp", [])

    for i in range(len(timestamps)):

        ts = datetime.fromtimestamp(timestamps[i], IST)

        if start_time <= ts <= end_time:

            candles.append({
                "timestamp": timestamps[i],
                "open": opens[i],
                "high": highs[i],
                "low": lows[i],
                "close": closes[i],
                "volume": volumes[i]
            })

    return candles[-51:]


def update_ema(state, candle):

    state["candles"].append(candle)
    if len(state["candles"]) > 200:
        state["candles"].pop(0)
    closes = [
        float(c["close"])
        for c in state["candles"]
    ]
    state["ema50"] = calculate_ema(closes)
    return state["ema50"]


def calculate_ema(closes, period=9):

    if len(closes) < period:
        return None

    multiplier = 2 / (period + 1)

    ema = sum(closes[:period]) / period

    for close in closes[period:]:
        ema = ((close - ema) * multiplier) + ema

    return ema



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


ce_state["candles"] = load_history(str(ce_security_id))

print("candles")
print(ce_state["candles"])

 