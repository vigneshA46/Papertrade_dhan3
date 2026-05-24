from dispatcher import publish
from dhanhq import MarketFeed
from dhanhq import dhanhq,DhanContext
from datetime import datetime
from dhan_token import get_access_token
from dotenv import load_dotenv
import os
import pytz
import threading
#import paper_trade_niftyoption50_reentry as strategy2
#import paper_trade_niftyoption35_reentry as strategy3
#import paper_trade_niftyoption35_reentry_point as strategy4
#import paper_trade_niftyoption50_reentry_point as strategy5
#import nifty_option_buying_50_ltp as strategy15
#import nifty_option_buying_cumulative_ltp as straegy16

#import delta_option_buying as strategy6
#import paper_trade_niftyoption8_no_reentry as strategy8
#import vwap_option_buying as strategy10

import Nifty_option_buying3k as strategy9

try:
    import paper_trade_niftyoption50_no_reentry as strategy1
except Exception as e:
    print("strategy1 ERROR:", e)


rb_started = False
rb_buying=False

# collect all tokens
ALL_TOKENS = set()
if 'strategy1' in globals():
    ALL_TOKENS.update(strategy1.TOKENS)


access_token = get_access_token()
client_id = os.getenv("CLIENT_ID")
dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)

print("ALL TOKENS",ALL_TOKENS)

instruments = [
    (MarketFeed.NSE_FNO, token, MarketFeed.Quote)
    for (token) in ALL_TOKENS
]

instruments.append((MarketFeed.IDX, "13", MarketFeed.Quote))

feed = MarketFeed(dhan_context, instruments, "v2")

def on_message(msg):

    global rb_started, rb_buying

    try:

        token = str(msg["security_id"])

        publish(token, msg)

        ist = pytz.timezone("Asia/Kolkata")
        now = datetime.now(ist)

        if not rb_buying and now.hour >= 9 and now.minute >= 1:

            try:

                print("Starting Range Breakout Buying")
                import ORBbuying3k as strategy11


                rb_buying = True

            except Exception as e:

                print("RB BUYING ERROR:", e)



    except Exception as e:

        print("ON MESSAGE ERROR:", e)
    
feed.run_forever()
while True:
    try:


            #ALL_TOKENS.update(strategy9.TOKENS)

            #instruments = [
                #(MarketFeed.NSE_FNO, token, MarketFeed.Quote)
                #for (token) in ALL_TOKENS
            #]

            #feed = MarketFeed(dhan_context, instruments, "v2")

            #feed.on_message = on_message

            #rb_started = True

        data = feed.get_data()

        if data:
            on_message(data)

    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()
        