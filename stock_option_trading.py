import requests

API_URL = "https://dreaminalgo-backend-production.up.railway.app/api/stocks/today"


def get_todays_stocks():
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()

        data = response.json()

        if not data.get("success"):
            print("API returned success=False")
            return []

        stocks = data.get("stocks", [])

        print(f"Found {len(stocks)} stock(s)")

        for stock in stocks:
            print(
                f"{stock['symbol']} | "
                f"IEP: {stock['iep']} | "
                f"Prev Close: {stock['prev_close']}"
            )

        return stocks

    except Exception as e:
        print(f"Error fetching stocks: {e}")
        return []

if __name__ == "__main__":
    stocks = get_todays_stocks()
    print("STOCKS FOR TODAY:", stocks)
    stocks = get_todays_stocks()

    symbols = [stock["symbol"] for stock in stocks]

    print("SYMBOLS:", symbols)