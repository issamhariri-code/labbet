import json
import os
import urllib.parse
import urllib.request


def fetch_daily_prices(symbol):
    params = urllib.parse.urlencode({
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": os.environ["ALPHAVANTAGE_API_KEY"],
    })

    url = "https://www.alphavantage.co/query?" + params

    with urllib.request.urlopen(url, timeout=30) as response:
        data = json.load(response)

    prices = data.get("Time Series (Daily)")

    if not prices:
        message = (
            data.get("Error Message")
            or data.get("Information")
            or data.get("Note")
            or "API-svaret saknar kursdata."
        )
        raise RuntimeError(message)

    return prices