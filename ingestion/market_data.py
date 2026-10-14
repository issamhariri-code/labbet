import json
import os
import urllib.parse
import urllib.request
import urllib.error


def fetch_daily_prices(symbol):
    params = urllib.parse.urlencode({
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": os.environ["ALPHAVANTAGE_API_KEY"],
    })

    url = "https://www.alphavantage.co/query?" + params

    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            data = json.load(response)
    except TimeoutError as error:
        raise RuntimeError(
            f"{symbol}: tidsgränsen för hämtning av kursdata överskreds"
        ) from error
    except urllib.error.HTTPError as error:
        raise RuntimeError(
            f"{symbol}: datakällan svarade med HTTP-fel {error.code}"
        ) from error
    except urllib.error.URLError as error:
        if isinstance(error.reason, TimeoutError):
            raise RuntimeError(
                f"{symbol}: tidsgränsen för hämtning av kursdata överskreds"
            ) from error
        raise RuntimeError(
            f"{symbol}: nätverksfel vid hämtning av kursdata"
        ) from error

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