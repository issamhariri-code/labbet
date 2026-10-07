from datetime import date
from decimal import Decimal
import psycopg

from market_data import fetch_daily_prices


symbol = ""     #fyll i symbol
if not symbol.strip(): #stoppar innan API anrop om symbol är tom
    raise ValueError("Fyll i symbol innan körning")
prices = fetch_daily_prices(symbol)

rows = []

for trade_date, values in prices.items():
    if "4. close" not in values:
        raise ValueError(f"{symbol} {trade_date}: stängningspris saknas")

    rows.append((
        symbol,
        date.fromisoformat(trade_date),
        Decimal(values["1. open"]),
        Decimal(values["2. high"]),
        Decimal(values["3. low"]),
        Decimal(values["4. close"]),
        int(values["5. volume"]),
    ))

with psycopg.connect(connect_timeout=10) as connection:
    with connection.cursor() as cursor:
        cursor.executemany("""
            INSERT INTO daily_prices (
                symbol, trade_date, open_price, high_price,
                low_price, close_price, volume
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, trade_date) DO NOTHING;
        """, rows)

        inserted = cursor.rowcount

print(f"{symbol}: hämtade {len(rows)} dagsrader.")
print(f"Transaktionen sparad. Nya rader: {inserted}")

#processen är anropa funktionen som finns i market_data.py väljer vald ticker som testaktie
#omvandlar datum,priser och volym till rätt datayper
# rader skickas till postgre finns samma aktie och datum redan lämnas den oförändrad
