from datetime import date
from decimal import Decimal, InvalidOperation
import psycopg

from market_data import fetch_daily_prices


symbol = ""     #fyll i symbol
if not symbol.strip(): #stoppar innan API anrop om symbol är tom
    raise ValueError("Fyll i symbol innan körning")
prices = fetch_daily_prices(symbol)

required_fields = {
    "1. open": "öppningspris",
    "2. high": "högsta pris",
    "3. low": "lägsta pris",
    "4. close": "stängningspris",
    "5. volume": "volym",
}

rows = []

for trade_date, values in prices.items():
    for field, name in required_fields.items():
        if field not in values:   
            raise ValueError(f"{symbol} {trade_date}: {name} saknas")

        if values[field] is None or values[field] == "":
            raise ValueError(f"{symbol} {trade_date} {name} saknas")
        if field != "5. volume" and type(values[field]) not in (str, int):
            raise ValueError(
                f"{symbol} {trade_date}: {name} måste vara text eller heltal"
            )
    try:
        close_price = Decimal(values["4. close"])
    except InvalidOperation as error:
        raise ValueError(
            f"{symbol} {trade_date}: stängningspris är inte ett giltigt tal"
        ) from error

    try:
        open_price = Decimal(values["1. open"])
    except InvalidOperation as error:
        raise ValueError(
            f"{symbol} {trade_date}: öppningspris är inte ett giltigt tal"
        ) from error

    try:
        high_price = Decimal(values["2. high"])
    except InvalidOperation as error:
        raise ValueError(
            f"{symbol} {trade_date}: högsta pris är inte ett giltigt tal"
        ) from error

    try:
        low_price = Decimal(values["3. low"])
    except InvalidOperation as error:
        raise ValueError(
            f"{symbol} {trade_date}: lägsta pris är inte ett giltigt tal"
        ) from error

    for name, price in (
        ("öppningspris", open_price),
        ("högsta pris", high_price),
        ("lägsta pris", low_price),
        ("stängningspris", close_price),
    ):
        if not price.is_finite():
            raise ValueError(
                f"{symbol} {trade_date}: {name} måste vara ett ändligt tal"
            )
        if price <= 0:
            raise ValueError(
                f"{symbol} {trade_date}: {name} måste vara större än noll"
            )

        if price > Decimal("999999999999.999999"):
            raise ValueError(
                f"{symbol} {trade_date}: {name} överskrider databasens prisgräns"
            )
        if price != price.quantize(Decimal("0.000001")):
            raise ValueError(
                f"{symbol} {trade_date}: {name}: kan inte lagras exakt med sex decimaler"
            )

    if high_price < low_price:
        raise ValueError(
            f"{symbol} {trade_date}: högsta pris är lägre än lägsta pris"
        )
    
    if not low_price <= open_price <= high_price:
        raise ValueError(
            f"{symbol} {trade_date}: öppningspris ligger utanför prisintervallet"
        )

    if not low_price <= close_price <= high_price:
        raise ValueError(
            f"{symbol} {trade_date}: stängningspris ligger utanför prisintervallet"
        )

    if type(values["5. volume"]) not in (str, int):
        raise ValueError(
        f"{symbol} {trade_date}: volym är inte ett giltigt heltal"
        )

    try:
        volume = int(values["5. volume"])
    except ValueError as error:
        raise ValueError(
            f"{symbol} {trade_date}: volym är inte ett giltigt heltal"
        ) from error

    if volume < 0:
        raise ValueError(
            f"{symbol} {trade_date}: volym får inte vara negativ"
        )

    if volume > 9_223_372_036_854_775_807:
        raise ValueError(
            f"{symbol} {trade_date}: volym överskrider databasens heltalsgräns"
        )

    try:
        parsed_date = date.fromisoformat(trade_date)
        if trade_date != parsed_date.isoformat():
            raise ValueError("Datumet måste ha formatet YYYY-MM-DD")
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"{symbol} {trade_date}: ogiltigt handelsdatum"
        ) from error

    rows.append((
        symbol,
        parsed_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
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
