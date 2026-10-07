SELECT trade_date, open_price, high_price,
       low_price, close_price, volume
FROM daily_prices
WHERE symbol = '' -- fyll i samma aktiesymbol som i main.py (symbol)
ORDER BY trade_date DESC
LIMIT 100;