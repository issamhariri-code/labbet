CREATE TABLE daily_prices (
    symbol TEXT NOT NULL,
    trade_date DATE NOT NULL,
    open_price NUMERIC(18, 6) NOT NULL,
    high_price NUMERIC(18, 6) NOT NULL,
    low_price NUMERIC(18, 6) NOT NULL,
    close_price NUMERIC(18, 6) NOT NULL,
    volume BIGINT NOT NULL CHECK (volume >= 0),

    PRIMARY KEY (symbol, trade_date),

    CHECK (low_price > 0),
    CHECK (high_price >= low_price),
    CHECK (open_price BETWEEN low_price AND high_price),
    CHECK (close_price BETWEEN low_price AND high_price)
);
