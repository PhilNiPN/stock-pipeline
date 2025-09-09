CREATE TABLE IF NOT EXISTS price_metrics (
    date        DATE        NOT NULL,
    ticker      VARCHAR(16) NOT NULL,
    open        NUMERIC(18,6),
    high        NUMERIC(18,6),
    low         NUMERIC(18,6),
    close       NUMERIC(18,6),
    adj_close   NUMERIC(18,6),
    volume      BIGINT,
    return      NUMERIC(18,10),
    volatility  NUMERIC(18,10),
    ema_9       NUMERIC(18,6),
    ema_20      NUMERIC(18,6),
    ema_50      NUMERIC(18,6),
    -- MACD indicators
    macd        NUMERIC(18,10),
    macd_signal NUMERIC(18,10),
    macd_histogram NUMERIC(18,10),
    -- Bollinger Bands
    bb_middle   NUMERIC(18,6),
    bb_upper    NUMERIC(18,6),
    bb_lower    NUMERIC(18,6),
    bb_width    NUMERIC(18,10),
    bb_position NUMERIC(18,10),
    -- RSI
    rsi         NUMERIC(18,6),
    PRIMARY KEY (ticker, date)
);

CREATE INDEX IF NOT EXISTS idx_price_metrics_ticker_date ON price_metrics (ticker, date);
CREATE INDEX IF NOT EXISTS idx_price_metrics_date ON price_metrics (date);
