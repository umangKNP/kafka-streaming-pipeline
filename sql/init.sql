CREATE TABLE IF NOT EXISTS price_aggregates (
    symbol          VARCHAR(10)     NOT NULL,
    window_start    TIMESTAMPTZ     NOT NULL,
    vwap            NUMERIC(12, 4)  NOT NULL,
    total_volume    BIGINT          NOT NULL,
    trade_count     INT             NOT NULL,
    updated_at      TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (symbol, window_start)
);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id              SERIAL          PRIMARY KEY,
    raw_message     TEXT            NOT NULL,
    error_reason    TEXT            NOT NULL,
    failed_at       TIMESTAMPTZ     DEFAULT NOW(),
    retry_count     INT             DEFAULT 0
);

CREATE INDEX idx_price_aggregates_symbol ON price_aggregates(symbol);
CREATE INDEX idx_price_aggregates_window ON price_aggregates(window_start DESC);
