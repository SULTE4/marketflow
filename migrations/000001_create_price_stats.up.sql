CREATE TABLE aggregated_ticker (
    id           BIGSERIAL,
    pair_name    VARCHAR(20)             NOT NULL,
    exchange     VARCHAR(50)             NOT NULL,
    timestamp    TIMESTAMPTZ             NOT NULL,
    average_price NUMERIC(20, 8)         NOT NULL,
    min_price    NUMERIC(20, 8)          NOT NULL,
    max_price    NUMERIC(20, 8)          NOT NULL,

    PRIMARY KEY (id)
);

CREATE INDEX idx_aggregated_ticker_pair_time
    ON aggregated_ticker (pair_name, timestamp DESC);

CREATE INDEX idx_aggregated_ticker_pair_exchange_time
    ON aggregated_ticker (pair_name, exchange, timestamp DESC);
