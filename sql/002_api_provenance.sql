-- Additive migration. Apply only through a separately authorized deployment.
-- It neither reads nor changes existing daily_prices or its data.
BEGIN;
CREATE SCHEMA IF NOT EXISTS labbet_api;

CREATE TABLE IF NOT EXISTS labbet_api.ingestion_runs (
    run_id UUID PRIMARY KEY,
    source_id TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    request_id TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    config_version TEXT NOT NULL,
    payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    batch_hash TEXT NOT NULL CHECK (batch_hash ~ '^[0-9a-f]{64}$'),
    contract_hash TEXT NOT NULL CHECK (contract_hash ~ '^[0-9a-f]{64}$'),
    contract_metadata JSONB NOT NULL CHECK (jsonb_typeof(contract_metadata) = 'object'),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 100000),
    UNIQUE (source_id, request_id),
    UNIQUE (run_id, source_id, instrument_id, contract_hash, observed_at),
    CHECK (observed_at <= received_at)
);

CREATE TABLE IF NOT EXISTS labbet_api.price_revisions (
    revision_id UUID PRIMARY KEY,
    run_id UUID NOT NULL,
    source_id TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    contract_hash TEXT NOT NULL,
    trade_date DATE NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(18, 6) NOT NULL,
    high_price NUMERIC(18, 6) NOT NULL,
    low_price NUMERIC(18, 6) NOT NULL,
    close_price NUMERIC(18, 6) NOT NULL,
    volume BIGINT NOT NULL CHECK (volume >= 0),
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    UNIQUE (run_id, trade_date),
    UNIQUE (revision_id, source_id, instrument_id, contract_hash, trade_date, observed_at),
    FOREIGN KEY (run_id, source_id, instrument_id, contract_hash, observed_at)
        REFERENCES labbet_api.ingestion_runs
        (run_id, source_id, instrument_id, contract_hash, observed_at),
    CHECK (low_price > 0 AND high_price >= low_price),
    CHECK (open_price BETWEEN low_price AND high_price),
    CHECK (close_price BETWEEN low_price AND high_price)
);

CREATE INDEX IF NOT EXISTS price_revisions_observation
    ON labbet_api.price_revisions
    (source_id, instrument_id, contract_hash, trade_date, observed_at);

CREATE TABLE IF NOT EXISTS labbet_api.current_prices (
    source_id TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    contract_hash TEXT NOT NULL,
    trade_date DATE NOT NULL,
    revision_id UUID NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_id, instrument_id, contract_hash, trade_date),
    FOREIGN KEY (revision_id, source_id, instrument_id, contract_hash, trade_date, observed_at)
        REFERENCES labbet_api.price_revisions
        (revision_id, source_id, instrument_id, contract_hash, trade_date, observed_at)
);

CREATE OR REPLACE FUNCTION labbet_api.reject_history_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'API observation history is append-only' USING ERRCODE = '55000';
END;
$$;

CREATE OR REPLACE TRIGGER ingestion_runs_immutable
    BEFORE UPDATE OR DELETE ON labbet_api.ingestion_runs
    FOR EACH ROW EXECUTE FUNCTION labbet_api.reject_history_mutation();
CREATE OR REPLACE TRIGGER price_revisions_immutable
    BEFORE UPDATE OR DELETE ON labbet_api.price_revisions
    FOR EACH ROW EXECUTE FUNCTION labbet_api.reject_history_mutation();
COMMIT;
