BEGIN;

CREATE ROLE labbet_ingestion WITH
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS;

GRANT CONNECT ON DATABASE labbet TO labbet_ingestion;
GRANT USAGE ON SCHEMA labbet_api TO labbet_ingestion;

GRANT SELECT, INSERT ON TABLE
    labbet_api.ingestion_runs,
    labbet_api.price_revisions,
    labbet_api.current_prices
TO labbet_ingestion;

GRANT UPDATE (revision_id, observed_at)
ON TABLE labbet_api.current_prices
TO labbet_ingestion;

COMMIT;