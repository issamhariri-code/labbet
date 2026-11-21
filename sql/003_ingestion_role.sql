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

-- Support a fresh installation where the additive provenance schema came first.
DO $$
BEGIN
    IF pg_catalog.to_regclass('labbet_api.availability_manifests') IS NOT NULL
       AND pg_catalog.to_regclass('labbet_api.request_provenance') IS NOT NULL THEN
        GRANT SELECT, INSERT ON TABLE labbet_api.availability_manifests,
            labbet_api.request_provenance TO labbet_ingestion;
    END IF;
END;
$$;

COMMIT;
