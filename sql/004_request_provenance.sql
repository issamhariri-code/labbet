-- Additive, transactional migration; apply to deployment only after review.
-- Canonical text preserves the exact bytes covered by the application hashes.
BEGIN;
CREATE TABLE IF NOT EXISTS labbet_api.availability_manifests (
    manifest_hash TEXT PRIMARY KEY CHECK (manifest_hash ~ '^[0-9a-f]{64}$'),
    canonical_document TEXT NOT NULL CHECK (octet_length(canonical_document) BETWEEN 1 AND 4194304),
    CHECK (manifest_hash = pg_catalog.encode(pg_catalog.sha256(
        pg_catalog.convert_to(canonical_document, 'UTF8')), 'hex')),
    CHECK ((jsonb_typeof(canonical_document::jsonb) = 'object'
        AND canonical_document::jsonb ->> 'version' = '1'
        AND jsonb_typeof(canonical_document::jsonb -> 'contract') = 'object'
        AND jsonb_typeof(canonical_document::jsonb -> 'availability') = 'object'
        AND canonical_document::jsonb - ARRAY['version','contract','availability'] = '{}'::jsonb) IS TRUE)
);

CREATE TABLE IF NOT EXISTS labbet_api.request_provenance (
    run_id UUID PRIMARY KEY REFERENCES labbet_api.ingestion_runs(run_id),
    manifest_hash TEXT NOT NULL REFERENCES labbet_api.availability_manifests(manifest_hash),
    provenance_hash TEXT NOT NULL CHECK (provenance_hash ~ '^[0-9a-f]{64}$'),
    canonical_document TEXT NOT NULL CHECK (octet_length(canonical_document) BETWEEN 1 AND 262144),
    CHECK (provenance_hash = pg_catalog.encode(pg_catalog.sha256(
        pg_catalog.convert_to(canonical_document, 'UTF8')), 'hex')),
    CHECK ((jsonb_typeof(canonical_document::jsonb) = 'object'
        AND canonical_document::jsonb ->> 'version' = '1'
        AND canonical_document::jsonb ->> 'manifest_hash' = manifest_hash
        AND canonical_document::jsonb ->> 'profile_config_hash' ~ '^[0-9a-f]{64}$'
        AND jsonb_typeof(canonical_document::jsonb -> 'request') = 'object'
        AND canonical_document::jsonb - ARRAY['version','manifest_hash','profile_config_hash','request'] = '{}'::jsonb) IS TRUE)
);

CREATE OR REPLACE TRIGGER availability_manifests_immutable
    BEFORE UPDATE OR DELETE ON labbet_api.availability_manifests
    FOR EACH ROW EXECUTE FUNCTION labbet_api.reject_history_mutation();
CREATE OR REPLACE TRIGGER request_provenance_immutable
    BEFORE UPDATE OR DELETE ON labbet_api.request_provenance
    FOR EACH ROW EXECUTE FUNCTION labbet_api.reject_history_mutation();

REVOKE ALL ON TABLE labbet_api.availability_manifests, labbet_api.request_provenance FROM PUBLIC;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'labbet_ingestion') THEN
        GRANT SELECT, INSERT ON TABLE labbet_api.availability_manifests,
            labbet_api.request_provenance TO labbet_ingestion;
    END IF;
END;
$$;
COMMIT;
