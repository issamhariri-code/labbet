"""Provenance permissions using the real restricted role in an ephemeral cluster.

Requires test_ingestion_role's private socket and matching server run marker.
Run after schema 004; the harness separately exercises both installation orders.
"""
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import unittest
from uuid import uuid4

import psycopg

from api_availability import plan_daily_request
from api_contracts import ApiFailure, SessionAvailability, SourceAvailability
from api_provenance import RequestProvenance
from api_storage import persist_batch, read_request_provenance
from test_api_client import NOW, contract
from test_ingestion_role import connect


ROOT = Path(__file__).resolve().parents[1]
TABLES = ("availability_manifests", "request_provenance")


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class RequestProvenanceRoleTests(unittest.TestCase):
    def setUp(self):
        # connect refuses any server lacking the explicit ephemeral marker/socket.
        self.connection = connect()
        self.admin = connect("postgres")
        self.addCleanup(self.connection.close)
        self.addCleanup(self.admin.close)
        self.legacy_before = self.admin.execute(
            "SELECT * FROM public.daily_prices ORDER BY symbol,trade_date"
        ).fetchall()
        self.c = contract()
        self.source = "role-provenance-" + uuid4().hex
        schedule = SourceAvailability(self.source, "role-fixture-v1", self.c.fingerprint,
            tuple(SessionAvailability(day, close) for day, close in self.c.sessions))
        self.provenance = RequestProvenance(self.c, schedule, plan_daily_request(self.c, NOW), "c" * 64)
        self.args = dict(source_id=self.source, instrument_id=self.c.instrument_id,
            symbol=self.c.symbol, request_id="role-request", observed_at=dict(self.c.sessions)[
                self.provenance.request.sessions[-1]], received_at=NOW,
            config_version=self.provenance.config_version,
            payload_hash=digest("synthetic role provenance"),
            metadata={"currency": self.c.currency, "price_basis": self.c.price_basis,
                "schema_version": self.c.schema_version, "market_timezone": self.c.timezone,
                "market": self.c.exchange},
            rows=[(self.c.symbol, day, Decimal("10"), Decimal("11"), Decimal("9"),
                   Decimal("10.5"), 100) for day in self.provenance.request.sessions],
            provenance=self.provenance)

    def tearDown(self):
        self.assertEqual(self.admin.execute(
            "SELECT * FROM public.daily_prices ORDER BY symbol,trade_date"
        ).fetchall(), self.legacy_before)

    def save(self, **changes):
        values = dict(self.args)
        values.update(changes)
        return persist_batch(self.connection, **values)

    def read(self, request_id="role-request"):
        return read_request_provenance(self.connection, source_id=self.source, request_id=request_id)

    def reject_sql(self, query, parameters=None, *, code="42501", connection=None):
        connection = self.connection if connection is None else connection
        with self.assertRaises(psycopg.Error) as caught:
            with connection.transaction(force_rollback=True):
                connection.execute(query, parameters)
        self.assertEqual(caught.exception.sqlstate, code)

    def insert_record(self, run_id, manifest_hash=None, document=None, fingerprint=None):
        document = self.provenance.request_document if document is None else document
        return ("""INSERT INTO labbet_api.request_provenance
            (run_id,manifest_hash,provenance_hash,canonical_document) VALUES (%s,%s,%s,%s)""",
            (run_id, self.provenance.manifest_hash if manifest_hash is None else manifest_hash,
             digest(document) if fingerprint is None else fingerprint, document))

    def test_restricted_role_persists_replays_and_reads_complete_request_evidence(self):
        saved = self.save()
        replay = self.save()
        self.assertFalse(saved.replayed)
        self.assertTrue(replay.replayed)
        self.assertEqual(saved.run_id, replay.run_id)
        restored = self.read()
        self.assertEqual(restored.manifest_document, self.provenance.manifest_document)
        self.assertEqual(restored.request_document, self.provenance.request_document)
        self.assertEqual(self.connection.execute(
            "SELECT session_user,current_user"
        ).fetchone(), ("labbet_ingestion", "labbet_ingestion"))
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM labbet_api.request_provenance WHERE run_id=%s",
            (saved.run_id,)).fetchone()[0], 1)

    def test_new_tables_allow_only_select_insert_and_are_owned_by_admin(self):
        for table in TABLES:
            with self.subTest(table=table):
                relation = "labbet_api." + table
                for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE",
                                  "REFERENCES", "TRIGGER", "MAINTAIN"):
                    actual = self.connection.execute(
                        "SELECT has_table_privilege(current_user,%s,%s)",
                        (relation, privilege)).fetchone()[0]
                    self.assertEqual(actual, privilege in ("SELECT", "INSERT"), privilege)
                owner, public_acl = self.admin.execute("""SELECT pg_get_userbyid(c.relowner),
                    EXISTS (SELECT 1 FROM aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner)))
                            WHERE grantee=0)
                    FROM pg_class c WHERE c.oid=%s::regclass""", (relation,)).fetchone()
                self.assertEqual(owner, "postgres")
                self.assertFalse(public_acl)

    def test_restricted_role_cannot_update_delete_or_truncate_archived_evidence(self):
        saved = self.save()
        for table, key, value in (
            ("availability_manifests", "manifest_hash", self.provenance.manifest_hash),
            ("request_provenance", "run_id", saved.run_id),
        ):
            for operation in ("UPDATE", "DELETE", "TRUNCATE"):
                with self.subTest(table=table, operation=operation):
                    query = (f"UPDATE labbet_api.{table} SET canonical_document=canonical_document WHERE {key}=%s"
                             if operation == "UPDATE" else
                             f"DELETE FROM labbet_api.{table} WHERE {key}=%s"
                             if operation == "DELETE" else f"TRUNCATE labbet_api.{table}")
                    self.reject_sql(query, None if operation == "TRUNCATE" else (value,))
        self.assertEqual(self.read().fingerprint, self.provenance.fingerprint)

    def test_schema_escalation_public_grants_and_legacy_access_are_denied(self):
        queries = [
            "ALTER TABLE labbet_api.request_provenance ADD COLUMN forbidden integer",
            "DROP TABLE labbet_api.request_provenance",
            "CREATE TABLE labbet_api.forbidden_provenance(value integer)",
            "GRANT SELECT ON public.daily_prices TO PUBLIC",
            "SET ROLE postgres",
            "SELECT * FROM public.daily_prices",
        ]
        for query in queries:
            with self.subTest(query=query):
                self.reject_sql(query)

    def test_database_rejects_wrong_hashes_from_restricted_writer_without_application(self):
        self.save()
        legacy = self.save(request_id="unarchived-slot", provenance=None)
        self.reject_sql("""INSERT INTO labbet_api.availability_manifests
            (manifest_hash,canonical_document) VALUES (%s,%s)""",
            ("0" * 64, self.provenance.manifest_document), code="23514")
        query, parameters = self.insert_record(legacy.run_id, fingerprint="0" * 64)
        self.reject_sql(query, parameters, code="23514")
        self.assertIsNone(self.read("unarchived-slot"))

    def test_database_rejects_missing_run_and_missing_manifest_references(self):
        self.save()
        query, parameters = self.insert_record(str(uuid4()))
        self.reject_sql(query, parameters, code="23503")
        legacy = self.save(request_id="orphan-manifest-slot", provenance=None)
        document = json.loads(self.provenance.request_document)
        document["manifest_hash"] = "d" * 64
        text = json.dumps(document, sort_keys=True, separators=(",", ":"))
        query, parameters = self.insert_record(legacy.run_id, manifest_hash="d" * 64, document=text)
        self.reject_sql(query, parameters, code="23503")
        self.assertIsNone(self.read("orphan-manifest-slot"))

    def test_manifest_column_must_match_hashed_request_document(self):
        self.save()
        legacy = self.save(request_id="binding-slot", provenance=None)
        document = json.loads(self.provenance.request_document)
        document["manifest_hash"] = "e" * 64
        text = json.dumps(document, sort_keys=True, separators=(",", ":"))
        query, parameters = self.insert_record(legacy.run_id, document=text)
        self.reject_sql(query, parameters, code="23514")
        self.assertIsNone(self.read("binding-slot"))

    def test_direct_semantic_forgery_is_detected_by_independent_read_audit(self):
        # A role allowed to INSERT can satisfy SQL shapes while lying about a run.
        # The batch binding must detect that; SQL shape checks are not the full audit.
        with self.connection.transaction(force_rollback=True):
            legacy = self.save(request_id="forged-slot", provenance=None)
            self.connection.execute("""INSERT INTO labbet_api.availability_manifests
                (manifest_hash,canonical_document) VALUES (%s,%s)""",
                (self.provenance.manifest_hash, self.provenance.manifest_document))
            query, parameters = self.insert_record(legacy.run_id)
            self.connection.execute(query, parameters)
            with self.assertRaises(ApiFailure) as caught:
                self.read("forged-slot")
            self.assertEqual(caught.exception.code, "provenance_integrity")
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM labbet_api.ingestion_runs WHERE source_id=%s",
            (self.source,)).fetchone()[0], 0)

    def test_owner_triggers_still_block_update_and_delete_for_both_new_histories(self):
        saved = self.save()
        for table, key, value in (
            ("availability_manifests", "manifest_hash", self.provenance.manifest_hash),
            ("request_provenance", "run_id", saved.run_id),
        ):
            for operation in ("UPDATE", "DELETE"):
                with self.subTest(table=table, operation=operation):
                    query = (f"UPDATE labbet_api.{table} SET canonical_document=canonical_document WHERE {key}=%s"
                             if operation == "UPDATE" else f"DELETE FROM labbet_api.{table} WHERE {key}=%s")
                    self.reject_sql(query, (value,), code="55000", connection=self.admin)
        self.assertEqual(self.read().fingerprint, self.provenance.fingerprint)

    def test_migration_reapplication_preserves_documents_permissions_and_rows(self):
        self.save()
        before = self.read().to_json()
        privileges = self.connection.execute("""SELECT c.relname,c.relacl::text
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='labbet_api' AND c.relname IN ('availability_manifests','request_provenance')
            ORDER BY c.relname""").fetchall()
        self.admin.execute((ROOT / "sql/004_request_provenance.sql").read_text(), prepare=False)
        self.assertEqual(self.read().to_json(), before)
        self.assertEqual(self.connection.execute("""SELECT c.relname,c.relacl::text
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='labbet_api' AND c.relname IN ('availability_manifests','request_provenance')
            ORDER BY c.relname""").fetchall(), privileges)

    def test_failed_migration_rolls_back_new_table_and_preserves_archived_data(self):
        self.save()
        before = self.read().fingerprint
        old_name = "request_provenance_saved_" + uuid4().hex[:10]
        function_name = "reject_history_saved_" + uuid4().hex[:10]
        self.admin.execute(f"ALTER TABLE labbet_api.request_provenance RENAME TO {old_name}")
        try:
            self.admin.execute(f"ALTER FUNCTION labbet_api.reject_history_mutation() RENAME TO {function_name}")
            try:
                with self.assertRaises(psycopg.errors.UndefinedFunction) as caught:
                    self.admin.execute((ROOT / "sql/004_request_provenance.sql").read_text(), prepare=False)
                self.assertEqual(caught.exception.sqlstate, "42883")
                self.admin.rollback()
                self.assertIsNone(self.admin.execute(
                    "SELECT to_regclass('labbet_api.request_provenance')"
                ).fetchone()[0])
            finally:
                self.admin.rollback()
                self.admin.execute(f"ALTER FUNCTION labbet_api.{function_name}() RENAME TO reject_history_mutation")
        finally:
            self.admin.rollback()
            self.admin.execute(f"ALTER TABLE labbet_api.{old_name} RENAME TO request_provenance")
        self.assertEqual(self.read().fingerprint, before)

