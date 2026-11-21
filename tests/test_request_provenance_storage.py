"""Durable request evidence against disposable PostgreSQL, never production.

Requires an explicit labbet_api_test database. Every method owns a unique
source. Deliberate corruption is confined to transactions that always roll back.
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import FrozenInstanceError, replace
from datetime import date, timedelta
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import threading
import unittest
from unittest.mock import patch
from uuid import uuid4

import api_adapters
from api_availability import plan_daily_request
from api_contracts import ApiFailure
from api_provenance import RequestProvenance
from api_storage import persist_batch, read_request_provenance
from test_api_client import NOW, contract, profile
from test_availability_integration import RULE, schedule


ROOT = Path(__file__).resolve().parents[1]


class _RollbackProbe(Exception):
    pass


def _digest(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class RequestProvenancePostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dsn = os.environ.get("LABBET_TEST_DSN")
        if not cls.dsn:
            raise unittest.SkipTest("LABBET_TEST_DSN not set; real PostgreSQL tests not run")
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        if conninfo_to_dict(cls.dsn).get("dbname") != "labbet_api_test":
            raise RuntimeError("Refusing database tests: explicit labbet_api_test database required")
        cls.psycopg = psycopg
        candidates = sorted((ROOT / "sql").glob("004*.sql"))
        if len(candidates) != 1:
            raise RuntimeError("Exactly one request provenance migration 004 is required")
        cls.migration = candidates[0].read_text(encoding="utf-8")
        with cls.connect() as connection:
            if connection.info.dbname != "labbet_api_test":
                raise RuntimeError("Refusing database tests: connected database is not labbet_api_test")
            connection.execute((ROOT / "sql" / "002_api_provenance.sql").read_text(encoding="utf-8"))
            connection.execute(cls.migration)

    @classmethod
    def connect(cls, **options):
        return cls.psycopg.connect(cls.dsn, autocommit=True, connect_timeout=5, **options)

    def setUp(self):
        self.connection = self.connect()
        self.addCleanup(self.connection.close)
        self.p = profile(source_id="provenance-test-" + uuid4().hex,
                         availability_rule_version=RULE)
        self.c = contract()
        self.provenance = RequestProvenance(self.c, schedule(self.p, self.c),
            plan_daily_request(self.c, NOW), self.p.config_hash)
        self.args = self.arguments(self.provenance)

    @staticmethod
    def arguments(provenance):
        selected = provenance.contract
        return dict(source_id=provenance.availability.source_id,
            instrument_id=selected.instrument_id, symbol=selected.symbol,
            request_id="request-1", observed_at=dict(selected.sessions)[date(2026, 9, 30)],
            received_at=NOW, config_version=provenance.config_version,
            payload_hash=hashlib.sha256(b"synthetic provenance storage fixture").hexdigest(),
            metadata={"currency": selected.currency, "price_basis": selected.price_basis,
                      "schema_version": selected.schema_version,
                      "market_timezone": selected.timezone, "market": selected.exchange},
            rows=[("TEST", date(2026, 9, 30), Decimal("10.00"), Decimal("11.00"),
                   Decimal("9.00"), Decimal("10.50"), 1000)], provenance=provenance)

    def save(self, connection=None, **changes):
        args = dict(self.args)
        args.update(changes)
        return persist_batch(self.connection if connection is None else connection, **args)

    def read(self, connection=None, **changes):
        args = dict(source_id=self.args["source_id"], request_id=self.args["request_id"])
        args.update(changes)
        return read_request_provenance(self.connection if connection is None else connection, **args)

    def counts(self, connection=None):
        connection = self.connection if connection is None else connection
        source = self.args["source_id"]
        counts = [connection.execute(
            "SELECT count(*) FROM labbet_api." + table + " WHERE source_id=%s", (source,)
        ).fetchone()[0] for table in ("ingestion_runs", "price_revisions", "current_prices")]
        counts.append(connection.execute("""
            SELECT count(*) FROM labbet_api.request_provenance p
            JOIN labbet_api.ingestion_runs r USING(run_id) WHERE r.source_id=%s
        """, (source,)).fetchone()[0])
        return tuple(counts)

    def manifest_count(self, fingerprint=None, connection=None):
        connection = self.connection if connection is None else connection
        return connection.execute("""
            SELECT count(*) FROM labbet_api.availability_manifests WHERE manifest_hash=%s
        """, (self.provenance.manifest_hash if fingerprint is None else fingerprint,)).fetchone()[0]

    def assert_code(self, code, operation, *args, **kwargs):
        with self.assertRaises(ApiFailure) as caught:
            operation(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)
        self.assertIsNone(caught.exception.status)

    def interrupted(self, table, *, disconnect=False):
        actual = self.connection
        marker = "INSERT INTO labbet_api." + table
        class CursorProxy:
            def __init__(self):
                self.cursor = actual.cursor()
            def __enter__(self):
                self.cursor.__enter__()
                return self
            def __exit__(self, *args):
                return self.cursor.__exit__(*args)
            def execute(self, query, params=None):
                result = self.cursor.execute(query, params)
                if marker in " ".join(str(query).split()):
                    if disconnect:
                        actual.close()
                    raise RuntimeError("synthetic interruption after successful insert")
                return result
            def __getattr__(self, name):
                return getattr(self.cursor, name)
        class ConnectionProxy:
            def transaction(self):
                return actual.transaction()
            def cursor(self):
                return CursorProxy()
        return ConnectionProxy()

    @contextmanager
    def corrupt_transaction(self, table):
        # The test database guard is checked before any connection is established.
        # DDL and all deliberately invalid row changes are undone, including on failure.
        try:
            with self.connection.transaction():
                self.connection.execute("ALTER TABLE labbet_api." + table + " DISABLE TRIGGER USER")
                yield
                raise _RollbackProbe()
        except _RollbackProbe:
            pass

    def insert_manifest(self, provenance=None):
        selected = self.provenance if provenance is None else provenance
        self.connection.execute("""
            INSERT INTO labbet_api.availability_manifests(manifest_hash, canonical_document)
            VALUES (%s,%s) ON CONFLICT DO NOTHING
        """, (selected.manifest_hash, selected.manifest_document))

    def test_full_contract_schedule_and_request_roundtrip_as_exact_documents(self):
        result = self.save()
        stored = self.connection.execute("""
            SELECT p.provenance_hash,p.canonical_document,m.manifest_hash,m.canonical_document
            FROM labbet_api.request_provenance p
            JOIN labbet_api.availability_manifests m USING(manifest_hash) WHERE p.run_id=%s
        """, (result.run_id,)).fetchone()
        self.assertEqual(stored, (self.provenance.fingerprint, self.provenance.request_document,
            self.provenance.manifest_hash, self.provenance.manifest_document))
        restored = self.read()
        self.assertEqual(restored.to_json(), self.provenance.to_json())
        self.assertEqual(restored.contract.fingerprint, self.c.fingerprint)
        self.assertEqual(len(restored.availability.entries), 4)
        self.assertEqual(restored.availability.entries[-1].session_date, date(2026, 10, 2))
        self.assertEqual(self.counts(), (1, 1, 1, 1))

    def test_original_contract_alias_cannot_rewrite_captured_provenance(self):
        expected_manifest = self.provenance.manifest_document
        expected_request = self.provenance.request_document
        expected_fingerprint = self.provenance.fingerprint
        self.assertIsNot(self.c, self.provenance.contract)
        # DailyContract predates slots; exercise its actual mutable dictionary,
        # rather than bypassing the frozen RequestProvenance with object.__setattr__.
        self.c.__dict__["calendar_version"] = "caller-changed-after-capture"
        self.c.__dict__["currency"] = "EUR"
        self.assertEqual(self.provenance.manifest_document, expected_manifest)
        self.save()
        restored = self.read()
        self.assertEqual(restored.manifest_document, expected_manifest)
        self.assertEqual(restored.request_document, expected_request)
        self.assertEqual(restored.fingerprint, expected_fingerprint)
        self.assertEqual(restored.contract.currency, "USD")

    def test_transaction_entry_mutation_cannot_change_prevalidated_archive_bytes(self):
        expected_manifest = self.provenance.manifest_document
        expected_request = self.provenance.request_document
        expected_manifest_hash = self.provenance.manifest_hash
        expected_fingerprint = self.provenance.fingerprint
        actual, supplied = self.connection, self.provenance
        mutations = []
        class MutatingConnection:
            def transaction(self):
                mutations.append("transaction-entered")
                supplied.contract.__dict__["calendar_version"] = "mutated-during-sql-entry"
                supplied.contract.__dict__["currency"] = "EUR"
                return actual.transaction()
            def cursor(self):
                return actual.cursor()
        self.save(MutatingConnection())
        self.assertEqual(mutations, ["transaction-entered"])
        self.assertNotEqual(supplied.manifest_document, expected_manifest)
        restored = self.read()
        self.assertEqual(restored.manifest_document, expected_manifest)
        self.assertEqual(restored.request_document, expected_request)
        self.assertEqual(restored.fingerprint, expected_fingerprint)
        self.assertEqual(self.manifest_count(expected_manifest_hash), 1)
        self.assertEqual(self.counts(), (1, 1, 1, 1))

    def test_multiple_requests_reuse_one_manifest_without_splitting_price_stream(self):
        first = self.save()
        second = self.save(request_id="request-2")
        self.assertNotEqual(first.run_id, second.run_id)
        self.assertEqual(self.manifest_count(), 1)
        self.assertEqual(self.counts(), (2, 2, 1, 2))
        self.assertEqual(self.read(request_id="request-2").manifest_hash, self.provenance.manifest_hash)
        self.assertEqual(self.connection.execute("""
            SELECT count(DISTINCT contract_hash) FROM labbet_api.ingestion_runs WHERE source_id=%s
        """, (self.p.source_id,)).fetchone()[0], 1)

    def test_profile_hash_change_reuses_manifest_and_records_new_config_binding(self):
        self.save()
        changed = replace(self.provenance, profile_config_hash="c" * 64)
        self.assertEqual(changed.manifest_hash, self.provenance.manifest_hash)
        self.save(request_id="profile-v2", provenance=changed, config_version=changed.config_version)
        restored = self.read(request_id="profile-v2")
        self.assertEqual(restored.profile_config_hash, "c" * 64)
        self.assertNotEqual(restored.config_version, self.provenance.config_version)
        self.assertEqual(self.manifest_count(), 1)
        self.assertEqual(self.counts(), (2, 2, 1, 2))

    def test_schedule_revision_keeps_old_manifest_and_same_price_stream(self):
        self.save()
        changed = replace(self.provenance, availability=schedule(self.p, self.c,
            overrides={date(2026, 9, 29): NOW}))
        self.save(request_id="schedule-v2", provenance=changed, config_version=changed.config_version)
        self.assertEqual((self.manifest_count(), self.manifest_count(changed.manifest_hash)), (1, 1))
        self.assertEqual(self.read().fingerprint, self.provenance.fingerprint)
        self.assertEqual(self.read(request_id="schedule-v2").fingerprint, changed.fingerprint)
        self.assertEqual(self.counts(), (2, 2, 1, 2))

    def test_identical_replay_reuses_original_run_and_provenance(self):
        first, second = self.save(), self.save()
        self.assertEqual(first.run_id, second.run_id)
        self.assertTrue(second.replayed)
        self.assertEqual((second.inserted_revisions, second.advanced_current), (0, 0))
        self.assertEqual(self.counts(), (1, 1, 1, 1))
        self.assertEqual(self.manifest_count(), 1)

    def test_reordered_rows_replay_with_identical_evidence(self):
        current = self.args["rows"][0]
        rows = [current, (current[0], date(2026, 9, 29), *current[2:])]
        first = self.save(rows=rows)
        second = self.save(rows=list(reversed(rows)))
        self.assertEqual(first.run_id, second.run_id)
        self.assertTrue(second.replayed)
        self.assertEqual(self.read().fingerprint, self.provenance.fingerprint)
        self.assertEqual(self.counts(), (1, 2, 2, 1))

    def test_changed_reference_time_cannot_reuse_existing_request_id(self):
        received = NOW + timedelta(seconds=2)
        self.save(received_at=received)
        changed = replace(self.provenance,
            request=plan_daily_request(self.c, NOW + timedelta(seconds=1)))
        with self.assertRaisesRegex(ValueError, "request_id"):
            self.save(received_at=received, provenance=changed)
        self.assertEqual(self.read().fingerprint, self.provenance.fingerprint)
        self.assertEqual(self.counts(), (1, 1, 1, 1))

    def test_adding_or_removing_provenance_cannot_reuse_existing_request_id(self):
        self.save()
        with self.assertRaisesRegex(ValueError, "request_id"):
            self.save(provenance=None)
        self.save(request_id="legacy", provenance=None)
        with self.assertRaisesRegex(ValueError, "request_id"):
            self.save(request_id="legacy")
        self.assertEqual(self.counts(), (2, 2, 1, 1))
        self.assertIsNone(self.read(request_id="legacy"))

    def test_invalid_bindings_are_rejected_before_connection_or_transaction_use(self):
        class ForbiddenConnection:
            def __getattribute__(self, name):
                raise AssertionError("database interaction forbidden")
        for changes in ({"source_id": "another-source"}, {"instrument_id": "OTHER"},
                        {"config_version": "f" * 64},
                        {"metadata": dict(self.args["metadata"], currency="EUR")}):
            with self.subTest(field=tuple(changes)):
                self.assert_code("provenance_binding", self.save, ForbiddenConnection(), **changes)
        self.assertEqual(self.counts(), (0, 0, 0, 0))
        self.assertEqual(self.manifest_count(), 0)

    def test_missing_requested_row_is_rejected_before_transaction(self):
        row = self.args["rows"][0]
        self.assert_code("incomplete_series", self.save,
            rows=[(row[0], date(2026, 9, 29), *row[2:])])
        self.assertEqual(self.counts(), (0, 0, 0, 0))

    def test_legacy_batch_hash_and_optional_none_keep_original_representation(self):
        result = self.save(provenance=None)
        # Independent legacy canonical payload: provenance MUST be absent, not null.
        identity = {"source": self.p.source_id, "instrument": self.c.instrument_id,
            "symbol": "TEST", "request": "request-1",
            "observed": self.args["observed_at"].isoformat(), "received": NOW.isoformat(),
            "config": self.args["config_version"], "payload": self.args["payload_hash"],
            "contract": self.args["metadata"],
            "rows": [["2026-09-30", "10.000000", "11.000000", "9.000000", "10.500000", 1000]]}
        expected = _digest(json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=True))
        actual = self.connection.execute("SELECT batch_hash FROM labbet_api.ingestion_runs WHERE run_id=%s",
                                         (result.run_id,)).fetchone()[0]
        self.assertEqual(actual, expected)
        self.assertTrue(self.save(provenance=None).replayed)
        self.assertIsNone(self.read())
        self.assertEqual(self.manifest_count(), 0)

    def test_unknown_request_and_wrong_source_do_not_disclose_other_history(self):
        self.assert_code("unknown_request", self.read)
        self.save()
        self.assert_code("unknown_request", self.read, source_id="unrelated-source")
        self.assert_code("unknown_request", self.read, request_id="unknown-request")
        self.assertEqual(self.read().availability.source_id, self.p.source_id)

    def test_readback_is_frozen_and_survives_current_rule_registry_removal(self):
        self.save()
        with patch.object(api_adapters, "_DAILY_ADAPTERS", {}):
            restored = self.read()
        self.assertEqual(restored.fingerprint, self.provenance.fingerprint)
        with self.assertRaises(FrozenInstanceError):
            restored.profile_config_hash = "0" * 64
        detached = restored.to_json()
        detached["request"]["profile_config_hash"] = "0" * 64
        self.assertEqual(self.read().fingerprint, self.provenance.fingerprint)

    def test_audit_is_independent_of_database_session_timezone(self):
        self.save()
        self.save(request_id="legacy", provenance=None)
        for zone in ("America/New_York", "Europe/Stockholm", "Asia/Kathmandu"):
            with self.subTest(zone=zone):
                self.connection.execute("SELECT pg_catalog.set_config('TimeZone',%s,false)", (zone,))
                self.assertEqual(self.read().fingerprint, self.provenance.fingerprint)
                self.assertIsNone(self.read(request_id="legacy"))

    def test_concurrent_identical_replay_creates_one_run_and_one_archive(self):
        barrier = threading.Barrier(2)
        def writer():
            with self.connect() as connection:
                barrier.wait(timeout=5)
                return self.save(connection)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(writer) for _ in range(2)]
            results = [future.result(timeout=15) for future in futures]
        self.assertEqual(sorted(result.replayed for result in results), [False, True])
        self.assertEqual(results[0].run_id, results[1].run_id)
        self.assertEqual(self.counts(), (1, 1, 1, 1))
        self.assertEqual(self.manifest_count(), 1)

    def test_concurrent_distinct_requests_share_manifest_and_stream(self):
        barrier = threading.Barrier(2)
        def writer(request_id):
            with self.connect() as connection:
                barrier.wait(timeout=5)
                return self.save(connection, request_id=request_id)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(writer, "request-" + str(index)) for index in (1, 2)]
            results = [future.result(timeout=15) for future in futures]
        self.assertTrue(all(not result.replayed for result in results))
        self.assertEqual(self.counts(), (2, 2, 1, 2))
        self.assertEqual(self.manifest_count(), 1)

    def test_interruption_after_archive_insert_rolls_back_new_manifest_and_run(self):
        with self.assertRaisesRegex(RuntimeError, "synthetic interruption"):
            self.save(self.interrupted("request_provenance"))
        self.assertEqual(self.counts(), (0, 0, 0, 0))
        self.assertEqual(self.manifest_count(), 0)
        self.assertFalse(self.save().replayed)

    def test_interruption_after_revision_insert_rolls_back_entire_new_batch(self):
        with self.assertRaisesRegex(RuntimeError, "synthetic interruption"):
            self.save(self.interrupted("price_revisions"))
        self.assertEqual(self.counts(), (0, 0, 0, 0))
        self.assertEqual(self.manifest_count(), 0)

    def test_failed_second_run_preserves_existing_shared_manifest(self):
        self.save()
        with self.assertRaisesRegex(RuntimeError, "synthetic interruption"):
            self.save(self.interrupted("request_provenance"), request_id="failed-second")
        self.assertEqual(self.counts(), (1, 1, 1, 1))
        self.assertEqual(self.manifest_count(), 1)
        self.assertEqual(self.read().fingerprint, self.provenance.fingerprint)

    def test_connection_loss_rolls_back_provenance_and_can_recover(self):
        with self.assertRaises(Exception):
            self.save(self.interrupted("request_provenance", disconnect=True))
        with self.connect() as observer:
            self.assertEqual(self.counts(observer), (0, 0, 0, 0))
            self.assertEqual(self.manifest_count(connection=observer), 0)
            self.save(observer)
            self.assertEqual(self.read(observer).fingerprint, self.provenance.fingerprint)

    def test_outer_transaction_keeps_commit_ownership_for_all_new_tables(self):
        with self.assertRaises(_RollbackProbe):
            with self.connection.transaction():
                self.save()
                with self.connect() as observer:
                    self.assertEqual(self.counts(observer), (0, 0, 0, 0))
                    self.assertEqual(self.manifest_count(connection=observer), 0)
                raise _RollbackProbe()
        self.assertEqual(self.counts(), (0, 0, 0, 0))
        self.assertEqual(self.manifest_count(), 0)

    def test_both_history_tables_reject_update_delete_even_as_test_owner(self):
        result = self.save()
        queries = (
            ("UPDATE labbet_api.availability_manifests SET canonical_document=canonical_document WHERE manifest_hash=%s", self.provenance.manifest_hash),
            ("DELETE FROM labbet_api.availability_manifests WHERE manifest_hash=%s", self.provenance.manifest_hash),
            ("UPDATE labbet_api.request_provenance SET canonical_document=canonical_document WHERE run_id=%s", result.run_id),
            ("DELETE FROM labbet_api.request_provenance WHERE run_id=%s", result.run_id),
        )
        for query, identity in queries:
            with self.subTest(query=query), self.assertRaises(self.psycopg.errors.ObjectNotInPrerequisiteState):
                self.connection.execute(query, (identity,))
        self.assertEqual(self.counts(), (1, 1, 1, 1))
        self.assertEqual(self.manifest_count(), 1)

    def test_database_checks_manifest_and_request_hashes_without_application(self):
        with self.assertRaises(self.psycopg.errors.CheckViolation):
            self.connection.execute("""
                INSERT INTO labbet_api.availability_manifests(manifest_hash,canonical_document)
                VALUES(%s,%s)
            """, ("0" * 64, self.provenance.manifest_document))
        result = self.save(provenance=None)
        self.insert_manifest()
        with self.assertRaises(self.psycopg.errors.CheckViolation):
            self.connection.execute("""
                INSERT INTO labbet_api.request_provenance
                    (run_id,manifest_hash,provenance_hash,canonical_document) VALUES(%s,%s,%s,%s)
            """, (result.run_id, self.provenance.manifest_hash, "0" * 64,
                  self.provenance.request_document))
        self.assertEqual(self.counts(), (1, 1, 1, 0))

    def test_database_rejects_orphan_run_reference(self):
        self.insert_manifest()
        with self.assertRaises(self.psycopg.errors.ForeignKeyViolation):
            self.connection.execute("""
                INSERT INTO labbet_api.request_provenance
                    (run_id,manifest_hash,provenance_hash,canonical_document) VALUES(%s,%s,%s,%s)
            """, (str(uuid4()), self.provenance.manifest_hash,
                  self.provenance.fingerprint, self.provenance.request_document))
        self.assertEqual(self.counts(), (0, 0, 0, 0))

    def test_database_binds_manifest_column_to_manifest_in_request_document(self):
        result = self.save(provenance=None)
        different = replace(self.provenance, availability=schedule(self.p, self.c,
            overrides={date(2026, 9, 29): NOW}))
        self.insert_manifest()
        self.insert_manifest(different)
        with self.assertRaises(self.psycopg.errors.CheckViolation):
            self.connection.execute("""
                INSERT INTO labbet_api.request_provenance
                    (run_id,manifest_hash,provenance_hash,canonical_document) VALUES(%s,%s,%s,%s)
            """, (result.run_id, different.manifest_hash,
                  self.provenance.fingerprint, self.provenance.request_document))
        self.assertEqual(self.counts(), (1, 1, 1, 0))

    def test_migration_reapplication_preserves_legacy_and_enabled_history(self):
        self.save()
        self.save(request_id="legacy", provenance=None)
        before = self.counts()
        self.connection.execute(self.migration)
        self.connection.execute(self.migration)
        self.assertEqual(self.counts(), before)
        self.assertEqual(self.manifest_count(), 1)
        self.assertEqual(self.read().fingerprint, self.provenance.fingerprint)
        self.assertIsNone(self.read(request_id="legacy"))

    def test_self_consistent_archive_tamper_is_rejected_by_stored_batch_binding(self):
        result = self.save()
        changed = replace(self.provenance,
            request=plan_daily_request(self.c, NOW - timedelta(seconds=1)))
        with self.corrupt_transaction("request_provenance"):
            self.connection.execute("""
                UPDATE labbet_api.request_provenance
                SET canonical_document=%s,provenance_hash=%s WHERE run_id=%s
            """, (changed.request_document, changed.fingerprint, result.run_id))
            self.assert_code("provenance_integrity", self.read)
        self.assertEqual(self.read().fingerprint, self.provenance.fingerprint)

    def test_missing_archive_is_integrity_failure_not_silent_legacy_downgrade(self):
        result = self.save()
        with self.corrupt_transaction("request_provenance"):
            self.connection.execute("DELETE FROM labbet_api.request_provenance WHERE run_id=%s",
                                     (result.run_id,))
            self.assert_code("provenance_integrity", self.read)
        self.assertEqual(self.read().fingerprint, self.provenance.fingerprint)

    def test_readback_detects_revision_hash_changed_values_and_wrong_run_rowcount(self):
        result = self.save()
        new_row_hash = _digest('["2026-09-30","10.000000","11.000000","9.000000","10.750000",1000]')
        probes = (
            ("price_revisions", "UPDATE labbet_api.price_revisions SET content_hash=%s WHERE run_id=%s", ("0" * 64, result.run_id)),
            ("price_revisions", "UPDATE labbet_api.price_revisions SET close_price=10.75,content_hash=%s WHERE run_id=%s", (new_row_hash, result.run_id)),
            ("ingestion_runs", "UPDATE labbet_api.ingestion_runs SET row_count=2 WHERE run_id=%s", (result.run_id,)),
        )
        for table, query, parameters in probes:
            with self.subTest(table=table, query=query):
                with self.corrupt_transaction(table):
                    self.connection.execute(query, parameters)
                    self.assert_code("provenance_integrity", self.read)
                self.assertEqual(self.read().fingerprint, self.provenance.fingerprint)

    def test_stream_lock_timeout_rolls_back_archive_and_recovers(self):
        from api_storage import _digest as storage_digest, _lock_key
        key = _lock_key(["stream", self.p.source_id, self.c.instrument_id,
                         storage_digest(self.args["metadata"])])
        with self.connect(options="-c lock_timeout=100 -c statement_timeout=1500") as limited:
            with self.connect() as blocker:
                with blocker.transaction():
                    blocker.execute("SELECT pg_advisory_xact_lock(%s)", (key,))
                    with self.assertRaises(self.psycopg.errors.LockNotAvailable):
                        self.save(limited)
                    self.assertEqual(self.counts(), (0, 0, 0, 0))
                    self.assertEqual(self.manifest_count(), 0)
                self.assertFalse(self.save(limited).replayed)
        self.assertEqual(self.read().fingerprint, self.provenance.fingerprint)


if __name__ == "__main__":
    unittest.main()
