"""Pure validation plus opt-in real PostgreSQL transaction tests.

The integration suite only accepts LABBET_TEST_DSN with an explicit database
name labbet_api_test. Run it against the disposable test container. Test rows
use unique source IDs; no DELETE, TRUNCATE or DROP is issued by this suite.
"""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import os
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest.mock import Mock
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ingestion"))
from api_storage import persist_batch


def batch(source="test-source"):
    observed = datetime(2026, 10, 14, 20, tzinfo=timezone.utc)
    return dict(
        source_id=source, instrument_id="XNAS:TEST", symbol="TEST",
        request_id="request-1", observed_at=observed,
        received_at=observed + timedelta(minutes=2), config_version="v1",
        payload_hash=hashlib.sha256(b"synthetic payload").hexdigest(),
        metadata={"currency": "USD", "price_basis": "raw", "schema_version": "v1",
                  "market_timezone": "UTC", "market": "XTEST"},
        rows=[("TEST", date(2026, 10, 14), Decimal("10.00"), Decimal("11.00"),
               Decimal("9.00"), Decimal("10.50"), 1000)],
    )


class StorageValidationTests(unittest.TestCase):
    def assert_invalid(self, args):
        connection = Mock()
        with self.assertRaises(ValueError):
            persist_batch(connection, **args)
        self.assertEqual(connection.mock_calls, [])

    def test_invalid_identifiers_stop_before_transaction(self):
        for field in ("source_id", "instrument_id", "symbol", "request_id", "config_version"):
            for value in ("", " x", "x\ny", "x" * 129, None, 12):
                with self.subTest(field=field, value=value):
                    args = batch()
                    args[field] = value
                    self.assert_invalid(args)

    def test_contract_metadata_is_required_and_whitelisted(self):
        variants = [None, {}, {**batch()["metadata"], "api_key": "synthetic-secret"},
                    {**batch()["metadata"], "currency": "usd"},
                    {**batch()["metadata"], "price_basis": "unknown"},
                    {**batch()["metadata"], "market_timezone": "Unknown/Zone"},
                    {**batch()["metadata"], "schema_version": "v1\nsecret"}]
        for metadata in variants:
            with self.subTest(metadata=metadata):
                args = batch()
                args["metadata"] = metadata
                self.assert_invalid(args)

    def test_timestamps_and_hash_are_validated(self):
        for field, value in (("observed_at", datetime(2026, 10, 14)),
                             ("received_at", "2026-10-14"),
                             ("received_at", datetime(2026, 10, 13, tzinfo=timezone.utc)),
                             ("payload_hash", "bad"), ("payload_hash", "A" * 64)):
            with self.subTest(field=field, value=value):
                args = batch()
                args[field] = value
                self.assert_invalid(args)

    def test_invalid_rows_do_not_start_transaction(self):
        original = batch()["rows"][0]
        replacements = [(0, "OTHER"), (1, "2026-10-14"), (1, datetime(2026, 10, 14)),
                        (2, Decimal("NaN")), (2, Decimal("Infinity")),
                        (2, Decimal("10.0000001")), (2, 10), (2, Decimal("0")),
                        (2, Decimal("1000000000000")), (3, Decimal("8")),
                        (5, Decimal("12")), (6, True), (6, -1), (6, 2**63)]
        for position, value in replacements:
            with self.subTest(position=position, value=value):
                row = list(original)
                row[position] = value
                args = batch()
                args["rows"] = [tuple(row)]
                self.assert_invalid(args)

    def test_empty_duplicate_and_malformed_rows_are_rejected(self):
        row = batch()["rows"][0]
        for rows in ([], [row, row], [row[:-1]], ["bad"], None):
            with self.subTest(rows=rows):
                args = batch()
                args["rows"] = rows
                self.assert_invalid(args)

    def test_row_limit_stops_unbounded_iterator(self):
        # A small configured boundary is enough to test that iteration stops;
        # every item is otherwise valid and has a distinct date.
        from unittest.mock import patch
        args = batch()
        original = args["rows"][0]
        consumed = []
        def rows():
            for index in range(20):
                consumed.append(index)
                yield (original[0], date(2026, 10, 1) + timedelta(days=index), *original[2:])
        args["rows"] = rows()
        with patch("api_storage.MAX_ROWS", 2):
            self.assert_invalid(args)
        self.assertEqual(consumed, [0, 1, 2])


class StoragePostgresTests(unittest.TestCase):
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
        with cls.connect() as connection:
            if connection.info.dbname != "labbet_api_test":
                raise RuntimeError("Refusing database tests: connected database is not labbet_api_test")
            connection.execute((ROOT / "sql" / "002_api_provenance.sql").read_text(encoding="utf-8"))

    @classmethod
    def connect(cls):
        return cls.psycopg.connect(cls.dsn, autocommit=True, connect_timeout=5)

    def setUp(self):
        self.args = batch("test-" + uuid4().hex)
        self.connection = self.connect()

    def tearDown(self):
        self.connection.close()

    def save(self, **changes):
        args = deepcopy(self.args)
        args.update(changes)
        return persist_batch(self.connection, **args)

    def counts(self, connection=None):
        connection = connection or self.connection
        return tuple(connection.execute(
            f"SELECT count(*) FROM labbet_api.{table} WHERE source_id = %s",
            (self.args["source_id"],)).fetchone()[0]
            for table in ("ingestion_runs", "price_revisions", "current_prices"))

    def current(self):
        return self.connection.execute("""
            SELECT r.close_price, r.observed_at FROM labbet_api.current_prices c
            JOIN labbet_api.price_revisions r USING (revision_id)
            WHERE c.source_id = %s ORDER BY c.trade_date
        """, (self.args["source_id"],)).fetchall()

    def test_atomic_batch_preserves_exact_values_and_metadata(self):
        row = self.args["rows"][0]
        rows = [row, (row[0], date(2026, 10, 13), *row[2:])]
        result = self.save(rows=rows)
        self.assertEqual((result.row_count, result.inserted_revisions, result.advanced_current), (2, 2, 2))
        self.assertFalse(result.replayed)
        self.assertEqual(self.counts(), (1, 2, 2))
        stored = self.connection.execute("""
            SELECT contract_metadata, payload_hash FROM labbet_api.ingestion_runs WHERE run_id=%s
        """, (result.run_id,)).fetchone()
        self.assertEqual(stored, (self.args["metadata"], self.args["payload_hash"]))
        self.assertEqual(self.current()[0][0], Decimal("10.500000"))

    def test_replay_is_idempotent_even_if_rows_are_reordered(self):
        row = self.args["rows"][0]
        rows = [row, (row[0], date(2026, 10, 13), *row[2:])]
        first = self.save(rows=rows)
        second = self.save(rows=list(reversed(rows)))
        self.assertEqual(first.run_id, second.run_id)
        self.assertTrue(second.replayed)
        self.assertEqual((second.inserted_revisions, second.advanced_current), (0, 0))
        self.assertEqual(self.counts(), (1, 2, 2))

    def test_request_id_reuse_with_changed_content_is_rejected(self):
        self.save()
        for changes in ({"payload_hash": "a" * 64}, {"instrument_id": "OTHER"},
                        {"config_version": "v2"},
                        {"received_at": self.args["received_at"] + timedelta(seconds=1)}):
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, "request_id"):
                self.save(**changes)
        self.assertEqual(self.counts(), (1, 1, 1))

    def test_newer_correction_advances_and_retains_history(self):
        self.save()
        row = list(self.args["rows"][0])
        row[5] = Decimal("10.75")
        observed = self.args["observed_at"] + timedelta(minutes=1)
        result = self.save(request_id="correction", rows=[tuple(row)], observed_at=observed)
        self.assertEqual(result.advanced_current, 1)
        self.assertEqual(self.counts(), (2, 2, 1))
        self.assertEqual(self.current(), [(Decimal("10.750000"), observed)])

    def test_late_old_response_is_retained_without_replacing_current(self):
        self.save()
        row = list(self.args["rows"][0])
        row[5] = Decimal("10.25")
        result = self.save(request_id="late-old", rows=[tuple(row)],
                           observed_at=self.args["observed_at"] - timedelta(days=1))
        self.assertEqual(result.advanced_current, 0)
        self.assertEqual(self.counts(), (2, 2, 1))
        self.assertEqual(self.current()[0][0], Decimal("10.500000"))

    def test_equal_observation_conflict_rolls_back_whole_batch(self):
        self.save()
        row = list(self.args["rows"][0])
        row[5] = Decimal("10.75")
        fresh = ("TEST", date(2026, 10, 13), *row[2:])
        with self.assertRaisesRegex(ValueError, "same observation timestamp"):
            self.save(request_id="ambiguous", rows=[fresh, tuple(row)])
        self.assertEqual(self.counts(), (1, 1, 1))

    def test_conflicting_historical_timestamp_is_also_rejected(self):
        self.save()
        self.save(request_id="newer", observed_at=self.args["observed_at"] + timedelta(minutes=1))
        row = list(self.args["rows"][0])
        row[5] = Decimal("10.75")
        with self.assertRaisesRegex(ValueError, "same observation timestamp"):
            self.save(request_id="historical-conflict", rows=[tuple(row)])
        self.assertEqual(self.counts(), (2, 2, 1))

    def test_sources_and_contracts_are_isolated(self):
        self.save()
        other = deepcopy(self.args)
        other["source_id"] += "-other"
        persist_batch(self.connection, **other)
        contract = {**self.args["metadata"], "currency": "SEK"}
        self.save(request_id="other-contract", metadata=contract)
        self.assertEqual(self.counts(), (2, 2, 2))
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM labbet_api.current_prices WHERE source_id=%s",
            (other["source_id"],)).fetchone()[0], 1)

    def _interrupted_connection(self, disconnect=False):
        actual = self.connection
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
                if "INSERT INTO labbet_api.price_revisions" in query:
                    if disconnect:
                        actual.close()
                    raise RuntimeError("simulated interruption after a row insert")
                return result
            def __getattr__(self, name):
                return getattr(self.cursor, name)
        class ConnectionProxy:
            def transaction(self):
                return actual.transaction()
            def cursor(self):
                return CursorProxy()
        return ConnectionProxy()

    def test_exception_after_insert_rolls_back_run_and_rows(self):
        with self.assertRaises(RuntimeError):
            persist_batch(self._interrupted_connection(), **self.args)
        self.assertEqual(self.counts(), (0, 0, 0))
        self.save()
        self.assertEqual(self.counts(), (1, 1, 1))

    def test_connection_loss_rolls_back_uncommitted_rows(self):
        with self.assertRaises(Exception):
            persist_batch(self._interrupted_connection(disconnect=True), **self.args)
        with self.connect() as observer:
            self.assertEqual(self.counts(observer), (0, 0, 0))
            persist_batch(observer, **self.args)
            self.assertEqual(self.counts(observer), (1, 1, 1))

    def test_outer_transaction_retains_commit_ownership(self):
        with self.assertRaisesRegex(RuntimeError, "outer rollback"):
            with self.connection.transaction():
                self.save()
                raise RuntimeError("outer rollback")
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_history_is_immutable_in_database(self):
        result = self.save()
        for query in (
            "UPDATE labbet_api.ingestion_runs SET config_version='changed' WHERE run_id=%s",
            "DELETE FROM labbet_api.ingestion_runs WHERE run_id=%s",
            "UPDATE labbet_api.price_revisions SET close_price=10 WHERE run_id=%s",
            "DELETE FROM labbet_api.price_revisions WHERE run_id=%s",
        ):
            with self.subTest(query=query), self.assertRaises(self.psycopg.errors.ObjectNotInPrerequisiteState):
                self.connection.execute(query, (result.run_id,))
        self.assertEqual(self.counts(), (1, 1, 1))

    def test_concurrent_replay_creates_only_one_run(self):
        barrier = threading.Barrier(2)
        def writer():
            with self.connect() as connection:
                barrier.wait(timeout=5)
                return persist_batch(connection, **deepcopy(self.args))
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(writer) for _ in range(2)]
            results = [future.result(timeout=15) for future in futures]
        self.assertEqual(sorted(result.replayed for result in results), [False, True])
        self.assertEqual(results[0].run_id, results[1].run_id)
        self.assertEqual(self.counts(), (1, 1, 1))

    def test_concurrent_old_and_new_response_keep_newest(self):
        old = deepcopy(self.args)
        new = deepcopy(self.args)
        new["request_id"] = "newer"
        new["observed_at"] += timedelta(minutes=1)
        row = list(new["rows"][0])
        row[5] = Decimal("10.75")
        new["rows"] = [tuple(row)]
        barrier = threading.Barrier(2)
        def writer(args):
            with self.connect() as connection:
                barrier.wait(timeout=5)
                return persist_batch(connection, **args)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(writer, args) for args in (old, new)]
            for future in futures:
                future.result(timeout=15)
        self.assertEqual(self.counts(), (2, 2, 1))
        self.assertEqual(self.current(), [(Decimal("10.750000"), new["observed_at"])])

    def test_blocked_stream_lock_times_out_rolls_back_and_can_recover(self):
        from api_storage import _digest, _lock_key
        lock_key = _lock_key(["stream", self.args["source_id"], self.args["instrument_id"],
                              _digest(self.args["metadata"])])
        with self.psycopg.connect(self.dsn, autocommit=True, connect_timeout=5,
                                 options="-c lock_timeout=100 -c statement_timeout=1500") as limited:
            with self.connect() as blocker:
                with blocker.transaction():
                    blocker.execute("SELECT pg_advisory_xact_lock(%s)", (lock_key,))
                    started = time.monotonic()
                    with self.assertRaises(self.psycopg.errors.LockNotAvailable):
                        persist_batch(limited, **self.args)
                    self.assertLess(time.monotonic() - started, 2.5)
                    self.assertEqual(self.counts(), (0, 0, 0))
                result = persist_batch(limited, **self.args)
                self.assertFalse(result.replayed)
                self.assertEqual(self.counts(), (1, 1, 1))


if __name__ == "__main__":
    unittest.main()
