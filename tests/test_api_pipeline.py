"""Composed loopback TLS -> actual client/main -> disposable PostgreSQL tests.

Only runtime configuration, explicit fixture clocks and the database destination are injected. HTTP,
TLS certificate verification, JSON, credentials, durable shared state, row
validation, transactions and SQL are the actual implementation. All providers,
credentials and market observations here are synthetic. No external requests.
"""

from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import io
import json
import os
from pathlib import Path
import ssl
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"
sys.path.insert(0, str(ROOT / "ingestion"))

from api_client import ApiClient, FetchPolicy
from api_contracts import ApiFailure, DailyContract
from api_profiles import CredentialStore, SourceProfile
from api_state import StateStore
from api_transport import TransportPolicy
import market_data
import api_admin


NOW = datetime(2026, 9, 30, 21, tzinfo=timezone.utc)
DB_OPTIONS = "-c statement_timeout=15000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=30000"

# Independent byte fixtures and expected SQL facts are deliberately handwritten.
# Neither expected rows nor fixtures are generated through production helpers.
BODY_A = b'''{"Meta Data":{"2. Symbol":"TEST","3. Last Refreshed":"2026-09-30","5. Time Zone":"UTC"},"Time Series (Daily)":{"2026-09-29":{"1. open":"9.50","2. high":"11.00","3. low":"9.00","4. close":"10.00","5. volume":"101"},"2026-09-30":{"1. open":"10.00","2. high":"12.00","3. low":"9.50","4. close":"11.25","5. volume":"202"}}}'''
BODY_B = b'''{"Meta Data":{"2. Symbol":"TEST","3. Last Refreshed":"2026-09-30","5. Time Zone":"UTC"},"Time Series (Daily)":{"2026-09-29":{"1. open":"19.50","2. high":"21.00","3. low":"19.00","4. close":"20.00","5. volume":"702"},"2026-09-30":{"1. open":"20.00","2. high":"22.00","3. low":"19.50","4. close":"21.25","5. volume":"803"}}}'''


class _LocalHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        target = urlsplit(self.path)
        self.server.requests.append((target.path, parse_qs(target.query)))
        if self.server.arrived is not None:
            self.server.arrived.set()
        if self.server.wait_for is not None and not self.server.wait_for.wait(8):
            self.send_error(504)
            return
        status, body = self.server.routes[target.path]
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()
        if self.server.completions is not None:
            self.server.completions.append(self.server.label)
        if self.server.completed is not None:
            self.server.completed.set()
        self.close_connection = True


class _LocalServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, *args):
        # Never echo a request URL through the test HTTP server's diagnostics.
        pass


class ComposedPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dsn = os.environ.get("LABBET_TEST_DSN")
        if not cls.dsn:
            raise unittest.SkipTest("LABBET_TEST_DSN not set; composed PostgreSQL pipeline not run")
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        if conninfo_to_dict(cls.dsn).get("dbname") != "labbet_api_test":
            raise RuntimeError("Refusing pipeline tests: explicit labbet_api_test database required")
        cls.real_connect = staticmethod(psycopg.connect)
        with cls.real_connect(cls.dsn, autocommit=True, connect_timeout=5) as connection:
            if connection.info.dbname != "labbet_api_test":
                raise RuntimeError("Refusing pipeline tests: connected database is not labbet_api_test")
            connection.execute((ROOT / "sql" / "002_api_provenance.sql").read_text(encoding="utf-8"))
        spec = importlib.util.spec_from_file_location("labbet_pipeline_main", ROOT / "ingestion" / "main.py")
        cls.main = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.main)

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.state_path = Path(self.directory.name) / "shared.sqlite"
        self.servers = []
        self.source_prefix = "pipeline-" + uuid4().hex
        self.sources = []
        self.context = threading.local()
        self.output = io.StringIO()
        self.errors = io.StringIO()
        self.storage_recorder = market_data.record_storage_outcome
        self.read_only_database = False
        self.legacy_before = self.legacy_snapshot()

    def tearDown(self):
        for server, thread in self.servers:
            server.shutdown()
            server.server_close()
            thread.join(2)
        self.directory.cleanup()
        self.assertEqual(self.legacy_snapshot(), self.legacy_before)

    def legacy_snapshot(self):
        with self.real_connect(self.dsn, autocommit=True, connect_timeout=5) as connection:
            if connection.execute("SELECT to_regclass('public.daily_prices')").fetchone()[0] is None:
                return None
            return connection.execute("""
                SELECT symbol, trade_date, open_price, high_price, low_price, close_price, volume
                FROM public.daily_prices ORDER BY symbol, trade_date
            """).fetchall()

    def server(self, body=BODY_A, *, status=200, label="one", arrived=None,
               wait_for=None, completed=None, completions=None):
        server = _LocalServer(("127.0.0.1", 0), _LocalHandler)
        server.routes = {"/query": (status, body)}
        server.requests = []
        server.label = label
        server.arrived, server.wait_for = arrived, wait_for
        server.completed, server.completions = completed, completions
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(FIXTURES / "server.pem"), str(FIXTURES / "server.key"))
        server.socket = context.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever,
                                  kwargs={"poll_interval": 0.025}, daemon=True)
        thread.start()
        self.servers.append((server, thread))
        return server

    def runtime(self, server, suffix):
        source = self.source_prefix + "-" + suffix
        self.sources.append(source)
        origin = "https://localhost:" + str(server.server_port)
        profile = SourceProfile(
            source_id=source, account_ref="account-" + suffix, environment="test",
            endpoint=origin + "/query", credential_env="PIPELINE_KEY",
            credential_version="v1", quota_scope="quota-" + suffix,
            currency="USD", exchange="XTEST", timezone="UTC", price_basis="raw",
            schema_version="alphavantage-daily-v1", rights_verified=True,
            rights_review_date="2026-09-29", review_due_date="2026-10-01",
            max_attempts_per_window=100, circuit_failure_threshold=10,
        )
        contract = DailyContract(
            symbol="TEST", instrument_id="XTEST:TEST", currency="USD", exchange="XTEST",
            timezone="UTC", price_basis="raw", calendar_version="independent-fixture-v1",
            calendar_valid_from=date(2026, 9, 29), calendar_valid_until=date(2026, 10, 1),
            sessions=((date(2026, 9, 29), datetime(2026, 9, 29, 20, tzinfo=timezone.utc)),
                      (date(2026, 9, 30), datetime(2026, 9, 30, 20, tzinfo=timezone.utc))),
            minimum_sessions=2,
        )
        state = StateStore(self.state_path)
        credentials = CredentialStore(state)
        credentials.install(profile, {"PIPELINE_KEY": "SYNTHETIC-KEY-" + suffix})
        client = ApiClient(state, credentials,
            policy=FetchPolicy(total_seconds=20, socket_seconds=10, max_attempts=1),
            wall=lambda: NOW.timestamp(),
            transport_policy=TransportPolicy(origin, test_mode=True, ca_file=str(FIXTURES / "ca.pem")))
        return client, profile, contract

    def route_connection(self, *args, **kwargs):
        # Any deviation from main's explicit connection contract fails the test.
        self.assertEqual(args, ())
        self.assertEqual(kwargs, {"connect_timeout": 10, "options": DB_OPTIONS})
        routed = dict(kwargs)
        if self.read_only_database:
            routed["options"] += " -c default_transaction_read_only=on"
        return self.real_connect(self.dsn, **routed)

    def run_bundle(self, runtime):
        self.context.runtime = runtime
        self.main.run("TEST")

    def invoke(self, runtimes, *, parallel=False):
        connect = Mock(side_effect=self.route_connection)
        with patch.object(market_data, "load_runtime", side_effect=lambda symbol: self.context.runtime), \
             patch.object(market_data, "record_storage_outcome", side_effect=lambda batch, succeeded:
                 self.storage_recorder(batch, succeeded=succeeded, now=NOW.timestamp() + 0.25)), \
             patch.object(self.main.psycopg, "connect", connect), \
             redirect_stdout(self.output), redirect_stderr(self.errors):
            if parallel:
                with ThreadPoolExecutor(max_workers=len(runtimes)) as executor:
                    futures = [executor.submit(self.run_bundle, runtime) for runtime in runtimes]
                    results = []
                    for future in futures:
                        try:
                            future.result(timeout=30)
                            results.append(None)
                        except Exception as error:
                            results.append(error)
            else:
                results = []
                for runtime in runtimes:
                    try:
                        self.run_bundle(runtime)
                        results.append(None)
                    except Exception as error:
                        results.append(error)
        return results, connect

    def stored(self):
        with self.real_connect(self.dsn, autocommit=True, connect_timeout=5) as connection:
            return connection.execute("""
                SELECT c.source_id, r.trade_date, r.open_price, r.high_price, r.low_price,
                       r.close_price, r.volume, r.observed_at, j.symbol,
                       j.contract_metadata->>'currency', j.contract_metadata->>'market'
                FROM labbet_api.current_prices c
                JOIN labbet_api.price_revisions r USING (revision_id)
                JOIN labbet_api.ingestion_runs j USING (run_id)
                WHERE c.source_id = ANY(%s) ORDER BY c.source_id, r.trade_date
            """, (self.sources,)).fetchall()

    def assert_output_safe(self):
        output = self.output.getvalue() + self.errors.getvalue()
        self.assertNotIn("SYNTHETIC-KEY", output)
        self.assertNotIn("apikey=", output)
        self.assertNotIn("postgresql://", output)

    def test_real_tls_to_main_to_postgres_preserves_independent_expected_facts(self):
        server = self.server()
        runtime = self.runtime(server, "a")
        results, connect = self.invoke([runtime])
        self.assertEqual(results, [None])
        connect.assert_called_once_with(connect_timeout=10, options=DB_OPTIONS)
        source = runtime[1].source_id
        observed = datetime(2026, 9, 30, 20, tzinfo=timezone.utc)
        self.assertEqual(self.stored(), [
            (source, date(2026, 9, 29), Decimal("9.5"), Decimal("11"), Decimal("9"),
             Decimal("10"), 101, observed, "TEST", "USD", "XTEST"),
            (source, date(2026, 9, 30), Decimal("10"), Decimal("12"), Decimal("9.5"),
             Decimal("11.25"), 202, observed, "TEST", "USD", "XTEST"),
        ])
        self.assertEqual(server.requests[0][1]["apikey"], ["SYNTHETIC-KEY-a"])
        self.assert_output_safe()

    def test_real_error_and_malformed_responses_stop_before_database_connection(self):
        cases = ((200, b'{"broken":', "invalid_json"),
                 (403, b'{"message":"SYNTHETIC-KEY-hidden"}', "permission_unknown"),
                 (200, b'{"Information":"SYNTHETIC-KEY-hidden"}', "provider_information"))
        for index, (status, body, code) in enumerate(cases):
            with self.subTest(code=code):
                runtime = self.runtime(self.server(body, status=status), "bad" + str(index))
                results, connect = self.invoke([runtime])
                self.assertIsInstance(results[0], ApiFailure)
                self.assertEqual(results[0].code, code)
                connect.assert_not_called()
        self.assertEqual(self.stored(), [])
        self.assert_output_safe()

    def test_two_real_https_sources_overlap_and_reverse_without_key_or_data_mixing(self):
        a_arrived, b_completed = threading.Event(), threading.Event()
        completions = []
        a = self.server(BODY_A, label="a", arrived=a_arrived,
                        wait_for=b_completed, completions=completions)
        b = self.server(BODY_B, label="b", wait_for=a_arrived,
                        completed=b_completed, completions=completions)
        first, second = self.runtime(a, "a"), self.runtime(b, "b")
        results, connect = self.invoke([first, second], parallel=True)
        self.assertEqual(results, [None, None])
        self.assertEqual(connect.call_count, 2)
        self.assertEqual(completions, ["b", "a"])
        self.assertEqual(a.requests[0][1]["apikey"], ["SYNTHETIC-KEY-a"])
        self.assertEqual(b.requests[0][1]["apikey"], ["SYNTHETIC-KEY-b"])
        rows = self.stored()
        self.assertEqual([(row[0], row[5], row[6]) for row in rows], [
            (first[1].source_id, Decimal("10"), 101),
            (first[1].source_id, Decimal("11.25"), 202),
            (second[1].source_id, Decimal("20"), 702),
            (second[1].source_id, Decimal("21.25"), 803),
        ])
        self.assert_output_safe()

    def test_failed_real_source_does_not_block_other_source_or_create_partial_rows(self):
        good = self.runtime(self.server(BODY_A), "good")
        bad = self.runtime(self.server(b'{"message":"temporary"}', status=503), "bad")
        results, connect = self.invoke([good, bad], parallel=True)
        self.assertIsNone(results[0])
        self.assertIsInstance(results[1], ApiFailure)
        self.assertEqual(results[1].code, "http")
        self.assertEqual(connect.call_count, 1)
        rows = self.stored()
        self.assertEqual(len(rows), 2)
        self.assertEqual({row[0] for row in rows}, {good[1].source_id})
        with self.real_connect(self.dsn, autocommit=True, connect_timeout=5) as connection:
            self.assertEqual(connection.execute(
                "SELECT count(*) FROM labbet_api.ingestion_runs WHERE source_id=%s",
                (bad[1].source_id,)).fetchone()[0], 0)
        self.assert_output_safe()

    def test_postgres_failure_is_unhealthy_until_a_new_batch_really_commits(self):
        server = self.server(BODY_A)
        failed = self.runtime(server, "recovery")
        self.read_only_database = True
        results, connect = self.invoke([failed])
        self.assertIsNotNone(results[0])
        self.assertEqual(getattr(results[0], "sqlstate", None), "25006")
        self.assertEqual(connect.call_count, 1)
        self.assertEqual(self.stored(), [])

        def health():
            output = io.StringIO()
            with redirect_stdout(output):
                code = api_admin.main([
                    "--state", str(self.state_path), "health", "--source", failed[1].source_id,
                    "--max-silence", "7200", "--max-data-age", "7200",
                ], wall=lambda: NOW.timestamp() + 0.5)
            return code, json.loads(output.getvalue())

        code, report = health()
        self.assertEqual(code, 1)
        self.assertFalse(report["healthy"])
        self.assertEqual(report["health"]["storage_status"], "failed")

        self.read_only_database = False
        recovered = self.runtime(server, "recovery")
        results, connect = self.invoke([recovered])
        self.assertEqual(results, [None])
        self.assertEqual(connect.call_count, 1)
        self.assertEqual(len(self.stored()), 2)
        code, report = health()
        self.assertEqual(code, 0)
        self.assertTrue(report["healthy"])
        self.assertEqual(report["health"]["storage_status"], "stored")
        self.assertEqual(report["health"]["storage_request_id"], report["health"]["last_success_request_id"])
        self.assert_output_safe()


if __name__ == "__main__":
    unittest.main()
