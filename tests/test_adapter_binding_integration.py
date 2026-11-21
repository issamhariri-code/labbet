"""Per-fetch binding, real durable state and loopback TLS; synthetic secrets."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from copy import deepcopy
from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import multiprocessing
from pathlib import Path
import sqlite3
import ssl
import threading
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from api_adapters import AlphaDailyAdapter, resolve_daily_adapter
from api_client import ApiClient, FetchPolicy
from api_contracts import ApiFailure
from api_profiles import SourceProfile
from api_transport import TransportError, TransportPolicy
import test_api_client as fixtures
import test_api_runtime as runtime_fixtures
import test_client_runtime_adapter_review as review_fixtures
import test_quota_json_integration as quota_fixtures


ALPHA = "alphavantage-daily-v1"
PRODUCTION = "https://www.alphavantage.co/query"


@dataclass(frozen=True, slots=True)
class RawBindingProfileDouble(SourceProfile):
    """Only for unreachable future-ID/error branches; not a valid profile."""
    def __post_init__(self):
        pass


class BoundAdapterDouble:
    """Resolver-return probe with distinct URL and parser behavior per source.

    The real resolver still validates profiles before this test-only object is
    returned. No alternative production adapter or ID is enabled.
    """
    def __init__(self, label, close):
        self.label, self.close = label, close
        self.built, self.parsed = [], []

    def build_url(self, endpoint, symbol, secret):
        self.built.append((endpoint, symbol, secret))
        return AlphaDailyAdapter.build_url(endpoint, symbol, secret) + "&test_adapter=" + self.label

    def parse(self, data, contract, received):
        self.parsed.append((deepcopy(data), contract, received))
        copied = deepcopy(data)
        copied["Time Series (Daily)"]["2026-09-30"]["4. close"] = self.close
        return AlphaDailyAdapter.parse(copied, contract, received)


class AdapterBindingClientTests(unittest.TestCase):
    setUp = review_fixtures.ClientAdapterInsertionTests.setUp
    snapshot = review_fixtures.ClientAdapterInsertionTests.snapshot

    def backup(self):
        p = replace(self.p, source_id="source-b", account_ref="account-b", quota_scope="account-b",
                    endpoint="https://source-b.invalid/query", credential_env="KEY_B")
        self.keys.install(p, {"KEY_B": "FAKE-B"})
        return p

    def raw(self, **changes):
        values = {field.name: getattr(self.p, field.name) for field in fields(self.p)}
        return RawBindingProfileDouble(**dict(values, **changes))

    def seed(self):
        self.state.register(self.p)
        lease = self.state.acquire(self.p, "seed", self.clock.wall())
        self.state.release(lease, self.clock.wall())

    def deny(self, p, c, code):
        before = self.snapshot()
        self.transport.reset_mock()
        with patch.object(self.state, "register", side_effect=AssertionError("registration forbidden")) as register, \
                patch.object(self.keys, "snapshot", side_effect=AssertionError("key snapshot forbidden")) as snapshot:
            with self.assertRaises(ApiFailure) as caught:
                self.client.fetch(p, c)
            self.assertEqual(caught.exception.code, code)
            self.assertEqual(str(caught.exception), "API-hämtningen stoppades: " + code)
            register.assert_not_called()
            snapshot.assert_not_called()
            self.transport.assert_not_called()
        self.assertEqual(self.snapshot(), before)

    def test_fetch_uses_the_returned_adapter_not_a_hardcoded_alpha_path(self):
        adapter = BoundAdapterDouble("chosen", "10.25")
        def choose(p, c):
            self.assertIs(type(resolve_daily_adapter(p, c)), AlphaDailyAdapter)
            return adapter
        with patch("api_client.resolve_daily_adapter", side_effect=choose) as resolver:
            batch = self.client.fetch(self.p, self.c)
        resolver.assert_called_once_with(self.p, self.c)
        self.assertEqual(adapter.built, [(self.p.endpoint, self.c.symbol, "FAKE-SECRET-A")])
        self.assertEqual(len(adapter.parsed), 1)
        self.assertEqual(parse_qs(urlsplit(self.transport.call_args.args[0]).query)["test_adapter"], ["chosen"])
        self.assertEqual(batch.prices["2026-09-30"]["4. close"], "10.25")
        self.assertEqual(adapter.parsed[0][2], fixtures.NOW)

    def test_resolver_rejections_leave_seeded_state_and_keys_untouched(self):
        self.seed()
        self.deny(self.raw(adapter_id="FAKE-UNKNOWN"), self.c, "unsupported_adapter")
        self.deny(replace(self.p, environment="production", endpoint="https://other.invalid/query"),
                  self.c, "adapter_endpoint_mismatch")
        for name, value in (("currency", "EUR"), ("exchange", "OTHER"), ("timezone", "Europe/Stockholm"),
                            ("price_basis", "adjusted"), ("schema_version", "other-v2")):
            with self.subTest(field=name):
                self.deny(replace(self.p, **{name: value}), self.c, "source_contract_mismatch")
        self.deny(replace(self.p, price_basis="adjusted"), replace(self.c, price_basis="adjusted"), "adapter_contract_mismatch")
        self.deny(replace(self.p, schema_version="other-v2"), replace(self.c, schema_version="other-v2"), "adapter_contract_mismatch")

    def test_both_environment_conflicts_fail_before_side_effects(self):
        self.seed()
        production = replace(self.p, environment="production", endpoint=PRODUCTION)
        for p, policy in ((self.p, TransportPolicy("https://source-a.invalid", test_mode=False)),
                          (production, TransportPolicy("https://www.alphavantage.co", test_mode=True))):
            with self.subTest(environment=p.environment):
                self.client.transport_policy = policy
                self.deny(p, self.c, "environment_mismatch")

    def test_strict_policy_type_origin_ca_and_bool_keep_state_unchanged(self):
        self.seed()
        production = replace(self.p, environment="production", endpoint=PRODUCTION)
        invalid = (False, 0, [], {}, "FAKE-POLICY", object(),
                   TransportPolicy("https://source-a.invalid", test_mode=1),
                   TransportPolicy("https://source-a.invalid", test_mode="test"),
                   TransportPolicy("https://other.invalid", test_mode=True),
                   TransportPolicy("https://source-a.invalid:443", test_mode=True),
                   TransportPolicy("https://source-a.invalid/query", test_mode=True),
                   TransportPolicy(None, test_mode=True))
        for policy in invalid:
            with self.subTest(policy_type=type(policy).__name__):
                self.client.transport_policy = policy
                self.deny(self.p, self.c, "configuration")
        self.client.transport_policy = TransportPolicy("https://www.alphavantage.co", ca_file="FAKE-CA")
        self.deny(production, self.c, "configuration")

    def test_matching_origin_with_optional_slash_succeeds(self):
        for slash in ("", "/"):
            with self.subTest(slash=slash):
                self.client.transport_policy = TransportPolicy("https://source-a.invalid" + slash, test_mode=True)
                batch = self.client.fetch(self.p, self.c)
                self.assertEqual(batch.prices["2026-09-30"], fixtures.ROW)

    def test_fallback_uses_own_adapter_endpoint_key_and_remaining_budget(self):
        backup = self.backup()
        self.client.policy = FetchPolicy(total_seconds=20, socket_seconds=10, max_attempts=1)
        adapters = {self.p.source_id: BoundAdapterDouble("primary", "10.25"),
                    backup.source_id: BoundAdapterDouble("backup", "10.75")}
        calls = []
        def choose(p, c):
            resolve_daily_adapter(p, c)
            return adapters[p.source_id]
        def transport(url, **options):
            calls.append((url, options))
            if urlsplit(url).hostname == "source-a.invalid":
                self.clock.elapsed += 7
                raise TransportError("network")
            return fixtures.response()
        self.client.transport = transport
        with patch("api_client.resolve_daily_adapter", side_effect=choose) as resolver, \
                patch.object(self.keys, "snapshot", wraps=self.keys.snapshot) as snapshots:
            batch = self.client.fetch_with_fallback(self.p, backup, self.c,
                approved_sources=((self.p.source_id, backup.source_id),))
        self.assertEqual([call.args[0].source_id for call in resolver.call_args_list], ["source-a", "source-b"])
        self.assertEqual([call.args[0].credential_env for call in snapshots.call_args_list], ["KEY_A", "KEY_B"])
        self.assertEqual([item[1]["total_timeout"] for item in calls], [20, 13])
        self.assertEqual([item[1]["policy"].origin for item in calls],
                         ["https://source-a.invalid", "https://source-b.invalid"])
        self.assertEqual([parse_qs(urlsplit(item[0]).query)["apikey"][0] for item in calls], ["FAKE-SECRET-A", "FAKE-B"])
        self.assertEqual([parse_qs(urlsplit(item[0]).query)["test_adapter"][0] for item in calls], ["primary", "backup"])
        self.assertEqual(len(adapters["source-a"].parsed), 0)
        self.assertEqual(len(adapters["source-b"].parsed), 1)
        self.assertEqual((batch.source_id, batch.prices["2026-09-30"]["4. close"]), ("source-b", "10.75"))

    def test_fixed_primary_policy_is_not_reused_for_other_fallback_origin(self):
        backup = self.backup()
        self.client.transport_policy = TransportPolicy("https://source-a.invalid", test_mode=True)
        self.transport.side_effect = TransportError("network")
        with patch("api_client.resolve_daily_adapter", wraps=resolve_daily_adapter) as resolver, \
                patch.object(self.state, "register", wraps=self.state.register) as register, \
                patch.object(self.keys, "snapshot", wraps=self.keys.snapshot) as snapshots:
            with self.assertRaises(ApiFailure) as caught:
                self.client.fetch_with_fallback(self.p, backup, self.c, approved_sources=(("source-a", "source-b"),))
            self.assertEqual(caught.exception.code, "configuration")
            self.assertEqual(resolver.call_count, 1)
            self.assertEqual(register.call_count, 1)
            self.assertEqual(snapshots.call_count, 1)
            self.transport.assert_called_once()
        with closing(sqlite3.connect(self.state.path)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM attempts WHERE scope=?", (backup.quota_scope,)).fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM profiles WHERE source_id=?", (backup.source_id,)).fetchone()[0], 0)

    def test_exhausted_budget_never_resolves_or_contacts_backup(self):
        backup = self.backup()
        self.client.policy = FetchPolicy(total_seconds=20, socket_seconds=10, max_attempts=1)
        def fail(url, **options):
            self.clock.elapsed += 20
            raise TransportError("network")
        self.transport.side_effect = fail
        with patch("api_client.resolve_daily_adapter", wraps=resolve_daily_adapter) as resolver, \
                patch.object(self.keys, "snapshot", wraps=self.keys.snapshot) as snapshots:
            with self.assertRaises(ApiFailure) as caught:
                self.client.fetch_with_fallback(self.p, backup, self.c, approved_sources=(("source-a", "source-b"),))
            # Existing fallback behavior: zero remaining total_seconds is rejected.
            self.assertEqual(caught.exception.code, "invalid_fetch_policy")
            self.assertEqual(resolver.call_count, 1)
            self.assertEqual(snapshots.call_count, 1)
            self.transport.assert_called_once()

    def test_permanent_response_and_transport_errors_never_fallback(self):
        backup = self.backup()
        # Keep the matrix below the account/circuit limits without resetting data.
        p = replace(self.p, config_version="permanent-errors", circuit_failure_threshold=1000)
        cases = [(TransportError("http", status=code), expected) for code, expected in
                 ((400, "http"), (401, "authentication"), (403, "permission_unknown"), (404, "http"), (429, "quota"))]
        cases += [(TransportError(kind), kind) for kind in ("tls", "protocol", "configuration")]
        for field, expected in (("Error Message", "provider_error"), ("Information", "provider_information"), ("Note", "provider_note")):
            data = fixtures.payload(); data[field] = "FAKE-PROVIDER-ERROR"
            cases.append((fixtures.response(data), expected))
        bad = fixtures.payload(); bad["Meta Data"]["2. Symbol"] = "OTHER"
        cases.append((fixtures.response(bad), "instrument_mismatch"))
        for value, expected in cases:
            with self.subTest(expected=expected):
                self.transport.reset_mock()
                if isinstance(value, Exception):
                    self.transport.side_effect = value
                else:
                    self.transport.side_effect = None
                    self.transport.return_value = value
                with patch("api_client.resolve_daily_adapter", wraps=resolve_daily_adapter) as resolver, \
                        patch.object(self.keys, "snapshot", wraps=self.keys.snapshot) as snapshots:
                    with self.assertRaises(ApiFailure) as caught:
                        self.client.fetch_with_fallback(p, backup, self.c, approved_sources=(("source-a", "source-b"),))
                    self.assertEqual(caught.exception.code, expected)
                    self.assertEqual(resolver.call_count, 1)
                    self.assertEqual(snapshots.call_count, 1)
                    self.transport.assert_called_once()
                self.clock.elapsed += 5

    def test_permanent_preflight_and_quota_errors_never_fallback(self):
        backup = self.backup()
        cases = ((self.raw(adapter_id="unknown"), backup, "unsupported_adapter"),
                 (replace(self.p, currency="EUR"), replace(backup, currency="EUR"), "source_contract_mismatch"),
                 (replace(self.p, rights_verified=False), backup, "rights_not_verified"))
        for p, b, expected in cases:
            with self.subTest(expected=expected):
                before = self.snapshot()
                with patch.object(self.keys, "snapshot", side_effect=AssertionError("key read forbidden")) as snapshots:
                    with self.assertRaises(ApiFailure) as caught:
                        self.client.fetch_with_fallback(p, b, self.c, approved_sources=(("source-a", "source-b"),))
                    self.assertEqual(caught.exception.code, expected)
                    snapshots.assert_not_called()
                self.transport.assert_not_called()
                self.assertEqual(self.snapshot(), before)
        lease = self.state.acquire(self.p, "fill", self.clock.wall(), cost=self.p.max_attempts_per_window)
        self.state.release(lease, self.clock.wall())
        with patch("api_client.resolve_daily_adapter", wraps=resolve_daily_adapter) as resolver:
            with self.assertRaises(ApiFailure) as caught:
                self.client.fetch_with_fallback(self.p, backup, self.c, approved_sources=(("source-a", "source-b"),))
            self.assertEqual(caught.exception.code, "quota")
            self.assertEqual(resolver.call_count, 1)
            self.transport.assert_not_called()

    def test_concurrent_fetches_keep_distinct_adapter_key_endpoint_and_parser(self):
        backup = self.backup()
        barrier = threading.Barrier(2)
        adapters = {"source-a": BoundAdapterDouble("a", "10.25"), "source-b": BoundAdapterDouble("b", "10.75")}
        def choose(p, c):
            resolve_daily_adapter(p, c)
            return adapters[p.source_id]
        def transport(url, **options):
            query = parse_qs(urlsplit(url).query)
            if urlsplit(url).hostname == "source-a.invalid":
                self.assertEqual((query["apikey"], query["test_adapter"]), (["FAKE-SECRET-A"], ["a"]))
            else:
                self.assertEqual((query["apikey"], query["test_adapter"]), (["FAKE-B"], ["b"]))
            barrier.wait(timeout=5)
            return fixtures.response()
        self.client.transport = transport
        with patch("api_client.resolve_daily_adapter", side_effect=choose) as resolver, ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(self.client.fetch, p, self.c) for p in (self.p, backup)]
            results = [future.result(timeout=10) for future in futures]
            self.assertEqual(resolver.call_count, 2)
        self.assertEqual([(batch.source_id, batch.prices["2026-09-30"]["4. close"]) for batch in results],
                         [("source-a", "10.25"), ("source-b", "10.75")])
        self.assertEqual([len(adapters[source].parsed) for source in ("source-a", "source-b")], [1, 1])
        self.assertEqual(self.state.health("source-a")["active_jobs"], 0)
        self.assertEqual(self.state.health("source-b")["active_jobs"], 0)

    def test_rotation_during_fetch_keeps_old_snapshot_and_new_fetch_uses_new_key(self):
        arrived, released = threading.Event(), threading.Event()
        urls = []
        def transport(url, **options):
            urls.append(url)
            if len(urls) == 1:
                arrived.set()
                if not released.wait(5):
                    raise AssertionError("rotation synchronization timed out")
            return fixtures.response()
        self.client.transport = transport
        with patch("api_client.resolve_daily_adapter", wraps=resolve_daily_adapter) as resolver, ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(self.client.fetch, self.p, self.c)
            try:
                self.assertTrue(arrived.wait(5))
                updated = self.keys.rotate(self.p, "v2", {"KEY_A": "FAKE-A-V2"}, now=self.clock.wall())
            finally:
                released.set()
            old = pending.result(timeout=10)
            new = self.client.fetch(updated, self.c)
            self.assertEqual(resolver.call_count, 2)
        self.assertEqual([parse_qs(urlsplit(url).query)["apikey"] for url in urls], [["FAKE-SECRET-A"], ["FAKE-A-V2"]])
        self.assertNotEqual(old.config_version, new.config_version)
        self.assertEqual(old.payload_hash, new.payload_hash)
        self.assertEqual(old.prices, new.prices)
        with self.assertRaises(ApiFailure) as caught:
            self.client.fetch(self.p, self.c)
        self.assertEqual(caught.exception.code, "credential_superseded")
        self.assertEqual(len(urls), 2)
        with closing(sqlite3.connect(self.state.path)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*),SUM(cost) FROM attempts").fetchone(), (2, 2))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM scopes").fetchone(), (1,))


class AdapterBindingLoaderTests(unittest.TestCase):
    setUp = runtime_fixtures.RuntimeRegistryTests.setUp
    load = runtime_fixtures.RuntimeRegistryTests.load
    two_sources = runtime_fixtures.RuntimeRegistryTests.two_sources

    def test_missing_and_explicit_profile_id_bind_to_entry_without_hash_change(self):
        for explicit in (False, True):
            with self.subTest(explicit=explicit):
                p_data = self.registry["sources"][0]["profile"]
                if explicit:
                    p_data["adapter_id"] = ALPHA
                else:
                    p_data.pop("adapter_id", None)
                with patch("api_runtime.SourceProfile", wraps=SourceProfile) as constructor, \
                        patch("api_runtime.resolve_daily_adapter", wraps=resolve_daily_adapter) as resolver:
                    client, p, c = self.load()
                try:
                    self.assertEqual(constructor.call_args.kwargs["adapter_id"], ALPHA)
                    resolver.assert_called_once_with(p, c)
                    self.assertEqual(p.config_hash, "e4b68301d950c6cdbac903de9f8ec5c14f80b2707cbd7abd313c8e063e0d0849")
                    self.assertNotIn("adapter_id", p.metadata())
                finally:
                    client.credentials.close(); client.state.close()

    def test_future_valid_identity_conflict_is_rejected_before_state_or_keys(self):
        # The real constructor currently rejects this ID sooner. A marked
        # profile double reaches the future identity-conflict guard explicitly.
        for selected in (True, False):
            registry = deepcopy(self.registry)
            if not selected:
                other = deepcopy(registry["sources"][0]); other["profile"]["source_id"] = "source-b"
                registry["sources"].append(other)
            bad_source = "source-a" if selected else "source-b"
            registry["sources"][-1]["profile"]["adapter_id"] = "future-accepted-id"
            def constructor(**values):
                if values["source_id"] == bad_source:
                    self.assertEqual(values["adapter_id"], "future-accepted-id")
                    return RawBindingProfileDouble(**values)
                return SourceProfile(**values)
            with self.subTest(selected=selected), patch("api_runtime.SourceProfile", side_effect=constructor), \
                    patch("api_runtime.StateStore", side_effect=AssertionError("state forbidden")) as state, \
                    patch("api_runtime.CredentialStore", side_effect=AssertionError("credentials forbidden")) as keys:
                with self.assertRaises(ApiFailure) as caught:
                    self.load(registry, source_id="source-a")
                self.assertEqual(caught.exception.code, "adapter_identity_mismatch")
                state.assert_not_called(); keys.assert_not_called()
                self.assertFalse((self.folder / "state.sqlite").exists())

    def test_unselected_source_keeps_adapter_endpoint_environment_guards(self):
        self.two_sources()
        for field, value, code in (("adapter_id", "unknown-id", "unsupported_adapter"),
                                   ("endpoint", "https://other.invalid/query", "adapter_endpoint_mismatch"),
                                   ("environment", "test", "environment_mismatch")):
            registry = deepcopy(self.registry)
            registry["sources"][1]["profile"][field] = value
            with self.subTest(field=field), patch("api_runtime.StateStore", side_effect=AssertionError("state forbidden")) as state, \
                    patch("api_runtime.CredentialStore", side_effect=AssertionError("credentials forbidden")) as keys:
                with self.assertRaises(ApiFailure) as caught:
                    self.load(registry, source_id="source-a")
                self.assertEqual(caught.exception.code, code)
                state.assert_not_called(); keys.assert_not_called()
                self.assertFalse((self.folder / "state.sqlite").exists())


class AdapterFetchQuotaPersistenceTests(unittest.TestCase):
    setUp = quota_fixtures.QuotaJsonIntegrationTests.setUp
    registry = quota_fixtures.QuotaJsonIntegrationTests.registry
    load = quota_fixtures.QuotaJsonIntegrationTests.load
    close_client = staticmethod(quota_fixtures.QuotaJsonIntegrationTests.close_client)
    snapshot = staticmethod(quota_fixtures.QuotaJsonIntegrationTests.snapshot)
    query = staticmethod(quota_fixtures.QuotaJsonIntegrationTests.query)
    assert_inventory = quota_fixtures.QuotaJsonIntegrationTests.assert_inventory

    def test_actual_fetch_keeps_both_quotas_inventory_and_consumption_after_reopen(self):
        registry = self.registry([quota_fixtures.rule(3, 60)], max_attempts_per_window=2, quota_window_seconds=10)
        clock = fixtures.Clock()
        def reopen():
            client, p = self.load(registry)
            client.wall, client.monotonic, client.sleep = clock.wall, clock.monotonic, clock.sleep
            client.transport = Mock(return_value=fixtures.response())
            return client, p
        client, p = reopen()
        for _ in range(2):
            client.fetch(p, fixtures.contract())
        before = self.snapshot(client.state)
        with self.assertRaises(ApiFailure) as caught:
            client.fetch(p, fixtures.contract())
        self.assertEqual(caught.exception.code, "quota")
        self.assertEqual(self.snapshot(client.state), before)
        self.assertEqual(client.transport.call_count, 2)
        self.close_client(client)
        client, p = reopen()
        self.assertEqual(self.snapshot(client.state), before)
        self.assert_inventory(client.state, p)
        clock.elapsed = 10
        client.fetch(p, fixtures.contract())
        clock.elapsed = 21
        with self.assertRaises(ApiFailure) as caught:
            client.fetch(p, fixtures.contract())
        self.assertEqual(caught.exception.code, "quota")
        self.assertEqual(self.query(client.state, "SELECT COUNT(*),SUM(cost) FROM attempts"), [(3, 3)])
        before = self.snapshot(client.state)
        self.close_client(client)
        client, p = reopen()
        self.assertEqual(self.snapshot(client.state), before)
        self.assert_inventory(client.state, p)
        with self.assertRaises(ApiFailure) as caught:
            client.fetch(p, fixtures.contract())
        self.assertEqual(caught.exception.code, "quota")
        self.assertEqual(self.snapshot(client.state), before)
        client.transport.assert_not_called()


class _LoopbackHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        self.server.requests.append(parse_qs(urlsplit(self.path).query))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(self.server.body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(self.server.body)
        self.wfile.flush()
        self.close_connection = True


class _LoopbackServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, *args):
        # Deliberately do not print request URLs or synthetic key values.
        self.errors.append("server_error")


class AdapterLoopbackTLSTests(unittest.TestCase):
    setUp = review_fixtures.ClientAdapterInsertionTests.setUp

    @contextmanager
    def server(self):
        fixture = Path(__file__).parent / "fixtures"
        certificate, key = fixture / "adapter_test_tls.pem", fixture / "adapter_test_tls.key"
        server = _LoopbackServer(("127.0.0.1", 0), _LoopbackHandler)
        server.body = fixtures.response().body
        server.requests, server.errors = [], []
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(certificate), str(key))
        server.socket = context.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.025})
        thread.start()
        try:
            yield server, "https://localhost:" + str(server.server_port), str(certificate)
        finally:
            server.shutdown(); server.server_close(); thread.join(2)
            self.assertFalse(thread.is_alive())

    def test_real_client_adapter_transport_dns_tls_and_json_on_loopback(self):
        children_before = {child.pid for child in multiprocessing.active_children()}
        with self.server() as (server, origin, ca):
            p = replace(self.p, endpoint=origin + "/query")
            # Actual api_transport.request, actual worker, DNS and TLS. Only
            # the historical market wall clock is injected for the fixture.
            client = ApiClient(self.state, self.keys, policy=FetchPolicy(total_seconds=10, socket_seconds=3, max_attempts=1),
                               wall=lambda: fixtures.NOW.timestamp(),
                               transport_policy=TransportPolicy(origin + "/", test_mode=True, ca_file=ca))
            batch = client.fetch(p, self.c)
            self.assertEqual(batch.prices["2026-09-30"], fixtures.ROW)
            self.assertEqual(batch.received_at, fixtures.NOW)
            self.assertEqual(batch.payload_hash, hashlib.sha256(server.body).hexdigest())
            self.assertEqual(server.requests, [{"function": ["TIME_SERIES_DAILY"], "symbol": ["TEST"],
                "outputsize": ["compact"], "apikey": ["FAKE-SECRET-A"]}])
            self.assertEqual(server.errors, [])
        self.assertEqual({child.pid for child in multiprocessing.active_children()}, children_before)
        self.assertEqual(self.state.health(self.p.source_id)["active_jobs"], 0)
