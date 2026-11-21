"""Regression probes for the corrected client/runtime insertion blocks."""
from contextlib import closing
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

from api_adapters import AlphaDailyAdapter, resolve_daily_adapter
from api_client import ApiClient, FetchPolicy
from api_contracts import ApiFailure
from api_profiles import CredentialStore
from api_state import StateStore
from api_transport import TransportPolicy, TransportResponse
from test_api_client import Clock, NOW, ROW, contract, payload, profile, response
import test_api_runtime as runtime_tests


class ClientAdapterInsertionTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.state = StateStore(Path(temp.name) / "state.sqlite")
        self.addCleanup(self.state.close)
        self.keys = CredentialStore(self.state)
        self.addCleanup(self.keys.close)
        self.p, self.c, self.clock = profile(), contract(), Clock()
        self.keys.install(self.p, {"KEY_A": "FAKE-SECRET-A"})
        self.transport = Mock(return_value=response())
        self.client = ApiClient(self.state, self.keys, policy=FetchPolicy(max_attempts=1),
            transport=self.transport, monotonic=self.clock.monotonic,
            wall=self.clock.wall, sleep=self.clock.sleep, jitter=lambda: 1)

    def snapshot(self):
        with closing(sqlite3.connect(self.state.path)) as db:
            return tuple(db.iterdump())

    def test_fetch_resolves_builds_and_parses_once_in_correct_order(self):
        with patch("api_client.resolve_daily_adapter", wraps=resolve_daily_adapter) as resolver, \
                patch.object(AlphaDailyAdapter, "build_url", wraps=AlphaDailyAdapter.build_url) as build, \
                patch.object(AlphaDailyAdapter, "parse", wraps=AlphaDailyAdapter.parse) as parse:
            batch = self.client.fetch(self.p, self.c)
            resolver.assert_called_once_with(self.p, self.c)
            build.assert_called_once_with(self.p.endpoint, self.c.symbol, "FAKE-SECRET-A")
            parse.assert_called_once_with(payload(), self.c, NOW)
        self.assertEqual(batch.prices["2026-09-30"], ROW)
        self.assertEqual(batch.received_at, NOW)
        self.assertEqual(batch.observed_at, datetime(2026, 9, 30, 20, tzinfo=timezone.utc))
        self.assertEqual(self.state.health(self.p.source_id)["active_jobs"], 0)

    def test_http_error_is_checked_before_json_or_adapter_parse(self):
        self.transport.return_value = TransportResponse(503, {"retry-after": "2"}, b"not JSON")
        with patch.object(AlphaDailyAdapter, "parse", side_effect=AssertionError("parse forbidden")) as parse:
            with self.assertRaises(ApiFailure) as caught:
                self.client.fetch(self.p, self.c)
            self.assertEqual((caught.exception.code, caught.exception.status), ("http", 503))
            parse.assert_not_called()
        self.assertEqual(self.state.health(self.p.source_id)["active_jobs"], 0)

    def test_json_decoding_precedes_adapter_parse(self):
        self.transport.return_value = TransportResponse(200, {}, b"not JSON")
        with patch.object(AlphaDailyAdapter, "parse", side_effect=AssertionError("parse forbidden")) as parse:
            with self.assertRaises(ApiFailure) as caught:
                self.client.fetch(self.p, self.c)
            self.assertEqual(caught.exception.code, "invalid_json")
            parse.assert_not_called()
        self.assertEqual(self.state.health(self.p.source_id)["active_jobs"], 0)

    def test_calendar_errors_precede_register_credentials_and_transport(self):
        cases = ((contract(calendar_valid_until=date(2026, 9, 29), sessions=(
                    (date(2026, 9, 29), datetime(2026, 9, 29, 20, tzinfo=timezone.utc)),)), "calendar_expired"),
                 (contract(max_age_seconds=1), "stale_calendar"),
                 (contract(sessions=((date(2026, 10, 1), datetime(2026, 10, 1, 20, tzinfo=timezone.utc)),)),
                  "no_completed_session"))
        before = self.snapshot()
        for c, code in cases:
            with self.subTest(code=code), \
                    patch.object(self.state, "register", side_effect=AssertionError("register forbidden")) as register, \
                    patch.object(self.keys, "snapshot", side_effect=AssertionError("key read forbidden")) as snapshot:
                with self.assertRaises(ApiFailure) as caught:
                    self.client.fetch(self.p, c)
                self.assertEqual(caught.exception.code, code)
                register.assert_not_called()
                snapshot.assert_not_called()
            self.assertEqual(self.snapshot(), before)
        self.transport.assert_not_called()

    def test_policy_errors_precede_register_credentials_and_transport(self):
        production = replace(self.p, environment="production", endpoint="https://www.alphavantage.co/query")
        cases = ((self.p, [], "configuration"), (self.p, False, "configuration"),
                 (self.p, TransportPolicy("https://source-a.invalid", test_mode=1), "configuration"),
                 (self.p, TransportPolicy("https://source-a.invalid", test_mode=False), "environment_mismatch"),
                 (self.p, TransportPolicy("https://other.invalid", test_mode=True), "configuration"),
                 (production, TransportPolicy("https://www.alphavantage.co", ca_file="FAKE-CA"), "configuration"))
        before = self.snapshot()
        for p, policy, code in cases:
            self.client.transport_policy = policy
            with self.subTest(code=code, policy_type=type(policy).__name__), \
                    patch.object(self.state, "register", side_effect=AssertionError("register forbidden")) as register, \
                    patch.object(self.keys, "snapshot", side_effect=AssertionError("key read forbidden")) as snapshot:
                with self.assertRaises(ApiFailure) as caught:
                    self.client.fetch(p, self.c)
                self.assertEqual(caught.exception.code, code)
                register.assert_not_called()
                snapshot.assert_not_called()
            self.assertEqual(self.snapshot(), before)
        self.transport.assert_not_called()


class RuntimeAdapterInsertionTests(unittest.TestCase):
    setUp = runtime_tests.RuntimeRegistryTests.setUp
    load = runtime_tests.RuntimeRegistryTests.load

    def test_loader_converts_sessions_before_resolving_and_returns_single_match(self):
        with patch("api_runtime.resolve_daily_adapter", wraps=resolve_daily_adapter) as resolver:
            client, p, c = self.load()
            try:
                resolver.assert_called_once_with(p, c)
                self.assertEqual(c.fingerprint, contract().fingerprint)
                self.assertIs(type(c.sessions), tuple)
                self.assertIs(type(c.accepted_timezones), tuple)
                self.assertTrue(all(type(day) is date and type(close) is datetime and close.tzinfo is not None
                                    for day, close in c.sessions))
            finally:
                client.credentials.close()
                client.state.close()

    def test_loader_contract_rejection_precedes_state_and_credentials(self):
        for field, value, code in (("currency", "EUR", "source_contract_mismatch"),
                                   ("price_basis", "adjusted", "adapter_contract_mismatch")):
            with self.subTest(field=field):
                self.registry["sources"][0]["profile"][field] = value
                if field == "price_basis":
                    self.registry["sources"][0]["profile"]["currency"] = "USD"
                    self.registry["sources"][0]["instruments"][0][field] = value
                with patch("api_runtime.StateStore", side_effect=AssertionError("state forbidden")) as state, \
                        patch("api_runtime.CredentialStore", side_effect=AssertionError("credentials forbidden")) as credentials:
                    with self.assertRaises(ApiFailure) as caught:
                        self.load()
                    self.assertEqual(caught.exception.code, code)
                    state.assert_not_called()
                    credentials.assert_not_called()
                self.assertFalse((self.folder / "state.sqlite").exists())
