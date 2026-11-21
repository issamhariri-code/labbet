"""All 22 previous fetch/main regression families through the hardened client.

Real HTTP boundary reader, JSON parser, client, credential/state and main code
are exercised. Only HTTP IO, runtime configuration and market-data persistence
are simulated. Original raw cause identities deliberately became secret-safe
ApiFailure codes; an empty row now fails already inside the validated client.
Actual TLS/worker IO lives in test_api_transport.py.
"""

import ast
from contextlib import redirect_stdout
from datetime import date, datetime, timezone
from decimal import Decimal
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import traceback
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch
import urllib.error
import urllib.parse

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT / "ingestion" / "main.py"
MARKET = ROOT / "ingestion" / "market_data.py"
sys.path.insert(0, str(ROOT / "ingestion"))

from api_client import ApiClient, FetchPolicy
from api_contracts import ApiFailure, DailyContract, PriceBatch
from api_profiles import CredentialStore, SourceProfile
from api_state import StateStore
import api_transport

KEY = "LABBET-TEST-KEY-NOT-REAL"
NOW = datetime(2026, 9, 30, 21, tzinfo=timezone.utc).timestamp()
spec = importlib.util.spec_from_file_location("labbet_real_market_data", MARKET)
market_data = importlib.util.module_from_spec(spec)
spec.loader.exec_module(market_data)


class MarketTimeoutTests(unittest.TestCase):
    def setUp(self):
        self.assertEqual(market_data.fetch_daily_prices.__code__.co_filename, str(MARKET))
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.sequence = 0
        self.connect = MagicMock()
        self.cursor = self.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        self.persist = Mock(return_value=SimpleNamespace(replayed=False))
        self.complete = {
            "1. open": "10.00", "2. high": "11.00", "3. low": "9.00",
            "4. close": "10.50", "5. volume": "1000",
        }
        self.prices = {"2026-09-30": self.complete}
        self.body = self.encode(self.prices)

    def encode(self, prices):
        return json.dumps({"Meta Data": {
            "2. Symbol": "TEST", "3. Last Refreshed": "2026-09-30", "5. Time Zone": "UTC",
        }, "Time Series (Daily)": prices}).encode("utf-8")

    def runtime(self, boundary):
        self.sequence += 1
        state = StateStore(Path(self.temp.name) / (str(self.sequence) + ".sqlite"))
        profile = SourceProfile(
            "alpha-test", "account-test", "test", "https://www.alphavantage.co/query",
            "ALPHAVANTAGE_API_KEY", "v1", "alpha-test-quota",
            currency="USD", exchange="TESTX", timezone="UTC", price_basis="raw",
            schema_version="alphavantage-daily-v1", rights_verified=True,
            rights_review_date="2026-09-29", review_due_date="2026-10-01",
            max_attempts_per_window=1000, circuit_failure_threshold=1000,
        )
        credentials = CredentialStore(state)
        credentials.install(profile, {"ALPHAVANTAGE_API_KEY": KEY})
        contract = DailyContract(
            "TEST", "test-instrument", "USD", "TESTX", "UTC", "raw", "calendar-v1",
            date(2026, 9, 29), date(2026, 10, 1),
            tuple((date(2026, 9, day), datetime(2026, 9, day, 20, tzinfo=timezone.utc))
                  for day in (29, 30)),
        )
        self.client = ApiClient(state, credentials, policy=FetchPolicy(max_attempts=1),
                                transport=boundary, wall=lambda: NOW, monotonic=lambda: 100.0)
        return self.client, profile, contract

    def network(self, phase=None, error=None, body=None):
        response = MagicMock()
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        response.status = 200
        response.getheader.return_value = None
        response.getheaders.return_value = [("Content-Type", "application/json")]
        response.read.return_value = self.body if body is None else body
        opener = Mock(return_value=response)
        if phase == "connection":
            opener.side_effect = error
        elif phase == "read":
            response.read.side_effect = error

        def boundary(url, **kwargs):
            try:
                with opener(url, **kwargs) as actual:
                    return api_transport._read_response(actual, kwargs["max_bytes"])
            except BaseException as raw:
                # Same normalization as the real process worker; no parser stub.
                raise api_transport._safe_error(raw) from None
        return boundary, opener, response

    def assert_request(self, opener, response, phase=None):
        opener.assert_called_once()
        self.assertEqual(len(opener.call_args.args), 1)
        url = urllib.parse.urlsplit(opener.call_args.args[0])
        self.assertEqual((url.scheme, url.netloc, url.path),
                         ("https", "www.alphavantage.co", "/query"))
        self.assertEqual(urllib.parse.parse_qs(url.query), {
            "function": ["TIME_SERIES_DAILY"], "symbol": ["TEST"],
            "outputsize": ["compact"], "apikey": [KEY],
        })
        kwargs = opener.call_args.kwargs
        self.assertEqual((kwargs["timeout"], kwargs["total_timeout"], kwargs["max_bytes"]),
                         (10, 30, 1048576))
        self.assertEqual(kwargs["policy"].origin, "https://www.alphavantage.co")
        if phase == "connection":
            response.__enter__.assert_not_called()
            response.read.assert_not_called()
            response.__exit__.assert_not_called()
        else:
            response.__enter__.assert_called_once_with()
            response.read.assert_called_once_with(1048577)
            response.__exit__.assert_called_once()

    def run_main(self):
        tree = ast.parse(MAIN.read_text(encoding="utf-8"), filename=str(MAIN))
        psycopg, storage = ModuleType("psycopg"), ModuleType("api_storage")
        psycopg.connect, storage.persist_batch = self.connect, self.persist
        with patch.dict(sys.modules, {"market_data": market_data, "psycopg": psycopg,
                                      "api_storage": storage}), redirect_stdout(io.StringIO()):
            namespace = {"__name__": "labbet_test_main", "__file__": str(MAIN)}
            exec(compile(tree, str(MAIN), "exec"), namespace)
            namespace["run"]("TEST")

    def exercise(self, *, body=None, error=None, phase=None, expected=None, main=False,
                 expected_status=None):
        self.connect.reset_mock()
        self.persist.reset_mock()
        boundary, opener, response = self.network(phase, error, body)
        runtime = self.runtime(boundary)
        with patch.object(market_data, "load_runtime", return_value=runtime) as load:
            if expected is None:
                result = self.run_main() if main else market_data.fetch_daily_prices("TEST")
            else:
                with self.assertRaises(ApiFailure) as caught:
                    self.run_main() if main else market_data.fetch_daily_prices("TEST")
                result = caught.exception
                self.assertEqual(result.code, expected)
                self.assertEqual(str(result), "API-hämtningen stoppades: " + expected)
                self.assertEqual(result.status, expected_status)
                self.assertIsNone(result.__cause__)
                self.assertNotIn(KEY, "".join(traceback.format_exception(result)))
                self.connect.assert_not_called()
                self.cursor.executemany.assert_not_called()
                self.persist.assert_not_called()
            load.assert_called_once_with("TEST")
        self.assertTrue(self.client.state._closed)
        self.assertTrue(self.client.credentials._closed)
        self.assert_request(opener, response, phase)
        return result

    def timeout_errors(self):
        return TimeoutError(KEY), urllib.error.URLError(TimeoutError(KEY))

    def other_errors(self):
        return urllib.error.URLError(KEY), urllib.error.URLError(OSError(KEY))

    def http_errors(self):
        return tuple(urllib.error.HTTPError("https://example.invalid/?key=" + KEY, code,
                                           KEY, None, None)
                     for code in (400, 401, 403, 404, 429, 500, 503, 504))

    def http_code(self, status):
        return {401: "authentication", 403: "permission_unknown", 429: "quota"}.get(status, "http")

    def invalid_json_responses(self):
        return (("empty", b""), ("html", b"<html>Service unavailable</html>"),
                ("broken", b'{"Time Series (Daily)":'))

    def invalid_encoding_responses(self):
        prefix = b'{"text": "'
        return (("invalid_start", prefix + b'\xff"}'),
                ("invalid_continuation", prefix + b'\xe2(\xa1"}'),
                ("truncated", prefix + b'\xe2\x82'))

    def nonobject_json_responses(self):
        return (("empty_list", b"[]"), ("list", b'[{"Time Series (Daily)": {}}]'),
                ("empty_text", b'""'), ("text", b'"kursdata"'), ("integer", b"0"),
                ("fraction", b"1.5"), ("true", b"true"), ("false", b"false"), ("null", b"null"))

    def invalid_daily_series_responses(self):
        values = (("empty_list", []), ("list", [self.complete]), ("empty_text", ""),
                  ("text", "kursdata"), ("zero_integer", 0), ("integer", 42),
                  ("zero_fraction", 0.0), ("fraction", 1.5), ("true", True), ("false", False))
        return tuple((kind, self.encode(value), "series_type") for kind, value in values) + (
            ("missing", b"{}", "missing_series"), ("null", self.encode(None), "missing_series"),
            ("empty_object", self.encode({}), "missing_series"))

    def invalid_daily_row_responses(self):
        values = (("empty_list", []), ("list", [self.complete]), ("empty_text", ""),
                  ("text", "kursdata"), ("zero_integer", 0), ("integer", 42),
                  ("zero_fraction", 0.0), ("fraction", 1.5), ("true", True),
                  ("false", False), ("null", None))
        for kind, value in values:
            for preceding in (False, True):
                prices = {"2026-09-29": self.complete.copy()} if preceding else {}
                prices["2026-09-30"] = value
                yield kind, preceding, self.encode(prices)

    def test_direct_timeout_at_connection_and_read(self):
        for phase in ("connection", "read"):
            with self.subTest(phase=phase):
                self.exercise(error=TimeoutError(KEY), phase=phase, expected="timeout")

    def test_wrapped_timeout_at_connection_and_read(self):
        for phase in ("connection", "read"):
            with self.subTest(phase=phase):
                self.exercise(error=urllib.error.URLError(TimeoutError(KEY)), phase=phase, expected="timeout")

    def test_other_url_errors_have_safe_network_classification(self):
        for phase in ("connection", "read"):
            for error in self.other_errors():
                with self.subTest(phase=phase, kind=type(error.reason).__name__):
                    self.exercise(error=error, phase=phase, expected="network")

    def test_http_errors_have_status_without_secret_cause(self):
        for phase in ("connection", "read"):
            for error in self.http_errors():
                with self.subTest(phase=phase, status=error.code):
                    self.exercise(error=error, phase=phase, expected=self.http_code(error.code),
                                  expected_status=error.code)

    def test_invalid_encoding_has_safe_error_from_actual_decoder(self):
        for kind, body in self.invalid_encoding_responses():
            with self.subTest(kind=kind):
                self.exercise(body=body, expected="invalid_encoding")

    def test_main_invalid_encoding_stops_before_database(self):
        for kind, body in self.invalid_encoding_responses():
            with self.subTest(kind=kind):
                self.exercise(body=body, expected="invalid_encoding", main=True)

    def test_nonobject_json_roots_have_clear_error(self):
        for kind, body in self.nonobject_json_responses():
            with self.subTest(kind=kind):
                self.exercise(body=body, expected="root_type")

    def test_main_nonobject_json_roots_stop_before_database(self):
        for kind, body in self.nonobject_json_responses():
            with self.subTest(kind=kind):
                self.exercise(body=body, expected="root_type", main=True)

    def test_invalid_or_empty_daily_series_have_clear_error(self):
        for kind, body, code in self.invalid_daily_series_responses():
            with self.subTest(kind=kind):
                self.exercise(body=body, expected=code)

    def test_main_invalid_or_empty_daily_series_stop_before_database(self):
        for kind, body, code in self.invalid_daily_series_responses():
            with self.subTest(kind=kind):
                self.exercise(body=body, expected=code, main=True)

    def test_invalid_daily_row_types_have_clear_safe_error(self):
        for kind, preceding, body in self.invalid_daily_row_responses():
            with self.subTest(kind=kind, preceding=preceding):
                self.exercise(body=body, expected="row_type")

    def test_main_invalid_daily_row_types_stop_before_database(self):
        for kind, preceding, body in self.invalid_daily_row_responses():
            with self.subTest(kind=kind, preceding=preceding):
                self.exercise(body=body, expected="row_type", main=True)

    def test_object_daily_rows_preserved_and_empty_object_rejected_earlier(self):
        prices = {"2026-09-29": self.complete.copy(),
                  "2026-09-30": dict(self.complete, **{"5. volume": "0"})}
        batch = self.exercise(body=self.encode(prices))
        self.assertIsInstance(batch, PriceBatch)
        self.assertEqual(batch.prices, prices)
        self.assertEqual(list(batch.prices.items()), list(prices.items()))
        with self.assertRaises(TypeError):
            batch.prices["2026-09-30"]["5. volume"] = "999"
        self.exercise(body=self.encode({"2026-09-30": {}}), expected="invalid_prices")

    def test_main_multiple_object_rows_keep_values_types_and_order(self):
        prices = {"2026-09-29": self.complete.copy(), "2026-09-30": {
            "1. open": "20.00", "2. high": "22.00", "3. low": "18.00",
            "4. close": "21.00", "5. volume": 0,
        }}
        self.exercise(body=self.encode(prices), main=True)
        self.connect.assert_called_once_with(connect_timeout=10,
            options="-c statement_timeout=15000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=30000")
        self.persist.assert_called_once()
        rows = self.persist.call_args.kwargs["rows"]
        self.assertEqual(rows, [
            ("TEST", date(2026, 9, 29), Decimal("10.00"), Decimal("11.00"),
             Decimal("9.00"), Decimal("10.50"), 1000),
            ("TEST", date(2026, 9, 30), Decimal("20.00"), Decimal("22.00"),
             Decimal("18.00"), Decimal("21.00"), 0),
        ])
        for row in rows:
            self.assertEqual([type(value) for value in row],
                             [str, date, Decimal, Decimal, Decimal, Decimal, int])

    def test_main_empty_row_object_keeps_missing_field_validation(self):
        for preceding in (False, True):
            with self.subTest(preceding=preceding):
                prices = {"2026-09-29": self.complete.copy()} if preceding else {}
                prices["2026-09-30"] = {}
                self.exercise(body=self.encode(prices), expected="invalid_prices", main=True)

    def test_successful_fetch_uses_actual_json_and_expected_request(self):
        batch = self.exercise()
        self.assertIsInstance(batch, PriceBatch)
        self.assertEqual(batch.prices, self.prices)
        self.assertEqual((batch.source_id, batch.instrument_id, batch.symbol),
                         ("alpha-test", "test-instrument", "TEST"))

    def test_invalid_json_has_safe_error_from_actual_decoder(self):
        for kind, body in self.invalid_json_responses():
            with self.subTest(kind=kind):
                self.exercise(body=body, expected="invalid_json")

    def test_main_timeouts_stop_before_database(self):
        for phase in ("connection", "read"):
            for error in self.timeout_errors():
                with self.subTest(phase=phase, kind=type(error).__name__):
                    self.exercise(error=error, phase=phase, expected="timeout", main=True)

    def test_main_other_url_errors_stop_before_database(self):
        for phase in ("connection", "read"):
            for error in self.other_errors():
                with self.subTest(phase=phase, kind=type(error.reason).__name__):
                    self.exercise(error=error, phase=phase, expected="network", main=True)

    def test_main_http_errors_stop_before_database(self):
        for phase in ("connection", "read"):
            for error in self.http_errors():
                with self.subTest(phase=phase, status=error.code):
                    self.exercise(error=error, phase=phase, expected=self.http_code(error.code),
                                  expected_status=error.code, main=True)

    def test_main_success_reaches_database_with_expected_row(self):
        self.exercise(main=True)
        self.connect.assert_called_once_with(connect_timeout=10,
            options="-c statement_timeout=15000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=30000")
        self.persist.assert_called_once()
        rows = self.persist.call_args.kwargs["rows"]
        self.assertEqual(rows, [("TEST", date(2026, 9, 30), Decimal("10.00"), Decimal("11.00"),
                                 Decimal("9.00"), Decimal("10.50"), 1000)])
        self.assertEqual([type(value) for value in rows[0]],
                         [str, date, Decimal, Decimal, Decimal, Decimal, int])
        self.assertEqual(self.persist.call_args.kwargs["source_id"], "alpha-test")
        self.assertEqual(self.persist.call_args.kwargs["instrument_id"], "test-instrument")

    def test_main_invalid_json_stops_before_database(self):
        for kind, body in self.invalid_json_responses():
            with self.subTest(kind=kind):
                self.exercise(body=body, expected="invalid_json", main=True)


if __name__ == "__main__":
    unittest.main()
