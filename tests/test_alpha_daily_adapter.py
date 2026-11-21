"""Pure Alpha adapter contract before client/loader binding; synthetic inputs."""
import hashlib
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import http.client
import io
import logging
from pathlib import Path
import pickle
import socket
import time
from types import MappingProxyType, SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import parse_qsl, urlencode, urlsplit
import urllib.request

import api_adapters
import api_contracts
from api_adapters import AlphaDailyAdapter
from api_contracts import ApiFailure, alpha_daily
from test_api_client import NOW, ROW, contract, payload


LEGACY_URL_SOURCE = Path(__file__).parent / "fixtures" / "alpha_daily_legacy_url_expression.txt"


def client_url_expression():
    """Frozen pre-adapter expression, verified on 2026-10-30 in run-62q25nke.

    Exact artifact: alpha-daily-adapter-20261030/run-62q25nke/
    legacy-client-url-expression.py. Do not derive this from the new client.
    """
    source = LEGACY_URL_SOURCE.read_bytes()
    if hashlib.sha256(source).hexdigest() != "2cbf3d4407fc2e87727c1cc2ce54a8f4f0a39e79eba64be05d79ca34c20298cc":
        raise AssertionError("Verified legacy URL fixture changed")
    return compile(source, str(LEGACY_URL_SOURCE), "eval")


class DateTimeMeta(type):
    def __instancecheck__(cls, value):
        return isinstance(value, datetime)


class NoClockDateTime(datetime, metaclass=DateTimeMeta):
    @classmethod
    def now(cls, *args, **kwargs):
        raise AssertionError("wall clock forbidden")

    utcnow = now
    today = now


class NoClockDate(date):
    fromisoformat = staticmethod(date.fromisoformat)

    @classmethod
    def today(cls):
        raise AssertionError("calendar clock forbidden")


@contextmanager
def no_side_effects():
    output, errors = io.StringIO(), io.StringIO()
    blocked = (
        (socket, "create_connection"), (socket, "getaddrinfo"), (socket.socket, "connect"),
        (http.client.HTTPConnection, "request"), (urllib.request, "urlopen"),
        (time, "time"), (time, "monotonic"), (time, "perf_counter"),
        (time, "process_time"), (time, "sleep"), (logging.Logger, "_log"),
    )
    with ExitStack() as stack:
        stack.enter_context(redirect_stdout(output))
        stack.enter_context(redirect_stderr(errors))
        for target, name in blocked:
            stack.enter_context(patch.object(target, name, side_effect=AssertionError(name + " forbidden")))
        stack.enter_context(patch("builtins.print", side_effect=AssertionError("print forbidden")))
        stack.enter_context(patch("builtins.open", side_effect=AssertionError("file I/O forbidden")))
        stack.enter_context(patch.object(api_contracts, "datetime", NoClockDateTime))
        stack.enter_context(patch.object(api_contracts, "date", NoClockDate))
        yield output, errors
    if output.getvalue() or errors.getvalue():
        raise AssertionError("adapter wrote to stdout/stderr")


class AlphaDailyAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.old_url = staticmethod(client_url_expression())

    @staticmethod
    def legacy_url(endpoint, symbol, secret):
        return eval(AlphaDailyAdapterTests.old_url, {"urlencode": urlencode},
                    {"profile": SimpleNamespace(endpoint=endpoint),
                     "contract": SimpleNamespace(symbol=symbol),
                     "credential": SimpleNamespace(secret=secret)})

    def reject(self, data, expected, *, c=None, received=NOW):
        c = contract() if c is None else c
        messages = []
        for parser in (AlphaDailyAdapter.parse, alpha_daily):
            candidate = deepcopy(data)
            before = pickle.dumps((candidate, c))
            with self.assertRaises(ApiFailure) as caught:
                parser(candidate, c, received)
            self.assertEqual(caught.exception.code, expected)
            self.assertEqual(pickle.dumps((candidate, c)), before)
            messages.append((type(caught.exception), str(caught.exception), caught.exception.status))
        self.assertEqual(messages[0], messages[1])

    def test_exact_url_matches_handwritten_expected_and_client_expression(self):
        actual = AlphaDailyAdapter.build_url("https://a.example.test/query", "TEST", "FAKE-KEY")
        self.assertEqual(actual, "https://a.example.test/query?function=TIME_SERIES_DAILY&symbol=TEST&outputsize=compact&apikey=FAKE-KEY")
        self.assertEqual(actual, self.legacy_url("https://a.example.test/query", "TEST", "FAKE-KEY"))

    def test_special_character_url_matches_handwritten_bytes(self):
        endpoint, symbol, secret = "https://b.example.test/query", "A B+&=/?:#%", "FAKE +&=/%?#"
        actual = AlphaDailyAdapter.build_url(endpoint, symbol, secret)
        self.assertEqual(actual, "https://b.example.test/query?function=TIME_SERIES_DAILY&symbol=A+B%2B%26%3D%2F%3F%3A%23%25&outputsize=compact&apikey=FAKE+%2B%26%3D%2F%25%3F%23")
        self.assertEqual(actual, self.legacy_url(endpoint, symbol, secret))

    def test_encoding_round_trips_without_extra_query_parameters(self):
        # Encoding stress probes need not be valid DailyContract symbols.
        for endpoint in ("https://a.example.test/query", "https://b.example.test:8443/v2/daily"):
            for symbol in ("TEST", "A B", "A+B", "A&apikey=INJECTED", "Å漢字/%?#", ""):
                for secret in ("FAKE-A", " FAKE +/%?=&KEY ", "FAKE#fragment", "FAKE&symbol=OTHER", "FAKE-å漢字", "FAKE\n\tKEY", ""):
                    with self.subTest(endpoint=endpoint, symbol=symbol, secret=secret):
                        actual = AlphaDailyAdapter.build_url(endpoint, symbol, secret)
                        self.assertEqual(actual, self.legacy_url(endpoint, symbol, secret))
                        parts = urlsplit(actual)
                        self.assertEqual((parts.scheme, parts.netloc, parts.path), urlsplit(endpoint)[:3])
                        self.assertEqual(parts.fragment, "")
                        self.assertEqual(parse_qsl(parts.query, keep_blank_values=True),
                            [("function", "TIME_SERIES_DAILY"), ("symbol", symbol),
                             ("outputsize", "compact"), ("apikey", secret)])

    def test_alternating_requests_and_instances_do_not_mix_source_values(self):
        first, second = AlphaDailyAdapter(), AlphaDailyAdapter()
        sources = (("https://a.example.test/query", "AAA", "FAKE-A"),
                   ("https://b.example.test:8443/daily", "BBB", "FAKE-B+&"))
        saved = []
        for index in (0, 1, 0, 1, 1, 0):
            endpoint, symbol, secret = sources[index]
            adapter = first if index else second
            actual = adapter.build_url(endpoint, symbol, secret)
            saved.append(actual)
            self.assertEqual(actual, self.legacy_url(endpoint, symbol, secret))
            self.assertEqual(dict(parse_qsl(urlsplit(actual).query))["apikey"], secret)
        self.assertEqual(saved[0], saved[2])
        self.assertEqual(saved[1], saved[3])
        self.assertEqual(saved[0], AlphaDailyAdapter().build_url(*sources[0]))

    def test_instances_retain_no_per_request_attributes(self):
        value = AlphaDailyAdapter()
        value.build_url("https://a.example.test/query", "TEST", "FAKE-NOT-RETAINED")
        value.parse(payload(), contract(), NOW)
        self.assertFalse(hasattr(value, "__dict__"))
        with self.assertRaises(AttributeError):
            value.secret = "FAKE-KEY"
        self.assertEqual(value.adapter_id, "alphavantage-daily-v1")

    def test_valid_parse_matches_prices_and_independent_observation_time(self):
        data, c = payload(), contract()
        before = pickle.dumps((data, c))
        prices, observed = AlphaDailyAdapter.parse(data, c, NOW)
        expected_prices, expected_observed = alpha_daily(deepcopy(data), c, NOW)
        self.assertEqual(prices, expected_prices)
        self.assertEqual(observed, expected_observed)
        self.assertEqual({day: dict(row) for day, row in prices.items()}, {"2026-09-30": ROW})
        self.assertEqual(observed, datetime(2026, 9, 30, 20, tzinfo=timezone.utc))
        self.assertNotEqual(observed, NOW)
        self.assertEqual(pickle.dumps((data, c)), before)

    def test_two_sessions_and_early_close_keep_prices_and_close_instant(self):
        data = payload()
        data["Time Series (Daily)"]["2026-09-29"] = dict(ROW, **{"4. close": "10.00"})
        c = contract(minimum_sessions=2, sessions=(
            (date(2026, 9, 29), datetime(2026, 9, 29, 20, tzinfo=timezone.utc)),
            (date(2026, 9, 30), datetime(2026, 9, 30, 18, tzinfo=timezone.utc))))
        actual = AlphaDailyAdapter.parse(data, c, NOW)
        self.assertEqual(actual, alpha_daily(data, c, NOW))
        self.assertEqual(len(actual[0]), 2)
        self.assertEqual(actual[1], datetime(2026, 9, 30, 18, tzinfo=timezone.utc))

    def test_accepted_timezone_alias_matches_existing_parser(self):
        data = payload()
        data["Meta Data"]["5. Time Zone"] = "US/Eastern"
        c = contract(timezone="America/New_York", accepted_timezones=("US/Eastern",))
        self.assertEqual(AlphaDailyAdapter.parse(data, c, NOW), alpha_daily(data, c, NOW))

    def test_alternating_parses_keep_independent_symbols_and_results(self):
        adapters = (AlphaDailyAdapter(), AlphaDailyAdapter())
        results = []
        for index, symbol in enumerate(("AAA", "BBB", "AAA", "BBB")):
            c = contract(symbol=symbol, instrument_id="TESTX:" + symbol)
            data = payload()
            data["Meta Data"]["2. Symbol"] = symbol
            closing = "10.50" if symbol == "AAA" else "10.75"
            data["Time Series (Daily)"]["2026-09-30"]["4. close"] = closing
            result = adapters[index % 2].parse(data, c, NOW)
            self.assertEqual(result, alpha_daily(data, c, NOW))
            self.assertEqual(result[0]["2026-09-30"]["4. close"], closing)
            results.append(result)
        self.assertEqual(results[0], results[2])
        self.assertEqual(results[1], results[3])
        self.assertNotEqual(results[0][0], results[1][0])
        self.assertIsNot(results[0][0], results[2][0])

    def test_provider_errors_and_formats_keep_exact_codes(self):
        for field, code in (("Error Message", "provider_error"),
                            ("Information", "provider_information"), ("Note", "provider_note")):
            for text in ("FAKE-PROVIDER-TEXT", "", " ", None, 123, []):
                with self.subTest(field=field, text=text):
                    data = payload()
                    data[field] = text
                    self.reject(data, code if isinstance(text, str) and text.strip() else "provider_error_format")

    def test_provider_error_priority_is_preserved(self):
        data = payload()
        data.update({"Error Message": "FAKE-ERROR", "Information": "FAKE-INFO", "Note": "FAKE-NOTE"})
        self.reject(data, "provider_error")

    def test_bad_root_series_rows_and_metadata_keep_exact_codes(self):
        for data in (None, [], "json text", 1):
            with self.subTest(root=type(data).__name__):
                self.reject(data, "root_type")
        for value, expected in (([], "series_type"), (None, "missing_series"), ({}, "missing_series")):
            data = payload()
            data["Time Series (Daily)"] = value
            self.reject(data, expected)
        data = payload()
        data["Time Series (Daily)"]["2026-09-30"] = []
        self.reject(data, "row_type")
        for value in (None, [], "metadata"):
            data = payload()
            data["Meta Data"] = value
            self.reject(data, "missing_metadata")

    def test_wrong_symbol_timezone_and_explicit_metadata_keep_exact_codes(self):
        for field, value, expected in (("2. Symbol", "OTHER", "instrument_mismatch"),
                ("5. Time Zone", "Europe/Stockholm", "timezone_mismatch"),
                ("currency", "EUR", "metadata_mismatch"), ("exchange", "OTHER", "metadata_mismatch"),
                ("price_basis", "adjusted", "metadata_mismatch")):
            with self.subTest(field=field):
                data = payload()
                data["Meta Data"][field] = value
                self.reject(data, expected)

    def test_incompatible_schema_and_price_basis_are_not_silently_changed(self):
        for changes in ({"schema_version": "other-v2"}, {"price_basis": "adjusted"}):
            with self.subTest(changes=changes):
                self.reject(payload(), "adapter_contract_mismatch", c=contract(**changes))

    def test_old_and_future_missing_required_session_keep_incomplete_code(self):
        for day in ("2026-09-29", "2026-10-01", "2020-01-01"):
            with self.subTest(day=day):
                data = payload()
                data["Time Series (Daily)"] = {day: dict(ROW)}
                self.reject(data, "incomplete_series")

    def test_future_row_and_old_refresh_keep_stale_or_future_code(self):
        data = payload()
        data["Time Series (Daily)"]["2026-10-01"] = dict(ROW)
        self.reject(data, "stale_or_future_data")
        data = payload()
        data["Meta Data"]["3. Last Refreshed"] = "2026-09-29"
        self.reject(data, "stale_or_future_data")

    def test_bad_dates_and_uncovered_non_session_rows_keep_exact_codes(self):
        for day in ("bad-date", "20260929", "2026-09-31"):
            data = payload()
            data["Time Series (Daily)"][day] = dict(ROW)
            self.reject(data, "invalid_date")
        for refreshed in (None, "bad-date", "2026-09-30T20:00:00"):
            data = payload()
            data["Meta Data"]["3. Last Refreshed"] = refreshed
            self.reject(data, "invalid_date")
        data = payload()
        data["Time Series (Daily)"]["2020-01-01"] = dict(ROW)
        self.reject(data, "calendar_coverage")
        data = payload()
        data["Time Series (Daily)"]["2026-09-29"] = dict(ROW)
        c = contract(sessions=((date(2026, 9, 30), datetime(2026, 9, 30, 20, tzinfo=timezone.utc)),))
        self.reject(data, "non_session_data", c=c)

    def test_pagination_row_limit_and_missing_sessions_are_preserved(self):
        for name in ("next", "next_page", "cursor", "pagination"):
            data = payload()
            data[name] = "FAKE-CURSOR"
            self.reject(data, "unsupported_pagination")
        data = payload()
        data["Time Series (Daily)"]["2026-09-29"] = dict(ROW)
        self.reject(data, "row_limit", c=contract(max_rows=1))
        self.reject(payload(), "incomplete_series", c=contract(minimum_sessions=2))

    def test_result_retains_two_levels_of_read_only_mapping(self):
        result = AlphaDailyAdapter.parse(payload(), contract(), NOW)
        self.assertIs(type(result), tuple)
        prices, _ = result
        self.assertIsInstance(prices, MappingProxyType)
        self.assertIsInstance(prices["2026-09-30"], MappingProxyType)
        with self.assertRaises(TypeError):
            prices["2026-09-29"] = {}
        with self.assertRaises(TypeError):
            prices["2026-09-30"]["4. close"] = "99"
        with self.assertRaises(TypeError):
            del prices["2026-09-30"]["4. close"]

    def test_later_input_mutations_do_not_change_previous_result(self):
        data = payload()
        result = AlphaDailyAdapter.parse(data, contract(), NOW)
        data["Meta Data"]["2. Symbol"] = "OTHER"
        data["Time Series (Daily)"]["2026-09-30"]["4. close"] = "99"
        data["Time Series (Daily)"].clear()
        self.assertEqual(dict(result[0]["2026-09-30"]), ROW)
        self.assertEqual(result[1], datetime(2026, 9, 30, 20, tzinfo=timezone.utc))

    def test_received_alone_changes_session_assessment(self):
        c, data = contract(), payload()
        before = datetime(2026, 9, 30, 19, 59, 59, tzinfo=timezone.utc)
        close = datetime(2026, 9, 30, 20, tzinfo=timezone.utc)
        after_next_close = datetime(2026, 10, 1, 21, tzinfo=timezone.utc)
        self.reject(data, "incomplete_series", c=c, received=before)
        self.assertEqual(AlphaDailyAdapter.parse(data, c, close), alpha_daily(data, c, close))
        self.assertEqual(AlphaDailyAdapter.parse(data, c, close)[1], close)
        self.reject(data, "incomplete_series", c=c, received=after_next_close)

    def test_received_timezone_and_invalid_clock_values_match_parser(self):
        local = NOW.astimezone(timezone(timedelta(hours=2)))
        self.assertEqual(AlphaDailyAdapter.parse(payload(), contract(), local),
                         AlphaDailyAdapter.parse(payload(), contract(), NOW))
        for received in (None, NOW.replace(tzinfo=None), "2026-09-30"):
            self.reject(payload(), "invalid_time", received=received)
        self.reject(payload(), "calendar_expired", received=datetime(2030, 1, 1, tzinfo=timezone.utc))
        self.reject(payload(), "no_completed_session", received=datetime(2026, 9, 29, 19, tzinfo=timezone.utc))
        self.reject(payload(), "stale_calendar", c=contract(max_age_seconds=1))

    def test_calls_have_no_network_clock_file_or_output_side_effects(self):
        data, c = payload(), contract()
        with no_side_effects():
            url = AlphaDailyAdapter.build_url("https://a.example.test/query", "TEST", "FAKE-KEY")
            result = AlphaDailyAdapter.parse(data, c, NOW)
            with self.assertRaises(ApiFailure):
                AlphaDailyAdapter.parse({"Error Message": "FAKE-PROVIDER-ERROR"}, c, NOW)
        self.assertIn("symbol=TEST", url)
        self.assertEqual(result[1], datetime(2026, 9, 30, 20, tzinfo=timezone.utc))

    def test_module_initialization_has_no_side_effects(self):
        source = Path(api_adapters.__file__).read_text(encoding="utf-8")
        code = compile(source, api_adapters.__file__, "exec")
        namespace = {"__name__": "isolated_alpha_adapter"}
        with no_side_effects():
            exec(code, namespace)
        self.assertEqual(namespace["AlphaDailyAdapter"].adapter_id, "alphavantage-daily-v1")
