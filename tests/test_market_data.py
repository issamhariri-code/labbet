"""Testar faktisk kurshämtning och main-flödet med simulerat nätverk/databas."""

import ast
from contextlib import redirect_stdout
from datetime import date
from decimal import Decimal
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest.mock import MagicMock, Mock, patch
import urllib.error
import urllib.parse


ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT / "ingestion" / "main.py"
MARKET = ROOT / "ingestion" / "market_data.py"
KEY = "LABBET-TEST-KEY-NOT-REAL"
EXPECTED_MESSAGE = "TEST: tidsgränsen för hämtning av kursdata överskreds"

spec = importlib.util.spec_from_file_location("labbet_real_market_data", MARKET)
market_data = importlib.util.module_from_spec(spec)
spec.loader.exec_module(market_data)


class MarketTimeoutTests(unittest.TestCase):
    def setUp(self):
        self.assertEqual(market_data.fetch_daily_prices.__code__.co_filename, str(MARKET))
        self.env = patch.dict(os.environ, {'ALPHAVANTAGE_API_KEY': KEY}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.connect = MagicMock()
        self.cursor = self.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        self.complete = {
            '1. open': '10.00', '2. high': '11.00', '3. low': '9.00',
            '4. close': '10.50', '5. volume': '1000',
        }
        self.prices = {'2026-09-30': self.complete}
        self.body = json.dumps({'Time Series (Daily)': self.prices}).encode('utf-8')

    def network(self, phase=None, error=None):
        response = MagicMock()
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        response.read.return_value = self.body
        opener = Mock(return_value=response)
        if phase == 'connection':
            opener.side_effect = error
        elif phase == 'read':
            response.read.side_effect = error
        return opener, response

    def assert_request(self, opener, response, phase=None):
        opener.assert_called_once()
        self.assertEqual(opener.call_args.kwargs, {'timeout': 30})
        self.assertEqual(len(opener.call_args.args), 1)
        url = urllib.parse.urlsplit(opener.call_args.args[0])
        self.assertEqual((url.scheme, url.netloc, url.path),
                         ('https', 'www.alphavantage.co', '/query'))
        self.assertEqual(urllib.parse.parse_qs(url.query), {
            'function': ['TIME_SERIES_DAILY'], 'symbol': ['TEST'],
            'outputsize': ['compact'], 'apikey': [KEY],
        })
        if phase == 'connection':
            response.__enter__.assert_not_called()
            response.read.assert_not_called()
            response.__exit__.assert_not_called()
        else:
            response.__enter__.assert_called_once_with()
            response.read.assert_called_once_with()
            response.__exit__.assert_called_once()

    def assert_timeout(self, error):
        self.assertIs(type(error), RuntimeError)
        self.assertEqual(str(error), EXPECTED_MESSAGE)

    def run_main(self):
        tree = ast.parse(MAIN.read_text(encoding='utf-8'), filename=str(MAIN))
        symbols = [node for node in tree.body if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == 'symbol'
                           for target in node.targets)]
        self.assertEqual(len(symbols), 1)
        symbols[0].value = ast.Constant(value='TEST')
        ast.fix_missing_locations(tree)
        psycopg = ModuleType('psycopg')
        psycopg.connect = self.connect
        # Faktisk hämtfunktion, faktisk JSON-tolkning; endast nätverk/DB simuleras.
        with patch.dict(sys.modules, {'market_data': market_data, 'psycopg': psycopg}), \
                redirect_stdout(io.StringIO()):
            exec(compile(tree, str(MAIN), 'exec'),
                 {'__name__': '__main__', '__file__': str(MAIN)})

    def timeout_errors(self):
        direct = TimeoutError('simulerad timeout')
        wrapped = urllib.error.URLError(TimeoutError('simulerad inpackad timeout'))
        return direct, wrapped

    def other_errors(self):
        return (
            urllib.error.URLError('simulerat transportfel'),
            urllib.error.URLError(OSError('simulerat anslutningsfel')),
        )

    def http_errors(self):
        return tuple(
            urllib.error.HTTPError('https://example.invalid/test', code,
                                   'simulerat HTTP-fel', None, None)
            for code in (400, 401, 403, 404, 429, 500, 503, 504)
        )

    def assert_http_error(self, error, original):
        self.assertIs(type(error), RuntimeError)
        self.assertEqual(
            str(error), f'TEST: datakällan svarade med HTTP-fel {original.code}',
        )
        self.assertIs(error.__cause__, original)

    def test_direct_timeout_at_connection_and_read(self):
        for phase in ('connection', 'read'):
            with self.subTest(phase=phase):
                original = TimeoutError('simulerad timeout')
                opener, response = self.network(phase, original)
                with patch.object(market_data.urllib.request, 'urlopen', opener):
                    with self.assertRaises(RuntimeError) as caught:
                        market_data.fetch_daily_prices('TEST')
                self.assert_timeout(caught.exception)
                self.assertIs(caught.exception.__cause__, original)
                self.assert_request(opener, response, phase)

    def test_wrapped_timeout_at_connection_and_read(self):
        for phase in ('connection', 'read'):
            with self.subTest(phase=phase):
                reason = TimeoutError('simulerad inpackad timeout')
                original = urllib.error.URLError(reason)
                opener, response = self.network(phase, original)
                with patch.object(market_data.urllib.request, 'urlopen', opener):
                    with self.assertRaises(RuntimeError) as caught:
                        market_data.fetch_daily_prices('TEST')
                self.assert_timeout(caught.exception)
                self.assertIs(caught.exception.__cause__, original)
                self.assertIs(caught.exception.__cause__.reason, reason)
                self.assert_request(opener, response, phase)

    def test_other_url_errors_are_preserved(self):
        for phase in ('connection', 'read'):
            for original in self.other_errors():
                with self.subTest(phase=phase, kind=type(original).__name__,
                                  reason=repr(original.reason)):
                    opener, response = self.network(phase, original)
                    with patch.object(market_data.urllib.request, 'urlopen', opener):
                        with self.assertRaises(RuntimeError) as caught:
                            market_data.fetch_daily_prices('TEST')
                    self.assertIs(type(caught.exception), RuntimeError)
                    self.assertEqual(
                        str(caught.exception),
                        'TEST: nätverksfel vid hämtning av kursdata',
                    )
                    self.assertIs(caught.exception.__cause__, original)
                    self.assert_request(opener, response, phase)

    def test_http_errors_have_status_and_cause(self):
        for phase in ('connection', 'read'):
            for original in self.http_errors():
                with self.subTest(phase=phase, status=original.code):
                    opener, response = self.network(phase, original)
                    with patch.object(market_data.urllib.request, 'urlopen', opener):
                        with self.assertRaises(RuntimeError) as caught:
                            market_data.fetch_daily_prices('TEST')
                    self.assert_http_error(caught.exception, original)
                    self.assert_request(opener, response, phase)

    def test_successful_fetch_uses_actual_json_and_expected_request(self):
        opener, response = self.network()
        with patch.object(market_data.urllib.request, 'urlopen', opener):
            actual = market_data.fetch_daily_prices('TEST')
        self.assertEqual(actual, self.prices)
        self.assert_request(opener, response)

    def test_main_timeouts_stop_before_database(self):
        for phase in ('connection', 'read'):
            for original in self.timeout_errors():
                with self.subTest(phase=phase, kind=type(original).__name__):
                    self.connect.reset_mock()
                    opener, response = self.network(phase, original)
                    try:
                        with patch.object(market_data.urllib.request, 'urlopen', opener):
                            with self.assertRaises(RuntimeError) as caught:
                                self.run_main()
                        self.assert_timeout(caught.exception)
                        self.assertIs(caught.exception.__cause__, original)
                        self.assert_request(opener, response, phase)
                    finally:
                        self.connect.assert_not_called()
                        self.cursor.executemany.assert_not_called()

    def test_main_other_url_errors_stop_before_database(self):
        for phase in ('connection', 'read'):
            for original in self.other_errors():
                with self.subTest(phase=phase, kind=type(original).__name__,
                                  reason=repr(original.reason)):
                    self.connect.reset_mock()
                    opener, response = self.network(phase, original)
                    try:
                        with patch.object(market_data.urllib.request, 'urlopen', opener):
                            with self.assertRaises(RuntimeError) as caught:
                                self.run_main()
                        self.assertIs(type(caught.exception), RuntimeError)
                        self.assertEqual(
                            str(caught.exception),
                            'TEST: nätverksfel vid hämtning av kursdata',
                        )
                        self.assertIs(caught.exception.__cause__, original)
                        self.assert_request(opener, response, phase)
                    finally:
                        self.connect.assert_not_called()
                        self.cursor.executemany.assert_not_called()

    def test_main_http_errors_stop_before_database(self):
        for phase in ('connection', 'read'):
            for original in self.http_errors():
                with self.subTest(phase=phase, status=original.code):
                    self.connect.reset_mock()
                    opener, response = self.network(phase, original)
                    try:
                        with patch.object(market_data.urllib.request, 'urlopen', opener):
                            with self.assertRaises(RuntimeError) as caught:
                                self.run_main()
                        self.assert_http_error(caught.exception, original)
                        self.assert_request(opener, response, phase)
                    finally:
                        self.connect.assert_not_called()
                        self.cursor.executemany.assert_not_called()

    def test_main_success_reaches_database_with_expected_row(self):
        opener, response = self.network()
        with patch.object(market_data.urllib.request, 'urlopen', opener):
            self.run_main()
        self.assert_request(opener, response)
        self.connect.assert_called_once_with(connect_timeout=10)
        self.cursor.executemany.assert_called_once()
        rows = self.cursor.executemany.call_args.args[1]
        self.assertEqual(rows, [(
            'TEST', date(2026, 9, 30), Decimal('10.00'), Decimal('11.00'),
            Decimal('9.00'), Decimal('10.50'), 1000,
        )])
        self.assertEqual([type(value) for value in rows[0]],
                         [str, date, Decimal, Decimal, Decimal, Decimal, int])


if __name__ == "__main__":
    unittest.main()
