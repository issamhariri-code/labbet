"""CLI and __main__ source selection with real fetch, synthetic IO and state."""
from contextlib import redirect_stderr
import io
import json
import os
from pathlib import Path
import runpy
import unittest
from unittest.mock import Mock, patch

from api_transport import TransportError, TransportResponse
import api_storage
import test_main_source_selection as main_fixture
from test_api_client import payload


class StartupSourceSelectionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = main_fixture.MainSourceSelectionTests()
        self.fixture.setUp()
        self.fixture.main.symbol = 'TEST'
        self.addCleanup(self.fixture.doCleanups)

    def invoke(self, environment):
        stderr = io.StringIO()
        with patch.dict(os.environ, environment, clear=True), redirect_stderr(stderr):
            code = self.fixture.main.cli()
        return code, stderr.getvalue()

    def assert_safe_error(self, code, stderr, expected):
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(stderr), {'status': 'error', 'code': expected})
        for private in ('SYNTHETIC-', 'apikey=', 'postgresql://', 'Traceback', 'private-source'):
            self.assertNotIn(private, stderr + self.fixture.output.getvalue())
        self.fixture.assert_no_database()

    def test_explicit_source_reaches_actual_fetch_and_storage(self):
        for source, key in [('source-a', 'KEY_A'), ('source-b', 'KEY_B')]:
            with self.subTest(source=source), self.fixture.execution(key=key) as loader:
                code, stderr = self.invoke({'LABBET_API_SOURCE': source})
                self.assertEqual(code, 0)
                self.assertEqual(stderr, '')
                loader.assert_called_once_with('TEST', source_id=source)
                self.fixture.assert_stored(source)

    def test_absent_source_preserves_single_source_startup(self):
        self.fixture.fixture.registry['sources'] = self.fixture.fixture.registry['sources'][:1]
        with self.fixture.execution() as loader:
            code, stderr = self.invoke({})
            self.assertEqual(code, 0)
            self.assertEqual(stderr, '')
            loader.assert_called_once_with('TEST')
            self.fixture.assert_stored('source-a')

    def test_bad_unknown_ambiguous_and_missing_key_fail_without_fallback(self):
        for environment, expected in [
                ({}, 'unknown_or_ambiguous_instrument'),
                ({'LABBET_API_SOURCE': ''}, 'invalid_source_id'),
                ({'LABBET_API_SOURCE': 'private-source/invalid?apikey=SYNTHETIC'}, 'invalid_source_id'),
                ({'LABBET_API_SOURCE': 'private-source-unknown'}, 'unknown_or_ambiguous_instrument'),
                ({'LABBET_API_SOURCE': 'source-b'}, 'missing_credential')]:
            with self.subTest(environment=environment), self.fixture.execution() as loader:
                code, stderr = self.invoke(environment)
                self.assert_safe_error(code, stderr, expected)
                selection = {} if not environment else {'source_id': environment['LABBET_API_SOURCE']}
                loader.assert_called_once_with('TEST', **selection)
                self.fixture.fixture.transport.assert_not_called()

    def test_transport_authentication_and_network_errors_keep_safe_codes(self):
        for error, expected, http_status in [
                (TransportError('network'), 'network', None),
                (TransportError('http', status=401), 'authentication', 401)]:
            transport = Mock(side_effect=error)
            with self.subTest(expected=expected), self.fixture.execution(key='KEY_B', transport=transport):
                code, stderr = self.invoke({'LABBET_API_SOURCE': 'source-b'})
                self.assertEqual(code, 1)
                message = {'status': 'error', 'code': expected}
                if http_status is not None:
                    message['http_status'] = http_status
                self.assertEqual(json.loads(stderr), message)
                transport.assert_called_once()
                self.fixture.assert_no_database()
                self.fixture.fixture.assert_closed()

    def test_cancellation_keeps_exit_130_and_safe_output(self):
        with self.fixture.execution(), patch.object(self.fixture.main, 'fetch_daily_prices',
                side_effect=KeyboardInterrupt('SYNTHETIC-PRIVATE')):
            code, stderr = self.invoke({'LABBET_API_SOURCE': 'source-b'})
            self.assertEqual(code, 130)
            self.assertEqual(json.loads(stderr), {'status': 'cancelled', 'code': 'cancelled'})
            self.fixture.assert_no_database()

    def test_unexpected_failure_keeps_safe_generic_error(self):
        with self.fixture.execution(), patch.object(self.fixture.main, 'fetch_daily_prices',
                side_effect=RuntimeError('postgresql://user:SYNTHETIC-PRIVATE@host/db')) as fetch:
            code, stderr = self.invoke({'LABBET_API_SOURCE': 'source-b'})
            self.assert_safe_error(code, stderr, 'ingestion_failed')
            fetch.assert_called_once_with('TEST', source_id='source-b')

    def test_actual_main_entrypoint_exits_zero_for_selected_source(self):
        for entry in self.fixture.fixture.registry['sources']:
            entry['instruments'][0]['symbol'] = 'ONDS'
        body = payload()
        body['Meta Data']['2. Symbol'] = 'ONDS'
        transport = Mock(return_value=TransportResponse(200, {}, json.dumps(body).encode()))
        stderr = io.StringIO()
        path = Path(__file__).resolve().parents[1] / 'ingestion/main.py'
        with self.fixture.execution(key='KEY_B', transport=transport) as loader, \
                patch.object(api_storage, 'persist_batch', self.fixture.persist), \
                patch.dict(os.environ, {'LABBET_API_SOURCE': 'source-b'}, clear=True), \
                redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as caught:
                runpy.run_path(str(path), run_name='__main__')
            self.assertEqual(caught.exception.code, 0)
            self.assertEqual(stderr.getvalue(), '')
            loader.assert_called_once_with('ONDS', source_id='source-b')
            self.assertEqual(self.fixture.persist.call_args.kwargs['source_id'], 'source-b')
            self.assertEqual(json.loads(self.fixture.output.getvalue())['source'], 'source-b')
