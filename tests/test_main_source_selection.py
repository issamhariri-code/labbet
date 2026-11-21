"""Main source selection through real fetch/loader; SQL destination is simulated."""
from contextlib import contextmanager, redirect_stdout
from datetime import date
from decimal import Decimal
import importlib.util
import io
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch

from api_contracts import ApiFailure
from api_transport import TransportError, TransportResponse
import market_data
import test_market_source_selection as market_fixture


class MainSourceSelectionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = market_fixture.MarketSourceSelectionTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.sequence = 0
        path = Path(__file__).resolve().parents[1] / 'ingestion/main.py'
        spec = importlib.util.spec_from_file_location('labbet_main_source_test', path)
        self.main = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.main)

    @contextmanager
    def execution(self, **runtime_options):
        self.sequence += 1
        self.fixture.registry['state_path'] = str(
            self.fixture.folder / f'main-case-{self.sequence}.sqlite')
        self.events = []
        self.batches = []
        self.output = io.StringIO()
        connect = MagicMock()
        self.connect = connect
        self.connection = connect.return_value.__enter__.return_value
        connect.return_value.__exit__.side_effect = lambda *args: self.events.append('exit')
        self.persist = Mock(side_effect=lambda *args, **kwargs:
                            (self.events.append('persist'), SimpleNamespace(replayed=False))[1])
        self.journal = Mock(side_effect=lambda *args, **kwargs: self.events.append('journal'))

        def fetch(symbol, **selection):
            batch = market_data.fetch_daily_prices(symbol, **selection)
            self.batches.append(batch)
            return batch

        with self.fixture.runtime(**runtime_options) as loader, \
                patch.object(self.main, 'fetch_daily_prices', side_effect=fetch) as fetched, \
                patch.object(self.main.psycopg, 'connect', connect), \
                patch.object(self.main, 'persist_batch', self.persist), \
                patch.object(market_data, 'record_storage_outcome', self.journal), \
                redirect_stdout(self.output):
            self.fetched = fetched
            yield loader

    def assert_no_database(self):
        self.connect.assert_not_called()
        self.persist.assert_not_called()
        self.journal.assert_not_called()
        self.assertEqual(self.output.getvalue(), '')

    def assert_stored(self, source):
        self.assertEqual(len(self.batches), 1)
        batch = self.batches[0]
        self.connect.assert_called_once_with(connect_timeout=10, options=self.main.DB_OPTIONS)
        self.persist.assert_called_once_with(self.connection, rows=[(
            'TEST', date(2026, 9, 30), Decimal('10.00'), Decimal('11.00'),
            Decimal('9.00'), Decimal('10.50'), 1000)], **batch.storage_context)
        context = self.persist.call_args.kwargs
        self.assertEqual(context['source_id'], source)
        self.assertEqual(context['instrument_id'], 'TESTX:TEST')
        self.assertEqual(context['metadata']['currency'], 'USD')
        self.assertEqual(context['metadata']['market'], 'TESTX')
        self.assertEqual(context['metadata']['price_basis'], 'raw')
        self.assertEqual(context['metadata']['market_timezone'], 'UTC')
        self.assertEqual(context['received_at'], market_fixture.NOW)
        self.assertEqual(context['observed_at'].isoformat(), '2026-09-30T20:00:00+00:00')
        self.journal.assert_called_once_with(batch, succeeded=True)
        self.assertEqual(self.events, ['persist', 'exit', 'journal'])
        self.assertEqual(json.loads(self.output.getvalue()), {
            'source': source, 'request_id': batch.request_id, 'rows': 1,
            'status': 'stored', 'replayed': False})
        self.fixture.assert_closed()

    def test_selected_source_batch_and_all_storage_metadata_reach_persistence(self):
        for source, key in [('source-a', 'KEY_A'), ('source-b', 'KEY_B')]:
            with self.subTest(source=source), self.execution(key=key) as loader:
                self.main.run('TEST', source_id=source)
                self.fetched.assert_called_once_with('TEST', source_id=source)
                loader.assert_called_once_with('TEST', source_id=source)
                self.assert_stored(source)

    def test_omitted_and_none_source_keep_existing_single_source_behavior(self):
        self.fixture.registry['sources'] = self.fixture.registry['sources'][:1]
        for selection in ({}, {'source_id': None}):
            with self.subTest(selection=selection), self.execution() as loader:
                self.main.run('TEST', **selection)
                self.fetched.assert_called_once_with('TEST')
                loader.assert_called_once_with('TEST')
                self.assert_stored('source-a')

    def test_real_source_selection_errors_stop_before_database_without_fallback(self):
        for selection, expected in [({}, 'unknown_or_ambiguous_instrument'),
                ({'source_id': None}, 'unknown_or_ambiguous_instrument'),
                ({'source_id': 'unknown'}, 'unknown_or_ambiguous_instrument'),
                ({'source_id': ''}, 'invalid_source_id'),
                ({'source_id': 'source-b'}, 'missing_credential')]:
            with self.subTest(selection=selection), self.execution() as loader:
                with self.assertRaises(ApiFailure) as caught:
                    self.main.run('TEST', **selection)
                self.assertEqual(caught.exception.code, expected)
                forwarded = selection if selection.get('source_id') is not None else {}
                loader.assert_called_once_with('TEST', **forwarded)
                self.fetched.assert_called_once_with('TEST', **forwarded)
                self.fixture.transport.assert_not_called()
                self.assert_no_database()

    def test_transport_and_invalid_payload_stop_before_database(self):
        for transport, expected in [
                (Mock(side_effect=TransportError('network')), 'network'),
                (Mock(return_value=TransportResponse(200, {}, b'{')), 'invalid_json')]:
            with self.subTest(expected=expected), self.execution(key='KEY_B',
                    transport=transport) as loader:
                with self.assertRaises(ApiFailure) as caught:
                    self.main.run('TEST', source_id='source-b')
                self.assertEqual(caught.exception.code, expected)
                loader.assert_called_once_with('TEST', source_id='source-b')
                transport.assert_called_once()
                self.assert_no_database()
                self.fixture.assert_closed()

    def test_fetch_error_identity_preserved_without_database_or_fallback(self):
        error = ApiFailure('authentication', status=401)
        with self.execution(), patch.object(self.main, 'fetch_daily_prices',
                side_effect=error) as fetch:
            with self.assertRaises(ApiFailure) as caught:
                self.main.run('TEST', source_id='source-b')
            self.assertIs(caught.exception, error)
            fetch.assert_called_once_with('TEST', source_id='source-b')
            self.assert_no_database()

    def test_empty_symbol_stops_before_fetch_and_database(self):
        for symbol in ('', '   '):
            for selection in ({}, {'source_id': 'source-b'}):
                with self.subTest(symbol=symbol, selection=selection), self.execution() as loader:
                    with self.assertRaisesRegex(ValueError, 'Fyll i symbol'):
                        self.main.run(symbol, **selection)
                    self.fetched.assert_not_called()
                    loader.assert_not_called()
                    self.assert_no_database()

    def test_storage_failure_preserves_error_and_reports_selected_batch(self):
        error = RuntimeError('synthetic database failure')
        with self.execution(key='KEY_B'):
            self.persist.side_effect = error
            with self.assertRaises(RuntimeError) as caught:
                self.main.run('TEST', source_id='source-b')
            self.assertIs(caught.exception, error)
            self.assertEqual(self.batches[0].source_id, 'source-b')
            self.journal.assert_called_once_with(self.batches[0], succeeded=False)
            self.assertEqual(self.persist.call_args.kwargs['source_id'], 'source-b')
            self.assertEqual(self.output.getvalue(), '')
            self.fixture.assert_closed()
