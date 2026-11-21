"""Public fetch source selection with synthetic registry, keys and local state."""
from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from api_contracts import ApiFailure
from api_runtime import load_runtime
from api_transport import TransportError
import market_data
from test_api_client import NOW, contract, profile, response


class MarketSourceSelectionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.folder = Path(self.tmp.name)
        self.path = self.folder / 'registry.json'
        self.registry = {'version': 1, 'environment': 'production',
                         'state_path': str(self.folder / 'state.sqlite'), 'sources': []}
        for source, key in [('source-a', 'KEY_A'), ('source-b', 'KEY_B')]:
            p = profile(source_id=source, account_ref=source, quota_scope=source,
                        credential_env=key, environment='production',
                        endpoint='https://www.alphavantage.co/query',
                        rights_review_date='2000-01-01', review_due_date='2099-12-31')
            self.registry['sources'].append({'adapter': 'alphavantage-daily-v1',
                'profile': asdict(p), 'fetch_policy': {'max_attempts': 1},
                'instruments': [asdict(contract())]})

    def runtime(self, *, key='KEY_A', transport=None):
        self.path.write_text(json.dumps(self.registry, default=str))
        self.clients = []
        self.transport = transport if transport is not None else Mock(return_value=response())

        def loader(symbol, **selection):
            client, p, c = load_runtime(symbol, environ={
                'LABBET_API_CONFIG': str(self.path), key: 'SYNTHETIC-' + key}, **selection)
            self.clients.append(client)
            client.wall = lambda: NOW.timestamp()
            client.transport = self.transport
            client.credentials.close = Mock(wraps=client.credentials.close)
            client.state.close = Mock(wraps=client.state.close)
            # Ensure test failure cannot leave resources open.
            self.addCleanup(client.state.close)
            self.addCleanup(client.credentials.close)
            return client, p, c

        return patch.object(market_data, 'load_runtime', side_effect=loader)

    def assert_closed(self):
        self.assertEqual(len(self.clients), 1)
        client = self.clients[0]
        client.credentials.close.assert_called_once_with()
        client.state.close.assert_called_once_with()
        self.assertTrue(client.credentials._closed)
        self.assertTrue(client.state._closed)
        self.assertEqual(client.credentials._values, {})

    def test_forwarding_and_return_identity_for_omitted_none_and_explicit_source(self):
        for selection, forwarded in [({}, {}), ({'source_id': None}, {}),
                                     ({'source_id': 'source-b'}, {'source_id': 'source-b'})]:
            client, p, c = Mock(), object(), object()
            with self.subTest(selection=selection), patch.object(market_data, 'load_runtime',
                    return_value=(client, p, c)) as loader:
                result = market_data.fetch_daily_prices('TEST', **selection)
                loader.assert_called_once_with('TEST', **forwarded)
                client.fetch.assert_called_once_with(p, c)
                self.assertIs(result, client.fetch.return_value)
                client.credentials.close.assert_called_once_with()
                client.state.close.assert_called_once_with()

    def test_loader_errors_propagate_unchanged_without_retry_or_fallback(self):
        for code in ('invalid_source_id', 'unknown_or_ambiguous_instrument',
                     'missing_credential', 'unsupported_adapter'):
            error = ApiFailure(code)
            with self.subTest(code=code), patch.object(market_data, 'load_runtime',
                    side_effect=error) as loader:
                with self.assertRaises(ApiFailure) as caught:
                    market_data.fetch_daily_prices('TEST', source_id='source-b')
                self.assertIs(caught.exception, error)
                loader.assert_called_once_with('TEST', source_id='source-b')

    def test_fetch_errors_propagate_unchanged_and_close_both_resources(self):
        for error in (ApiFailure('network'), RuntimeError('synthetic failure')):
            client, p, c = Mock(), object(), object()
            client.fetch.side_effect = error
            with self.subTest(error=type(error).__name__), patch.object(market_data,
                    'load_runtime', return_value=(client, p, c)) as loader:
                with self.assertRaises(type(error)) as caught:
                    market_data.fetch_daily_prices('TEST', source_id='source-b')
                self.assertIs(caught.exception, error)
                loader.assert_called_once_with('TEST', source_id='source-b')
                client.fetch.assert_called_once_with(p, c)
                client.credentials.close.assert_called_once_with()
                client.state.close.assert_called_once_with()

    def test_real_loader_client_select_each_source_and_close_after_success(self):
        for source, key in [('source-a', 'KEY_A'), ('source-b', 'KEY_B')]:
            with self.subTest(source=source), self.runtime(key=key) as loader:
                batch = market_data.fetch_daily_prices('TEST', source_id=source)
                self.assertEqual((batch.source_id, batch.symbol), (source, 'TEST'))
                self.assertEqual(batch.prices['2026-09-30']['4. close'], '10.50')
                loader.assert_called_once_with('TEST', source_id=source)
                self.transport.assert_called_once()
                query = parse_qs(urlsplit(self.transport.call_args.args[0]).query)
                self.assertEqual(query['apikey'], ['SYNTHETIC-' + key])
                self.assert_closed()

    def test_real_client_transport_failure_closes_resources_without_fallback(self):
        transport = Mock(side_effect=TransportError('network'))
        with self.runtime(key='KEY_B', transport=transport) as loader:
            with self.assertRaises(ApiFailure) as caught:
                market_data.fetch_daily_prices('TEST', source_id='source-b')
            self.assertEqual(caught.exception.code, 'network')
            loader.assert_called_once_with('TEST', source_id='source-b')
            transport.assert_called_once()
            self.assert_closed()

    def test_real_loader_rejects_invalid_unknown_ambiguous_and_missing_key(self):
        for selection, expected in [({}, 'unknown_or_ambiguous_instrument'),
                ({'source_id': None}, 'unknown_or_ambiguous_instrument'),
                ({'source_id': 'unknown'}, 'unknown_or_ambiguous_instrument'),
                ({'source_id': ''}, 'invalid_source_id'),
                ({'source_id': 'source-b'}, 'missing_credential')]:
            with self.subTest(selection=selection), self.runtime() as loader:
                with self.assertRaises(ApiFailure) as caught:
                    market_data.fetch_daily_prices('TEST', **selection)
                self.assertEqual(caught.exception.code, expected)
                forwarded = selection if selection.get('source_id') is not None else {}
                loader.assert_called_once_with('TEST', **forwarded)
                self.transport.assert_not_called()
                self.assertEqual(self.clients, [])

    def test_real_single_source_omitted_and_none_remain_compatible(self):
        self.registry['sources'] = [deepcopy(self.registry['sources'][0])]
        for selection in ({}, {'source_id': None}):
            with self.subTest(selection=selection), self.runtime() as loader:
                batch = market_data.fetch_daily_prices('TEST', **selection)
                self.assertEqual(batch.source_id, 'source-a')
                loader.assert_called_once_with('TEST')
                self.assert_closed()
