"""Registry boundaries; no network or provider credentials are used."""
from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from api_contracts import ApiFailure
from api_runtime import load_runtime
from test_api_client import profile, contract


class RuntimeRegistryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.folder = Path(self.tmp.name)
        self.path = self.folder / 'registry.json'
        p = profile(environment='production', endpoint='https://www.alphavantage.co/query',
                    rights_review_date='2000-01-01', review_due_date='2099-12-31')
        self.registry = {'version': 1, 'environment': 'production',
                         'state_path': str(self.folder / 'state.sqlite'), 'sources': [
            {'adapter': 'alphavantage-daily-v1', 'profile': asdict(p),
             'fetch_policy': {}, 'instruments': [asdict(contract())]}]}
        self.environ = {'LABBET_API_CONFIG': str(self.path), 'KEY_A': 'SYNTHETIC-KEY-A'}

    def load(self, registry=None, environ=None, **selection):
        self.path.write_text(json.dumps(self.registry if registry is None else registry,
                                       default=str), encoding='utf-8')
        return load_runtime('TEST', self.environ if environ is None else environ, **selection)

    def two_sources(self):
        other = deepcopy(self.registry['sources'][0])
        other['profile'].update(source_id='source-b', account_ref='account-b',
                                quota_scope='account-b', credential_env='KEY_B')
        other['instruments'][0]['instrument_id'] = 'TESTX:SECOND'
        self.registry['sources'].append(other)

    def test_explicit_source_selects_matching_profile_contract_and_key(self):
        self.two_sources()
        for source, key, instrument in [('source-a', 'KEY_A', 'TESTX:TEST'),
                                         ('source-b', 'KEY_B', 'TESTX:SECOND')]:
            with self.subTest(source=source), patch('socket.create_connection',
                    side_effect=AssertionError('network forbidden')):
                # Only the selected key exists; no fallback to the other key.
                client, p, c = self.load(source_id=source, environ={
                    'LABBET_API_CONFIG': str(self.path), key: 'SYNTHETIC-' + key})
                try:
                    self.assertEqual(p.source_id, source)
                    self.assertEqual(p.credential_env, key)
                    self.assertEqual((c.symbol, c.instrument_id), ('TEST', instrument))
                    self.assertEqual(client.credentials.snapshot(p, 1).secret,
                                     'SYNTHETIC-' + key)
                finally:
                    client.credentials.close()
                    client.state.close()

    def test_omitted_or_none_source_rejects_ambiguous_symbol_before_state(self):
        self.two_sources()
        for selection in ({}, {'source_id': None}):
            with self.subTest(selection=selection), self.assertRaises(ApiFailure) as caught:
                self.load(**selection)
            self.assertEqual(caught.exception.code, 'unknown_or_ambiguous_instrument')
            self.assertFalse((self.folder / 'state.sqlite').exists())

    def test_unknown_source_has_no_fallback(self):
        for source in ('unknown-source', 'SOURCE-A'):
            with self.subTest(source=source), self.assertRaises(ApiFailure) as caught:
                self.load(source_id=source)
            self.assertEqual(caught.exception.code, 'unknown_or_ambiguous_instrument')
            self.assertFalse((self.folder / 'state.sqlite').exists())

    def test_invalid_source_rejected_before_configuration_read(self):
        for source in ('', ' source-a', 'source-a ', 'source/a', 'a' * 129,
                       'source\n', 0, True, [], {}):
            with self.subTest(source=source), patch('builtins.open') as opened:
                with self.assertRaises(ApiFailure) as caught:
                    load_runtime('TEST', {}, source_id=source)
                self.assertEqual(caught.exception.code, 'invalid_source_id')
                opened.assert_not_called()
                self.assertFalse((self.folder / 'state.sqlite').exists())

    def test_single_source_remains_compatible_with_optional_selection(self):
        for selection in ({}, {'source_id': None}, {'source_id': 'source-a'}):
            with self.subTest(selection=selection):
                client, p, c = self.load(**selection)
                try:
                    self.assertEqual((p.source_id, c.symbol), ('source-a', 'TEST'))
                    self.assertEqual(client.credentials.snapshot(p, 1).secret,
                                     'SYNTHETIC-KEY-A')
                finally:
                    client.credentials.close()
                    client.state.close()

    def test_selected_source_without_symbol_does_not_use_another_source(self):
        self.two_sources()
        self.registry['sources'][1]['instruments'][0]['symbol'] = 'OTHER'
        with self.assertRaises(ApiFailure) as caught:
            self.load(source_id='source-b')
        self.assertEqual(caught.exception.code, 'unknown_or_ambiguous_instrument')
        self.assertFalse((self.folder / 'state.sqlite').exists())

    def test_explicit_selection_still_rejects_duplicate_definitions(self):
        for duplicate_source in (True, False):
            registry = deepcopy(self.registry)
            entry = registry['sources'][0]
            if duplicate_source:
                registry['sources'].append(deepcopy(entry))
            else:
                entry['instruments'].append(deepcopy(entry['instruments'][0]))
            with self.subTest(duplicate_source=duplicate_source):
                with self.assertRaises(ApiFailure) as caught:
                    self.load(registry, source_id='source-a')
                self.assertEqual(caught.exception.code, 'duplicate_source' if duplicate_source
                                 else 'unknown_or_ambiguous_instrument')
                self.assertFalse((self.folder / 'state.sqlite').exists())

    def test_explicit_selection_preserves_registry_adapter_and_endpoint_guards(self):
        self.two_sources()
        for field, value, expected in (
                ('adapter', 'unsupported-v1', 'unsupported_adapter'),
                ('endpoint', 'https://attacker.invalid/query', 'adapter_endpoint_mismatch'),
                ('environment', 'test', 'environment_mismatch')):
            registry = deepcopy(self.registry)
            other = registry['sources'][1]
            (other if field == 'adapter' else other['profile'])[field] = value
            with self.subTest(field=field), self.assertRaises(ApiFailure) as caught:
                self.load(registry, source_id='source-a')
            self.assertEqual(caught.exception.code, expected)
            self.assertFalse((self.folder / 'state.sqlite').exists())

    def test_selected_missing_key_does_not_use_available_other_key(self):
        self.two_sources()
        with self.assertRaises(ApiFailure) as caught:
            self.load(source_id='source-b')
        self.assertEqual(caught.exception.code, 'missing_credential')

    def test_reviewed_registry_loads_without_any_network(self):
        with patch('socket.create_connection', side_effect=AssertionError('network forbidden')):
            client, p, c = self.load()
        self.addCleanup(client.state.close)
        self.addCleanup(client.credentials.close)
        self.assertEqual((p.source_id, c.instrument_id), ('source-a', 'TESTX:TEST'))
        self.assertEqual(client.credentials.snapshot(p, 1).secret, 'SYNTHETIC-KEY-A')

    def test_unrelated_expired_source_does_not_disable_selected_source(self):
        other = deepcopy(self.registry['sources'][0])
        other['profile'].update(source_id='source-b', credential_env='KEY_B',
                                rights_review_date='2000-01-01', review_due_date='2001-01-01')
        other['instruments'][0]['symbol'] = 'OTHER'
        self.registry['sources'].append(other)
        client, p, _ = self.load()
        self.addCleanup(client.state.close)
        self.addCleanup(client.credentials.close)
        self.assertEqual(p.source_id, 'source-a')

    def test_ambiguous_symbol_and_duplicate_source_are_rejected(self):
        for source_id, expected in [('source-a', 'duplicate_source'),
                                    ('source-b', 'unknown_or_ambiguous_instrument')]:
            with self.subTest(source_id=source_id):
                registry = deepcopy(self.registry)
                other = deepcopy(registry['sources'][0])
                other['profile']['source_id'] = source_id
                registry['sources'].append(other)
                with self.assertRaises(ApiFailure) as caught:
                    self.load(registry)
                self.assertEqual(caught.exception.code, expected)
                self.assertFalse((self.folder / 'state.sqlite').exists())

    def test_malformed_registry_types_fail_before_state_or_credentials(self):
        variants = []
        for field, value in [('version', True), ('sources', {}), ('environment', 'test')]:
            registry = deepcopy(self.registry); registry[field] = value; variants.append(registry)
        for field, value in [('profile', []), ('fetch_policy', []), ('instruments', {}),
                             ('instruments', ['SYNTHETIC-KEY-IN-MALFORMED-DATA'])]:
            registry = deepcopy(self.registry); registry['sources'][0][field] = value; variants.append(registry)
        registry = deepcopy(self.registry)
        registry['sources'][0]['profile']['allowed_runtime_ips'] = '127.0.0.1'
        variants.append(registry)
        for registry in variants:
            with self.subTest(index=variants.index(registry)):
                with self.assertRaises(ApiFailure) as caught:
                    self.load(registry)
                self.assertNotIn('SYNTHETIC-KEY', str(caught.exception))
                self.assertFalse((self.folder / 'state.sqlite').exists())

    def test_unreviewed_source_fails_before_state_creation(self):
        self.registry['sources'][0]['profile']['rights_verified'] = False
        with self.assertRaises(ApiFailure) as caught:
            self.load()
        self.assertEqual(caught.exception.code, 'rights_not_verified')
        self.assertFalse((self.folder / 'state.sqlite').exists())

    def test_endpoint_substitution_and_missing_key_fail_closed(self):
        registry = deepcopy(self.registry)
        registry['sources'][0]['profile']['endpoint'] = 'https://attacker.invalid/query'
        with self.assertRaises(ApiFailure) as caught:
            self.load(registry)
        self.assertEqual(caught.exception.code, 'adapter_endpoint_mismatch')
        with self.assertRaises(ApiFailure) as caught:
            self.load(environ={'LABBET_API_CONFIG': str(self.path)})
        self.assertEqual(caught.exception.code, 'missing_credential')
