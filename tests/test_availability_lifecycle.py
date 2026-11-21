"""Enabled availability under TLS, concurrency, rotation and durable quotas."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import date, timedelta
import hashlib
import json
import multiprocessing
import sqlite3
import threading
from types import MappingProxyType
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlencode, urlsplit

import api_adapters
from api_adapters import DailyAdapterSpec
from api_client import ApiClient, FetchPolicy
from api_availability import plan_daily_request
from api_contracts import SourceAvailability
from api_profiles import CredentialStore
from api_state import StateStore
from api_storage import _metadata, _rows
from api_transport import TransportPolicy, TransportResponse
from api_validation import prepare_rows
import test_adapter_binding_integration as adapter_fixtures
import test_availability_integration as availability_fixtures
from test_api_client import NOW, ROW, response
from test_availability_integration import dated_payload, schedule


class AvailabilityLifecycleTests(unittest.TestCase):
    setUp = availability_fixtures.AvailabilityIntegrationTests.setUp
    client = availability_fixtures.AvailabilityIntegrationTests.client
    backup = availability_fixtures.AvailabilityIntegrationTests.backup
    reject = availability_fixtures.AvailabilityIntegrationTests.reject
    snapshot = availability_fixtures.AvailabilityIntegrationTests.snapshot
    server = adapter_fixtures.AdapterLoopbackTLSTests.server

    def test_enabled_request_through_real_loopback_dns_tls_json_and_state(self):
        children_before = {child.pid for child in multiprocessing.active_children()}
        with self.server() as (server, origin, certificate):
            p = replace(self.p, endpoint=origin + '/query')
            client = ApiClient(self.state, self.keys, availabilities=(self.a,),
                policy=FetchPolicy(total_seconds=10, socket_seconds=3, max_attempts=1),
                wall=lambda: NOW.timestamp(),
                transport_policy=TransportPolicy(origin, test_mode=True, ca_file=certificate))
            batch = client.fetch(p, self.c)
            self.assertEqual(batch.prices['2026-09-30'], ROW)
            self.assertEqual(batch.request_context, plan_daily_request(self.c, NOW))
            self.assertEqual(batch.payload_hash, hashlib.sha256(server.body).hexdigest())
            self.assertEqual(batch.observed_at, self.c.sessions[1][1])
            self.assertEqual(batch.received_at, NOW)
            self.assertEqual(server.requests, [{'function': ['TIME_SERIES_DAILY'],
                'symbol': ['TEST'], 'outputsize': ['compact'],
                'apikey': ['SYNTHETIC-AVAILABILITY-A']}])
            self.assertEqual(server.errors, [])
        self.assertEqual({child.pid for child in multiprocessing.active_children()}, children_before)
        self.assertEqual(self.state.health(self.p.source_id)['active_jobs'], 0)

    def test_future_gate_prevents_real_loopback_connection_and_quota(self):
        late = schedule(self.p, self.c, overrides={date(2026, 9, 30): NOW + timedelta(seconds=1)})
        with self.server() as (server, origin, certificate):
            p = replace(self.p, endpoint=origin + '/query')
            client = ApiClient(self.state, self.keys, availabilities=(late,),
                wall=lambda: NOW.timestamp(),
                transport_policy=TransportPolicy(origin, test_mode=True, ca_file=certificate))
            before = self.snapshot()
            self.reject('data_not_available', lambda: client.fetch(p, self.c))
            self.assertEqual(self.snapshot(), before)
            self.assertEqual(server.requests, [])
            self.assertEqual(server.errors, [])

    def test_concurrent_jobs_keep_independent_sources_keys_and_request_contexts(self):
        backup, other = self.backup()
        client = self.client(self.a, other)
        first_request = plan_daily_request(self.c, NOW)
        second_request = plan_daily_request(self.c, NOW - timedelta(days=1))
        barrier = threading.Barrier(2)
        calls = []
        def transport(url, **options):
            calls.append(url)
            barrier.wait(timeout=5)
            return response() if urlsplit(url).hostname == 'source-a.invalid' else response(dated_payload('2026-09-29'))
        client.transport = transport
        original = (self.a, other, first_request, second_request)
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(client.fetch, self.p, self.c, request_context=first_request)
            second = pool.submit(client.fetch, backup, self.c, request_context=second_request)
            batches = first.result(timeout=10), second.result(timeout=10)
        self.assertEqual([batch.request_context for batch in batches], [first_request, second_request])
        self.assertEqual([tuple(batch.prices) for batch in batches], [('2026-09-30',), ('2026-09-29',)])
        self.assertEqual([batch.source_id for batch in batches], ['source-a', 'source-b'])
        for url in calls:
            expected = 'SYNTHETIC-AVAILABILITY-A' if urlsplit(url).hostname == 'source-a.invalid' else 'SYNTHETIC-AVAILABILITY-B'
            self.assertEqual(parse_qs(urlsplit(url).query)['apikey'], [expected])
        self.assertEqual((self.a, other, first_request, second_request), original)
        self.assertEqual([self.state.health(p.source_id)['active_jobs'] for p in (self.p, backup)], [0, 0])

    def test_rotation_preserves_inflight_snapshot_and_next_job_uses_new_key(self):
        client = self.client(self.a)
        rotated = []
        calls = []
        def transport(url, **options):
            calls.append(url)
            if len(calls) == 1:
                rotated.append(self.keys.rotate(self.p, 'v2', {'KEY_A': 'SYNTHETIC-ROTATED'}, now=self.clock.wall()))
            return response()
        client.transport = transport
        first = client.fetch(self.p, self.c)
        second = client.fetch(rotated[0], self.c)
        self.assertEqual([parse_qs(urlsplit(url).query)['apikey'] for url in calls],
                         [['SYNTHETIC-AVAILABILITY-A'], ['SYNTHETIC-ROTATED']])
        self.assertEqual(first.request_context, second.request_context)
        self.assertNotEqual(first.config_version, second.config_version)
        with closing(sqlite3.connect(self.state.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0], 2)

    def test_revoked_inflight_response_is_rejected_without_returning_consumed_quota(self):
        p = replace(self.p, max_attempts_per_window=1)
        client = self.client(self.a)
        rotated = []
        def transport(*args, **kwargs):
            rotated.append(self.keys.rotate(p, 'v2', {'KEY_A': 'SYNTHETIC-ROTATED'}, now=self.clock.wall()))
            self.keys.revoke(p.source_id, p.credential_version, now=self.clock.wall())
            return response()
        self.transport.side_effect = transport
        self.reject('credential_revoked', lambda: client.fetch(p, self.c))
        self.assertIsNone(self.state.health(p.source_id)['last_success'])
        self.assertEqual(self.state.health(p.source_id)['active_jobs'], 0)
        self.reject('quota', lambda: client.fetch(rotated[0], self.c))
        self.assertEqual(self.transport.call_count, 1)
        with closing(sqlite3.connect(self.state.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0], 1)

    def test_disabling_rule_without_version_change_rejects_and_preserves_consumption(self):
        self.client(self.a).fetch(self.p, self.c)
        before = self.snapshot()
        legacy = replace(self.p, availability_rule_version=None)
        self.reject('config_version_reused', lambda: self.client().fetch(legacy, self.c))
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.transport.call_count, 1)

    def test_reopen_with_changed_schedule_does_not_create_fresh_quota(self):
        p = replace(self.p, max_attempts_per_window=2)
        first = self.client(self.a).fetch(p, self.c)
        self.keys.close()
        self.state.close()
        self.state = StateStore(self.folder / 'state.sqlite')
        self.addCleanup(self.state.close)
        self.keys = CredentialStore(self.state)
        self.addCleanup(self.keys.close)
        self.keys.install(p, {'KEY_A': 'SYNTHETIC-AVAILABILITY-A'})
        changed = schedule(p, self.c, overrides={date(2026, 9, 29): NOW})
        client = self.client(changed)
        second = client.fetch(p, self.c)
        self.assertNotEqual(first.config_version, second.config_version)
        self.reject('quota', lambda: client.fetch(p, self.c))
        self.assertEqual(self.transport.call_count, 2)
        self.assertEqual(self.state.inventory()[0]['config_hash'], p.config_hash)

    def test_real_fetched_rows_pass_existing_storage_numeric_contract(self):
        batch = self.client(self.a).fetch(self.p, self.c)
        rows = prepare_rows(batch.symbol, batch.prices)
        validated = _rows(rows, batch.symbol)
        self.assertEqual(len(validated), 1)
        self.assertEqual(validated[0][0][0:2], ('TEST', date(2026, 9, 30)))
        self.assertEqual(_metadata(batch.storage_context['metadata']), dict(batch.metadata))
        self.assertEqual(batch.storage_context['observed_at'], self.c.sessions[1][1])
        self.assertEqual(batch.storage_context['received_at'], NOW)

    def test_expired_single_fetch_preflight_budget_has_no_state_or_key_effect(self):
        client = self.client(self.a)
        original = client._availability_request
        def costly_validation(*args, **kwargs):
            result = original(*args, **kwargs)
            self.clock.elapsed += 31
            return result
        before = self.snapshot()
        with (patch.object(client, '_availability_request', side_effect=costly_validation),
              patch.object(self.state, 'register', side_effect=AssertionError('state forbidden')),
              patch.object(self.keys, 'snapshot', side_effect=AssertionError('keys forbidden'))):
            self.reject('deadline', lambda: client.fetch(self.p, self.c))
        self.assertEqual(self.snapshot(), before)
        self.transport.assert_not_called()

    def test_registered_other_adapter_schema_and_rule_use_shared_availability_flow(self):
        parsed = []
        adapter_id = 'synthetic-session-json-v1'
        schema = 'synthetic-ohlcv-v2'
        rule = 'synthetic-manual-sessions-v3'

        class SyntheticAdapter:
            __slots__ = ()

            @staticmethod
            def build_url(endpoint, symbol, secret):
                return endpoint + '?' + urlencode({'instrument': symbol, 'access_token': secret})

            @staticmethod
            def parse(data, selected_contract, evaluation_time):
                parsed.append((data, selected_contract, evaluation_time))
                item = data['daily_bar']
                row = MappingProxyType({
                    '1. open': item['open'], '2. high': item['high'],
                    '3. low': item['low'], '4. close': item['close'],
                    '5. volume': item['volume'],
                })
                return (MappingProxyType({item['session']: row}),
                        selected_contract.completed_sessions(evaluation_time)[-1][1])

        specs = dict(api_adapters._DAILY_ADAPTERS)
        specs[adapter_id] = DailyAdapterSpec(SyntheticAdapter,
            ('https://synthetic-provider.invalid/daily',),
            frozenset({('raw', schema)}), frozenset({rule}))
        with patch.object(api_adapters, '_DAILY_ADAPTERS', MappingProxyType(specs)):
            p = replace(self.p, source_id='synthetic-source', account_ref='synthetic-account',
                quota_scope='synthetic-account', endpoint='https://synthetic-provider.invalid/daily',
                credential_env='KEY_SYNTHETIC', adapter_id=adapter_id, schema_version=schema,
                availability_rule_version=rule)
            c = replace(self.c, schema_version=schema)
            bounds = SourceAvailability(p.source_id, rule, c.fingerprint, self.a.entries)
            self.keys.install(p, {'KEY_SYNTHETIC': 'SYNTHETIC-OTHER&=+'})
            wire = {'daily_bar': {'session': '2026-09-30', 'open': '10.00',
                'high': '11.00', 'low': '9.00', 'close': '10.50', 'volume': '1000'}}
            self.transport.return_value = TransportResponse(200,
                {'content-type': 'application/json'}, json.dumps(wire).encode())
            batch = self.client(bounds).fetch(p, c)
            self.assertEqual(batch.prices['2026-09-30'], ROW)
            self.assertEqual(batch.source_id, 'synthetic-source')
            self.assertEqual(batch.request_context, plan_daily_request(c, NOW))
            self.assertEqual(parsed, [(wire, c, NOW)])
            self.assertEqual(parse_qs(urlsplit(self.transport.call_args.args[0]).query),
                             {'instrument': ['TEST'], 'access_token': ['SYNTHETIC-OTHER&=+']})
            self.assertEqual(batch.metadata['schema_version'], schema)
            self.assertEqual(batch.config_version,
                hashlib.sha256((p.config_hash + c.fingerprint + bounds.fingerprint).encode()).hexdigest())
            self.assertEqual(self.state.health(p.source_id)['accepted_rows'], 1)
            self.assertEqual(self.state.health(p.source_id)['active_jobs'], 0)


if __name__ == '__main__':
    unittest.main()
