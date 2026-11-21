"""Availability across real profiles, SQLite, loader and API orchestration.

All market responses, credentials and clocks are synthetic. No provider access.
"""
from copy import deepcopy
from contextlib import closing
from dataclasses import asdict, FrozenInstanceError, replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from types import MappingProxyType
import unittest
from unittest.mock import Mock, patch

import api_adapters
from api_adapters import AlphaDailyAdapter, daily_adapter_spec
from api_availability import availability_to_json, plan_daily_request
from api_client import ApiClient, FetchPolicy
from api_contracts import ApiFailure, SessionAvailability, SourceAvailability
from api_profiles import CredentialStore, ProfileError
from api_runtime import load_runtime
from api_state import StateRejected, StateStore
from api_storage import _metadata, persist_batch
from api_transport import TransportError
from test_api_client import NOW, ROW, contract, payload, profile, response


RULE = "reviewed-session-bounds-v1"


def schedule(p, c, *, overrides=None):
    overrides = {} if overrides is None else overrides
    return SourceAvailability(p.source_id, RULE, c.fingerprint,
        tuple(SessionAvailability(day, overrides.get(day, close))
              for day, close in c.sessions))


def dated_payload(day):
    result = payload()
    result['Meta Data']['3. Last Refreshed'] = day
    result['Time Series (Daily)'] = {day: dict(ROW)}
    return result


class ControlledClock:
    def __init__(self, origin=NOW):
        self.origin, self.elapsed = origin, 0.0

    def wall(self):
        return self.origin.timestamp() + self.elapsed

    def monotonic(self):
        return self.elapsed

    def sleep(self, seconds):
        self.elapsed += seconds


class AvailabilityIntegrationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.folder = Path(temporary.name)
        self.state = StateStore(self.folder / 'state.sqlite')
        self.addCleanup(self.state.close)
        self.keys = CredentialStore(self.state)
        self.addCleanup(self.keys.close)
        self.p = profile(availability_rule_version=RULE)
        self.c = contract()
        self.a = schedule(self.p, self.c)
        self.clock = ControlledClock()
        self.keys.install(self.p, {'KEY_A': 'SYNTHETIC-AVAILABILITY-A'})
        self.transport = Mock(return_value=response())

    def client(self, *schedules, policy=None):
        return ApiClient(self.state, self.keys, availabilities=schedules,
            policy=policy, transport=self.transport, wall=self.clock.wall,
            monotonic=self.clock.monotonic, sleep=self.clock.sleep, jitter=lambda: 1)

    def backup(self):
        p = replace(self.p, source_id='source-b', account_ref='account-b',
                    quota_scope='account-b', endpoint='https://source-b.invalid/query',
                    credential_env='KEY_B')
        self.keys.install(p, {'KEY_B': 'SYNTHETIC-AVAILABILITY-B'})
        return p, schedule(p, self.c)

    def reject(self, code, operation):
        with self.assertRaises(ApiFailure) as caught:
            operation()
        self.assertEqual(caught.exception.code, code)
        self.assertNotIn('SYNTHETIC-AVAILABILITY', str(caught.exception))

    def snapshot(self):
        with closing(sqlite3.connect(self.state.path)) as db:
            tables = sorted(row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"))
            return tuple((name, tuple(db.execute('SELECT * FROM ' + name)))
                         for name in tables)

    def test_success_keeps_observation_receipt_and_request_times_distinct(self):
        def transport(*args, **kwargs):
            self.clock.elapsed += 2
            return response()
        self.transport.side_effect = transport
        result = self.client(self.a).fetch(self.p, self.c)
        self.assertEqual(result.observed_at, self.c.sessions[1][1])
        self.assertEqual(result.received_at, NOW + timedelta(seconds=2))
        self.assertEqual(result.request_context, plan_daily_request(self.c, NOW))
        self.assertEqual(self.state.health(self.p.source_id)['data_age_seconds'], 3602)
        self.assertEqual(result.prices['2026-09-30'], ROW)

    def test_missing_schedule_stops_before_state_credentials_and_network(self):
        client = self.client()
        before = self.snapshot()
        with (patch.object(self.state, 'register', side_effect=AssertionError('state forbidden')),
              patch.object(self.keys, 'snapshot', side_effect=AssertionError('keys forbidden'))):
            self.reject('availability_required', lambda: client.fetch(self.p, self.c))
        self.transport.assert_not_called()
        self.assertEqual(self.snapshot(), before)

    def test_wrong_source_or_contract_schedule_cannot_satisfy_selected_binding(self):
        for candidate in (replace(self.a, source_id='other-source'),
                          replace(self.a, contract_fingerprint='f' * 64)):
            with self.subTest(source=candidate.source_id):
                before = self.snapshot()
                self.reject('availability_required',
                            lambda: self.client(candidate).fetch(self.p, self.c))
                self.assertEqual(self.snapshot(), before)
        self.transport.assert_not_called()

    def test_rule_coverage_and_preclose_errors_leave_existing_state_unchanged(self):
        cases = (
            (replace(self.a, rule_version='unapproved-rule'), 'availability_rule_mismatch'),
            (replace(self.a, entries=self.a.entries[:-1]), 'availability_coverage'),
            (replace(self.a, entries=(replace(self.a.entries[0],
                not_before=self.a.entries[0].not_before - timedelta(microseconds=1)),)
                + self.a.entries[1:]), 'availability_before_close'),
        )
        for candidate, expected in cases:
            with self.subTest(code=expected):
                before = self.snapshot()
                self.reject(expected, lambda: self.client(candidate).fetch(self.p, self.c))
                self.assertEqual(self.snapshot(), before)
        self.transport.assert_not_called()

    def test_not_yet_available_rejects_without_attempt_or_older_window(self):
        candidate = schedule(self.p, self.c, overrides={
            date(2026, 9, 30): NOW + timedelta(microseconds=1)})
        before = self.snapshot()
        self.reject('data_not_available', lambda: self.client(candidate).fetch(self.p, self.c))
        self.assertEqual(self.snapshot(), before)
        self.transport.assert_not_called()

    def test_exact_gate_is_accepted(self):
        candidate = schedule(self.p, self.c, overrides={date(2026, 9, 30): NOW})
        batch = self.client(candidate).fetch(self.p, self.c)
        self.assertEqual(batch.request_context.sessions, (date(2026, 9, 30),))
        self.assertEqual(self.transport.call_count, 1)

    def test_legacy_profile_cannot_silently_ignore_schedule_or_explicit_request(self):
        legacy = replace(self.p, availability_rule_version=None)
        for client, keywords in ((self.client(self.a), {}),
                (self.client(), {'request_context': plan_daily_request(self.c, NOW)})):
            with self.subTest(has_context=bool(keywords)):
                self.reject('availability_not_enabled',
                            lambda: client.fetch(legacy, self.c, **keywords))
        self.transport.assert_not_called()

    def test_future_or_tampered_request_is_rejected_before_attempt(self):
        request = plan_daily_request(self.c, NOW)
        variants = ((replace(request, reference_time=NOW + timedelta(seconds=1)), 'future_request'),
                    (replace(request, contract_fingerprint='f' * 64), 'request_contract_mismatch'),
                    (replace(request, sessions=(date(2026, 9, 29),)), 'request_sessions_mismatch'))
        for candidate, expected in variants:
            with self.subTest(code=expected):
                before = self.snapshot()
                self.reject(expected, lambda: self.client(self.a).fetch(
                    self.p, self.c, request_context=candidate))
                self.assertEqual(self.snapshot(), before)
        self.transport.assert_not_called()

    def test_response_after_next_close_keeps_original_requested_session(self):
        self.clock = ControlledClock(datetime(2026, 9, 30, 19, 59, 59, tzinfo=timezone.utc))
        def transport(*args, **kwargs):
            self.clock.elapsed += 2
            return response(dated_payload('2026-09-29'))
        self.transport.side_effect = transport
        batch = self.client(self.a).fetch(self.p, self.c)
        self.assertEqual(batch.request_context.sessions, (date(2026, 9, 29),))
        self.assertEqual(tuple(batch.prices), ('2026-09-29',))
        self.assertEqual(batch.observed_at, self.c.sessions[0][1])

    def test_newer_response_after_crossing_close_is_rejected(self):
        self.clock = ControlledClock(datetime(2026, 9, 30, 19, 59, 59, tzinfo=timezone.utc))
        def transport(*args, **kwargs):
            self.clock.elapsed += 2
            return response()
        self.transport.side_effect = transport
        with self.assertRaises(ApiFailure):
            self.client(self.a).fetch(self.p, self.c)
        self.assertIsNone(self.state.health(self.p.source_id)['last_success'])
        self.assertEqual(self.state.health(self.p.source_id)['active_jobs'], 0)

    def test_backdated_request_does_not_bypass_actual_data_age(self):
        self.c = replace(self.c, max_age_seconds=3600)
        self.a = schedule(self.p, self.c)
        request = plan_daily_request(self.c, NOW - timedelta(seconds=1))
        def transport(*args, **kwargs):
            self.clock.elapsed += 1
            return response()
        self.transport.side_effect = transport
        self.reject('stale_data', lambda: self.client(self.a).fetch(
            self.p, self.c, request_context=request))
        self.assertIsNone(self.state.health(self.p.source_id)['last_success'])

    def test_every_returned_older_row_must_pass_its_own_bound(self):
        candidate = schedule(self.p, self.c, overrides={
            date(2026, 9, 29): NOW + timedelta(hours=1)})
        body = payload()
        body['Time Series (Daily)']['2026-09-29'] = dict(ROW)
        self.transport.return_value = response(body)
        self.reject('data_not_available', lambda: self.client(candidate).fetch(self.p, self.c))
        self.assertIsNone(self.state.health(self.p.source_id)['last_success'])

    def test_wall_clock_rollback_rejects_response_and_releases_attempt(self):
        def transport(*args, **kwargs):
            self.clock.origin -= timedelta(microseconds=500000)
            return response()
        self.transport.side_effect = transport
        self.reject('clock_rollback', lambda: self.client(self.a).fetch(self.p, self.c))
        self.assertIsNone(self.state.health(self.p.source_id)['last_success'])
        self.assertEqual(self.state.health(self.p.source_id)['active_jobs'], 0)

    def test_shared_guard_rejects_adapter_observation_time_substitution(self):
        with patch.object(AlphaDailyAdapter, 'parse', return_value=({'2026-09-30': dict(ROW)}, NOW)):
            self.reject('observation_mismatch', lambda: self.client(self.a).fetch(self.p, self.c))
        self.assertEqual(self.state.health(self.p.source_id)['active_jobs'], 0)

    def test_fallback_uses_backup_rule_and_preflights_before_primary_io(self):
        backup, _ = self.backup()
        late = schedule(backup, self.c, overrides={date(2026, 9, 30): NOW + timedelta(seconds=1)})
        client = self.client(self.a, late)
        self.transport.side_effect = TransportError('network')
        before = self.snapshot()
        self.reject('data_not_available', lambda: client.fetch_with_fallback(
            self.p, backup, self.c, approved_sources=((self.p.source_id, backup.source_id),)))
        self.assertEqual(self.snapshot(), before)
        self.transport.assert_not_called()

    def test_fallback_across_close_uses_identical_reference_and_sessions(self):
        backup, other = self.backup()
        self.clock = ControlledClock(datetime(2026, 9, 30, 19, 59, 59, tzinfo=timezone.utc))
        def transport(*args, **kwargs):
            if self.transport.call_count == 1:
                self.clock.elapsed += 2
                raise TransportError('network')
            return response(dated_payload('2026-09-29'))
        self.transport.side_effect = transport
        client = self.client(self.a, other, policy=FetchPolicy(max_attempts=1))
        batch = client.fetch_with_fallback(self.p, backup, self.c,
            approved_sources=((self.p.source_id, backup.source_id),))
        self.assertEqual(batch.source_id, backup.source_id)
        self.assertEqual(batch.request_context.reference_time, self.clock.origin)
        self.assertEqual(batch.request_context.sessions, (date(2026, 9, 29),))
        self.assertEqual(self.transport.call_count, 2)
        self.assertEqual(self.transport.call_args.kwargs['total_timeout'], 28)

    def test_fallback_cannot_downgrade_to_legacy_availability(self):
        backup, _ = self.backup()
        backup = replace(backup, availability_rule_version=None)
        before = self.snapshot()
        self.reject('fallback_availability_mismatch', lambda: self.client(self.a).fetch_with_fallback(
            self.p, backup, self.c, approved_sources=((self.p.source_id, backup.source_id),)))
        self.assertEqual(self.snapshot(), before)
        self.transport.assert_not_called()

    def test_fallback_preflight_time_counts_against_one_total_budget(self):
        backup, other = self.backup()
        client = self.client(self.a, other, policy=FetchPolicy(max_attempts=1))
        original = client._availability_request
        def costly_validation(*args, **kwargs):
            result = original(*args, **kwargs)
            self.clock.elapsed += 2
            return result
        self.transport.side_effect = [TransportError('network'), response()]
        with patch.object(client, '_availability_request', side_effect=costly_validation):
            result = client.fetch_with_fallback(self.p, backup, self.c,
                approved_sources=((self.p.source_id, backup.source_id),))
        self.assertEqual(result.source_id, backup.source_id)
        self.assertEqual([call.kwargs['total_timeout'] for call in self.transport.call_args_list], [24, 22])
        self.assertEqual(result.request_context.reference_time, NOW)

    def test_exhausted_fallback_preflight_budget_causes_no_attempt(self):
        backup, other = self.backup()
        client = self.client(self.a, other)
        original = client._availability_request
        def expensive_validation(*args, **kwargs):
            result = original(*args, **kwargs)
            self.clock.elapsed += 16
            return result
        before = self.snapshot()
        with patch.object(client, '_availability_request', side_effect=expensive_validation):
            self.reject('deadline', lambda: client.fetch_with_fallback(self.p, backup, self.c,
                approved_sources=((self.p.source_id, backup.source_id),)))
        self.assertEqual(self.snapshot(), before)
        self.transport.assert_not_called()

    def test_permanent_failure_does_not_use_backup_even_with_valid_bounds(self):
        backup, other = self.backup()
        self.transport.side_effect = TransportError('http', status=401)
        client = self.client(self.a, other, policy=FetchPolicy(max_attempts=1))
        self.reject('authentication', lambda: client.fetch_with_fallback(self.p, backup, self.c,
            approved_sources=((self.p.source_id, backup.source_id),)))
        self.assertEqual(self.transport.call_count, 1)
        with closing(sqlite3.connect(self.state.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM attempts WHERE scope=?',
                                         (backup.quota_scope,)).fetchone()[0], 0)

    def test_retry_uses_same_frozen_request_and_consumes_normal_quota(self):
        self.transport.side_effect = [TransportError('http', status=503), response()]
        batch = self.client(self.a).fetch(self.p, self.c)
        self.assertEqual(batch.request_context.reference_time, NOW)
        self.assertGreater(batch.received_at, NOW)
        with closing(sqlite3.connect(self.state.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0], 2)

    def test_enabled_config_binds_schedule_but_keeps_storage_metadata_compatible(self):
        batch = self.client(self.a).fetch(self.p, self.c)
        expected = hashlib.sha256((self.p.config_hash + self.c.fingerprint + self.a.fingerprint).encode()).hexdigest()
        self.assertEqual(batch.config_version, expected)
        context = batch.storage_context
        self.assertEqual(_metadata(context['metadata']), context['metadata'])
        self.assertNotIn('request_context', context)
        import inspect
        inspect.signature(persist_batch).bind(Mock(), rows=[], **context)
        with self.assertRaises(FrozenInstanceError):
            batch.request_context.reference_time = NOW + timedelta(seconds=1)

    def test_schedule_change_changes_config_version_without_resetting_consumption(self):
        first = self.client(self.a).fetch(self.p, self.c)
        changed = schedule(self.p, self.c, overrides={date(2026, 9, 29): NOW})
        second = self.client(changed).fetch(self.p, self.c)
        self.assertNotEqual(first.config_version, second.config_version)
        with closing(sqlite3.connect(self.state.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0], 2)

    def test_legacy_profile_metadata_and_batch_hash_remain_compatible(self):
        legacy = replace(self.p, availability_rule_version=None)
        self.assertNotIn('availability_rule_version', legacy.metadata())
        batch = self.client().fetch(legacy, self.c)
        expected = hashlib.sha256((legacy.config_hash + self.c.fingerprint).encode()).hexdigest()
        self.assertEqual(batch.config_version, expected)
        self.assertIsNone(batch.request_context)
        self.assertNotIn('availability', json.dumps(batch.storage_context['metadata']))

    def test_profile_rule_activation_requires_new_config_version(self):
        legacy = replace(self.p, availability_rule_version=None)
        self.state.register(legacy)
        before = self.snapshot()
        self.reject('config_version_reused', lambda: self.client(self.a).fetch(self.p, self.c))
        self.assertEqual(self.snapshot(), before)
        self.transport.assert_not_called()

    def test_inventory_reopen_keeps_rule_hash_and_consumed_quota(self):
        self.p = replace(self.p, max_attempts_per_window=1)
        self.client(self.a).fetch(self.p, self.c)
        self.keys.close()
        self.state.close()
        self.state = StateStore(self.folder / 'state.sqlite')
        self.addCleanup(self.state.close)
        self.keys = CredentialStore(self.state)
        self.addCleanup(self.keys.close)
        self.keys.install(self.p, {'KEY_A': 'SYNTHETIC-AVAILABILITY-A'})
        self.assertEqual(self.state.inventory(), [{'source_id': self.p.source_id,
            'config_version': self.p.config_version, 'config_hash': self.p.config_hash,
            'profile': self.p.metadata()}])
        self.reject('quota', lambda: self.client(self.a).fetch(self.p, self.c))
        self.assertEqual(self.transport.call_count, 1)

    def test_unknown_or_bad_rule_never_self_approves(self):
        for value in ('unknown-v1', '', True, [], {}, 1):
            with self.subTest(value=value), self.assertRaises(ProfileError) as caught:
                replace(self.p, availability_rule_version=value)
            self.assertEqual(caught.exception.code, 'unsupported_availability_rule')

    def test_previously_created_profile_fails_when_approved_rule_is_removed(self):
        specs = dict(api_adapters._DAILY_ADAPTERS)
        specs[self.p.adapter_id] = replace(daily_adapter_spec(self.p.adapter_id),
                                           availability_rules=frozenset())
        before = self.snapshot()
        with patch.object(api_adapters, '_DAILY_ADAPTERS', MappingProxyType(specs)):
            self.reject('unsupported_availability_rule',
                        lambda: self.client(self.a).fetch(self.p, self.c))
        self.assertEqual(self.snapshot(), before)
        self.transport.assert_not_called()

    def test_constructor_rejects_mutable_or_duplicate_schedule_inventory(self):
        for values in ([self.a], (self.a, self.a), ('bad',)):
            with self.subTest(kind=type(values).__name__), self.assertRaises(ApiFailure):
                ApiClient(self.state, self.keys, availabilities=values)


class AvailabilityLoaderTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.folder = Path(temporary.name)
        self.path = self.folder / 'registry.json'
        self.p = profile(environment='production', endpoint='https://www.alphavantage.co/query',
            rights_review_date='2000-01-01', review_due_date='2099-12-31',
            availability_rule_version=RULE)
        self.c = contract()
        self.a = schedule(self.p, self.c)
        instrument = asdict(self.c)
        instrument['availability'] = availability_to_json(self.a)
        self.registry = {'version': 1, 'environment': 'production',
            'state_path': str(self.folder / 'state.sqlite'), 'sources': [{
                'adapter': self.p.adapter_id, 'profile': asdict(self.p),
                'fetch_policy': {}, 'instruments': [instrument]}]}
        self.environ = {'LABBET_API_CONFIG': str(self.path), 'KEY_A': 'SYNTHETIC-LOADER-A'}

    def load(self, registry=None, environ=None):
        self.path.write_text(json.dumps(self.registry if registry is None else registry,
                                       default=str), encoding='utf-8')
        return load_runtime('TEST', self.environ if environ is None else environ)

    def test_loader_passes_bound_schedule_into_real_fetch_without_contract_hash_change(self):
        client, p, c = self.load()
        self.addCleanup(client.credentials.close)
        self.addCleanup(client.state.close)
        self.assertEqual(c.fingerprint, self.c.fingerprint)
        clock = ControlledClock()
        client.wall, client.monotonic, client.sleep = clock.wall, clock.monotonic, clock.sleep
        client.transport = Mock(return_value=response())
        result = client.fetch(p, c)
        self.assertEqual(result.request_context.reference_time, NOW)
        self.assertEqual(result.request_context.sessions, (date(2026, 9, 30),))

    def test_loader_rejects_missing_or_malformed_schedule_before_state_or_keys(self):
        variants = []
        for value in (None, [], {}, {'version': True},
                      dict(availability_to_json(self.a), unknown='SYNTHETIC-PRIVATE')):
            data = deepcopy(self.registry)
            data['sources'][0]['instruments'][0]['availability'] = value
            variants.append(data)
        missing = deepcopy(self.registry)
        del missing['sources'][0]['instruments'][0]['availability']
        variants.append(missing)
        for index, data in enumerate(variants):
            with self.subTest(index=index), patch('api_runtime.StateStore',
                    side_effect=AssertionError('state forbidden')):
                with self.assertRaises(ApiFailure) as caught:
                    self.load(data, {'LABBET_API_CONFIG': str(self.path)})
                self.assertNotIn('SYNTHETIC-PRIVATE', str(caught.exception))
                self.assertNotEqual(caught.exception.code, 'missing_credential')
            self.assertFalse((self.folder / 'state.sqlite').exists())

    def test_loader_rejects_wrong_binding_coverage_and_rule_before_state(self):
        variants = [('source_id', 'wrong', 'availability_source_mismatch'),
                    ('rule_version', 'wrong-v1', 'availability_rule_mismatch'),
                    ('contract_fingerprint', 'f' * 64, 'availability_contract_mismatch'),
                    ('entries', availability_to_json(self.a)['entries'][:-1], 'availability_coverage')]
        for field, value, code in variants:
            data = deepcopy(self.registry)
            data['sources'][0]['instruments'][0]['availability'][field] = value
            with self.subTest(field=field), patch('api_runtime.StateStore',
                    side_effect=AssertionError('state forbidden')):
                with self.assertRaises(ApiFailure) as caught:
                    self.load(data)
                self.assertEqual(caught.exception.code, code)

    def test_loader_rejects_schedule_when_profile_does_not_enable_it(self):
        self.registry['sources'][0]['profile']['availability_rule_version'] = None
        with self.assertRaises(ApiFailure) as caught:
            self.load()
        self.assertEqual(caught.exception.code, 'availability_not_enabled')
        self.assertFalse((self.folder / 'state.sqlite').exists())

    def test_unselected_enabled_instrument_must_have_valid_bound_schedule(self):
        other = deepcopy(self.registry['sources'][0]['instruments'][0])
        other['symbol'] = 'OTHER'
        other['instrument_id'] = 'TESTX:OTHER'
        # Keep the first instrument's old fingerprint: this must not be dormant bad config.
        self.registry['sources'][0]['instruments'].append(other)
        with patch('api_runtime.StateStore', side_effect=AssertionError('state forbidden')):
            with self.assertRaises(ApiFailure) as caught:
                self.load()
        self.assertEqual(caught.exception.code, 'availability_contract_mismatch')
        self.assertFalse((self.folder / 'state.sqlite').exists())

    def test_loader_legacy_registry_still_loads_without_rule_or_schedule(self):
        del self.registry['sources'][0]['profile']['availability_rule_version']
        del self.registry['sources'][0]['instruments'][0]['availability']
        client, p, c = self.load()
        self.addCleanup(client.credentials.close)
        self.addCleanup(client.state.close)
        self.assertIsNone(p.availability_rule_version)
        self.assertEqual(c.fingerprint, self.c.fingerprint)
        self.assertNotIn('availability_rule_version', p.metadata())


if __name__ == '__main__':
    unittest.main()
