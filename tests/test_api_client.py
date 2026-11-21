"""Cross-layer tests: real parser/validation/state, fake transport and controlled time."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from datetime import date, datetime, timezone
from email.utils import format_datetime
import json
from pathlib import Path
import tempfile
import threading
import traceback
import unittest
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

from api_client import ApiClient, FetchPolicy, retry_after
from api_contracts import ApiFailure, DailyContract, alpha_daily, decode_json
from api_profiles import SourceProfile, CredentialStore
from api_runtime import health_report, load_runtime
from api_state import StateStore
from api_transport import TransportError, TransportResponse, TransportPolicy


NOW = datetime(2026, 9, 30, 21, tzinfo=timezone.utc)
ROW = {'1. open': '10.00', '2. high': '11.00', '3. low': '9.00',
       '4. close': '10.50', '5. volume': '1000'}


def profile(**changes):
    values = dict(source_id='source-a', account_ref='account-a', environment='test',
        endpoint='https://source-a.invalid/query', credential_env='KEY_A',
        credential_version='v1', quota_scope='account-a', currency='USD', exchange='TESTX',
        timezone='UTC', schema_version='alphavantage-daily-v1', rights_verified=True,
        rights_review_date='2026-01-01', review_due_date='2027-01-01',
        max_attempts_per_window=100, circuit_failure_threshold=10)
    values.update(changes)
    return SourceProfile(**values)


def contract(**changes):
    values = dict(symbol='TEST', instrument_id='TESTX:TEST', currency='USD', exchange='TESTX',
        timezone='UTC', price_basis='raw', calendar_version='fixture-calendar-v1',
        calendar_valid_from=date(2026, 9, 29), calendar_valid_until=date(2026, 10, 2),
        sessions=tuple((date.fromisoformat(day), datetime.fromisoformat(day+'T20:00:00+00:00'))
                       for day in ('2026-09-29','2026-09-30','2026-10-01','2026-10-02')))
    values.update(changes)
    return DailyContract(**values)


def payload():
    return {'Meta Data': {'2. Symbol':'TEST', '3. Last Refreshed':'2026-09-30',
                         '5. Time Zone':'UTC'}, 'Time Series (Daily)': {'2026-09-30': dict(ROW)}}


def response(data=None):
    return TransportResponse(200, {'content-type':'application/json'},
                             json.dumps(payload() if data is None else data).encode())


class Clock:
    def __init__(self): self.elapsed = 0.0
    def wall(self): return NOW.timestamp() + self.elapsed
    def monotonic(self): return self.elapsed
    def sleep(self, seconds): self.elapsed += seconds


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.state = StateStore(Path(self.tmp.name)/'state.sqlite')
        self.keys = CredentialStore(self.state)
        self.p = profile(); self.c = contract(); self.clock = Clock()
        self.keys.install(self.p, {'KEY_A':'FAKE-SECRET-A'})
        self.transport = Mock(return_value=response())
        self.client = ApiClient(self.state, self.keys, transport=self.transport,
            monotonic=self.clock.monotonic, wall=self.clock.wall,
            sleep=self.clock.sleep, jitter=lambda: 1)

    def fetch(self): return self.client.fetch(self.p, self.c)

    def test_success_preserves_source_snapshot_types_and_no_secret_repr(self):
        batch = self.fetch()
        self.assertEqual(batch.prices['2026-09-30'], ROW)
        self.assertEqual((batch.source_id,batch.symbol,batch.instrument_id),('source-a','TEST','TESTX:TEST'))
        self.assertEqual(batch.observed_at, datetime(2026,9,30,20,tzinfo=timezone.utc))
        self.assertEqual(batch.received_at, NOW)
        self.assertIs(type(batch.storage_context['metadata']),dict)
        self.assertNotIn('FAKE-SECRET',repr(batch))
        with self.assertRaises(TypeError): batch.prices['x'] = {}
        self.assertEqual(parse_qs(urlsplit(self.transport.call_args.args[0]).query)['apikey'],['FAKE-SECRET-A'])
        self.assertEqual(self.state.health('source-a')['accepted_rows'],1)

    def test_retry_then_success_obeys_shared_cooldown_and_actual_attempt_budget(self):
        self.transport.side_effect = [TransportError('http',status=503,retry_after='2'),response()]
        self.fetch()
        self.assertEqual(self.transport.call_count,2)
        self.assertEqual(self.clock.elapsed,2)
        self.assertEqual(self.state.health('source-a')['rejected_jobs'],1)

    def test_retry_exhaustion_releases_all_workers(self):
        self.transport.side_effect = TransportError('network')
        with self.assertRaises(ApiFailure) as caught: self.fetch()
        self.assertEqual(caught.exception.code,'network')
        self.assertEqual(self.transport.call_count,3)
        self.assertEqual(self.state.health('source-a')['active_jobs'],0)

    def test_permanent_statuses_do_not_retry_or_guess_region(self):
        for status,code in ((400,'http'),(401,'authentication'),(403,'permission_unknown'),(404,'http')):
            with self.subTest(status=status):
                self.transport.reset_mock(); self.transport.side_effect=TransportError('http',status=status)
                with self.assertRaises(ApiFailure) as caught: self.fetch()
                self.assertEqual(caught.exception.code,code)
                self.transport.assert_called_once()

    def test_long_retry_after_pauses_without_retrying_early_and_survives_restart(self):
        self.transport.side_effect=TransportError('http',status=429,retry_after='3600')
        with self.assertRaises(ApiFailure) as caught: self.fetch()
        self.assertEqual(caught.exception.code,'retry_budget')
        self.transport.assert_called_once()
        other=StateStore(self.state.path)
        fresh=ApiClient(other,self.keys,transport=self.transport,wall=self.clock.wall,
                        monotonic=self.clock.monotonic,sleep=self.clock.sleep)
        with self.assertRaises(ApiFailure) as caught: fresh.fetch(self.p,self.c)
        self.assertEqual(caught.exception.code,'cooldown')
        self.transport.assert_called_once()

    def test_invalid_retry_after_falls_back_to_bounded_delay(self):
        self.transport.side_effect=[TransportError('http',status=503,retry_after='secret\nBAD'),response()]
        self.fetch(); self.assertEqual(self.clock.elapsed,1)

    def test_response_after_monotonic_deadline_is_rejected(self):
        def slow(*args,**kwargs): self.clock.elapsed=31; return response()
        self.transport.side_effect=slow
        with self.assertRaises(ApiFailure) as caught: self.fetch()
        self.assertEqual(caught.exception.code,'deadline')
        self.assertEqual(self.state.health('source-a')['active_jobs'],0)

    def test_revoked_inflight_key_cannot_publish_result(self):
        def revoke(*args,**kwargs):
            self.keys.revoke('source-a','v1',now=self.clock.wall()); return response()
        self.transport.side_effect=revoke
        with self.assertRaises(ApiFailure) as caught: self.fetch()
        self.assertEqual(caught.exception.code,'credential_revoked')
        self.assertIsNone(self.state.health('source-a')['last_success'])

    def test_two_sources_overlap_reverse_return_order_without_key_or_data_mixing(self):
        b=profile(source_id='source-b',account_ref='account-b',endpoint='https://source-b.invalid/query',
                  credential_env='KEY_B',quota_scope='account-b')
        self.keys.install(b,{'KEY_B':'FAKE-SECRET-B'})
        arrived=threading.Barrier(2); released=threading.Event(); seen=[]
        def transport(url,**kwargs):
            parsed=urlsplit(url); seen.append((parsed.hostname,parse_qs(parsed.query)['apikey'][0]))
            arrived.wait(timeout=3)
            if parsed.hostname=='source-a.invalid': self.assertTrue(released.wait(timeout=3))
            else: released.set()
            value=payload(); value['Time Series (Daily)']['2026-09-30']['5. volume']='10' if parsed.hostname=='source-a.invalid' else '20'
            return response(value)
        self.client.transport=transport
        with ThreadPoolExecutor(max_workers=2) as pool:
            a_future=pool.submit(self.client.fetch,self.p,self.c)
            b_future=pool.submit(self.client.fetch,b,self.c)
            results=[a_future.result(timeout=5),b_future.result(timeout=5)]
        self.assertCountEqual(seen,[('source-a.invalid','FAKE-SECRET-A'),('source-b.invalid','FAKE-SECRET-B')])
        self.assertEqual([(x.source_id,x.prices['2026-09-30']['5. volume']) for x in results],
                         [('source-a','10'),('source-b','20')])

    def test_scope_and_unverified_profile_fail_before_transport(self):
        for p in (replace(self.p,rights_verified=False),replace(self.p,currency='EUR')):
            with self.subTest(profile=p.source_id):
                with self.assertRaises(ApiFailure): self.client.fetch(p,self.c)
        self.transport.assert_not_called()

    def test_production_profile_cannot_use_test_tls_policy(self):
        p=replace(self.p,environment='production')
        self.keys.install(p,{'KEY_A':'FAKE-SECRET-A'})
        self.client.transport_policy=TransportPolicy('https://source-a.invalid',test_mode=True)
        with self.assertRaises(ApiFailure) as caught: self.client.fetch(p,self.c)
        self.assertEqual(caught.exception.code,'environment_mismatch')
        self.transport.assert_not_called()

    def test_provider_errors_win_even_when_valid_series_is_present(self):
        for field,code in (('Error Message','provider_error'),('Information','provider_information'),('Note','provider_note')):
            for value in ('FAKE-SECRET-A\nFORGED', '', None, 0, [], {}):
                with self.subTest(field=field,value=value):
                    body=payload(); body[field]=value; self.transport.return_value=response(body)
                    with self.assertRaises(ApiFailure) as caught: self.fetch()
                    self.assertEqual(caught.exception.code,code if value else 'provider_error_format')
                    self.assertNotIn('FAKE-SECRET', ''.join(traceback.format_exception(caught.exception)))

    def test_error_field_priority_and_unknown_failure_are_safe(self):
        body=payload(); body.update({'Error Message':'bad','Information':'info','Note':'note'})
        self.transport.return_value=response(body)
        with self.assertRaises(ApiFailure) as caught: self.fetch()
        self.assertEqual(caught.exception.code,'provider_error')
        self.transport.side_effect=RuntimeError('FAKE-SECRET-A in URL')
        with self.assertRaises(ApiFailure) as caught: self.fetch()
        self.assertEqual(caught.exception.code,'unexpected_failure')
        self.assertNotIn('FAKE-SECRET-A',''.join(traceback.format_exception(caught.exception)))

    def test_invalid_prices_are_rejected_before_success(self):
        body=payload(); body['Time Series (Daily)']['2026-09-30']['1. open']='12'
        self.transport.return_value=response(body)
        with self.assertRaises(ApiFailure) as caught: self.fetch()
        self.assertEqual(caught.exception.code,'invalid_prices')
        self.assertIsNone(self.state.health('source-a')['last_success'])

    def test_health_detects_silence_and_old_data_without_duplicate_alerts(self):
        self.fetch()
        first=health_report(self.state,'source-a',now=self.clock.wall(),max_silence_seconds=10,max_data_age_seconds=7200)
        self.assertFalse(first['stale'])
        later=self.clock.wall()+11
        self.assertTrue(health_report(self.state,'source-a',now=later,max_silence_seconds=10)['changed'])
        self.assertFalse(health_report(self.state,'source-a',now=later,max_silence_seconds=10)['changed'])

    def test_explicit_fallback_requires_compatible_data_and_transient_failure(self):
        b=replace(self.p,source_id='source-b',account_ref='account-b',quota_scope='account-b',
                  endpoint='https://source-b.invalid/query',credential_env='KEY_B')
        self.keys.install(b,{'KEY_B':'FAKE-B'})
        self.client.policy=FetchPolicy(max_attempts=1)
        with self.assertRaises(ApiFailure): self.client.fetch_with_fallback(self.p,b,self.c)
        with self.assertRaises(ApiFailure):
            self.client.fetch_with_fallback(self.p,replace(b,currency='EUR'),self.c,
                                           approved_sources=(('source-a','source-b'),))
        self.transport.side_effect=[TransportError('network'),response()]
        batch=self.client.fetch_with_fallback(self.p,b,self.c,approved_sources=(('source-a','source-b'),))
        self.assertEqual(batch.source_id,'source-b')


class ContractTests(unittest.TestCase):
    def test_duplicate_dates_price_fields_and_nonfinite_numbers_rejected(self):
        for body in (b'{"a":1,"a":2}',b'{"a":{"close":1,"close":2}}',
                     b'{"2026-09-30":{},"2026-09-30":{}}', b'{"a":NaN}',
                     b'{"a":Infinity}',b'{"a":-Infinity}',b'{"a":1e999}'):
            with self.subTest(body=body):
                with self.assertRaises(ApiFailure): decode_json(body)

    def test_json_size_depth_and_quoted_brackets(self):
        for size in (1,2):
            self.assertEqual(decode_json(b'1',max_bytes=size),1)
        with self.assertRaises(ApiFailure): decode_json(b'123',max_bytes=2)
        with self.assertRaises(ApiFailure): decode_json(b'['*17+b'0'+b']'*17)
        self.assertEqual(decode_json(b'{"text":"[[[\\\"{{"}'),{'text':'[[["{{'})

    def test_metadata_mismatch_and_missing_are_rejected(self):
        for key,value in (('2. Symbol','OTHER'),('5. Time Zone','Europe/Stockholm'),
                          ('currency','EUR'),('exchange','OTHER'),('price_basis','adjusted')):
            with self.subTest(key=key):
                body=payload(); body['Meta Data'][key]=value
                with self.assertRaises(ApiFailure): alpha_daily(body,contract(),NOW)
        body=payload(); del body['Meta Data']
        with self.assertRaises(ApiFailure): alpha_daily(body,contract(),NOW)

    def test_old_future_and_uncovered_rows_are_rejected(self):
        for day in ('2026-09-29','2026-10-01','2020-01-01'):
            with self.subTest(day=day):
                body=payload(); body['Time Series (Daily)']={day:ROW}
                with self.assertRaises(ApiFailure): alpha_daily(body,contract(),NOW)
        body=payload(); body['Time Series (Daily)']['2020-01-01']=ROW
        with self.assertRaises(ApiFailure) as caught: alpha_daily(body,contract(),NOW)
        self.assertEqual(caught.exception.code,'calendar_coverage')

    def test_required_sessions_row_limit_and_pagination_are_explicit(self):
        with self.assertRaises(ApiFailure): alpha_daily(payload(),contract(minimum_sessions=2),NOW)
        body=payload(); body['next_page']='https://other.invalid'
        with self.assertRaises(ApiFailure) as caught: alpha_daily(body,contract(),NOW)
        self.assertEqual(caught.exception.code,'unsupported_pagination')
        body=payload(); body['Time Series (Daily)']['2026-09-29']=ROW
        with self.assertRaises(ApiFailure): alpha_daily(body,contract(max_rows=1),NOW)

    def test_exchange_calendar_controls_holidays_early_close_and_dst(self):
        c=contract(timezone='America/New_York',accepted_timezones=('US/Eastern',),
            calendar_valid_from=date(2026,10,30),calendar_valid_until=date(2026,11,3),
            sessions=((date(2026,10,30),datetime(2026,10,30,20,tzinfo=timezone.utc)),
                      (date(2026,11,2),datetime(2026,11,2,18,tzinfo=timezone.utc))))
        # Explicit synthetic early close after DST, weekend omitted by reviewed fixture.
        self.assertEqual(c.completed_sessions(datetime(2026,11,1,21,tzinfo=timezone.utc))[-1][0],date(2026,10,30))
        self.assertEqual(c.completed_sessions(datetime(2026,11,2,17,tzinfo=timezone.utc))[-1][0],date(2026,10,30))
        self.assertEqual(c.completed_sessions(datetime(2026,11,2,19,tzinfo=timezone.utc))[-1][0],date(2026,11,2))

    def test_calendar_expiry_and_stale_calendar_stop_before_provider(self):
        for now in (datetime(2026,10,3,tzinfo=timezone.utc),datetime(2026,9,28,tzinfo=timezone.utc)):
            with self.assertRaises(ApiFailure): contract().completed_sessions(now)
        with self.assertRaises(ApiFailure): contract(max_age_seconds=1).completed_sessions(NOW)

    def test_retry_after_forms_and_fetch_budgets(self):
        self.assertEqual(retry_after('12',NOW.timestamp()),12)
        self.assertEqual(retry_after(format_datetime(NOW),NOW.timestamp()-5),5)
        for value in ('-1','garbage','\nsecret',None): self.assertEqual(retry_after(value,NOW.timestamp()),0)
        for values in ({'max_attempts':0},{'max_bytes':0},{'total_seconds':float('inf')},{'base_delay':0}):
            with self.assertRaises(ApiFailure): FetchPolicy(**values)

    def test_unreviewed_runtime_never_reads_secret_or_contacts_provider(self):
        with self.assertRaises(ApiFailure) as caught: load_runtime('TEST',{})
        self.assertEqual(caught.exception.code,'configuration_required')
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'config.json'
            path.write_text('{"version":1,"version":2}')
            with self.assertRaises(ApiFailure) as caught: load_runtime('TEST',{'LABBET_API_CONFIG':str(path)})
            self.assertEqual(caught.exception.code,'duplicate_json_field')


if __name__ == '__main__': unittest.main()
