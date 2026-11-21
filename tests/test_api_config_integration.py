"""Saved ONDS registry through the real client; Docker-only, synthetic responses.

Run with scripts/verify_api_config_isolated.py. Only the freshness-gap test
changes an in-memory contract; the mounted registry is always unchanged.
"""
from contextlib import contextmanager
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'ingestion'))
from api_contracts import ApiFailure, DailyContract
from api_runtime import load_runtime
from api_transport import TransportError, TransportResponse

NOW = datetime(2026, 10, 16, 22, tzinfo=timezone.utc)
CONFIG = ROOT / 'config/api_sources.json'
STATE = Path('/var/lib/labbet-api/state.sqlite')
KEY = 'SYNTHETIC-OFFLINE-ONDS-KEY'
ROW = {'1. open': '10.00', '2. high': '11.00', '3. low': '9.00',
       '4. close': '10.50', '5. volume': '1000'}


class ConfigIntegrationTests(unittest.TestCase):
    def setUp(self):
        # Refuse to clean this absolute state path outside the dedicated tmpfs.
        mounts = Path('/proc/self/mountinfo').read_text().splitlines()
        self.assertTrue(any(line.split()[4] == str(STATE.parent)
                            and ' - tmpfs ' in line for line in mounts))
        self.before = hashlib.sha256(CONFIG.read_bytes()).hexdigest()
        self.addCleanup(lambda: self.assertEqual(
            self.before, hashlib.sha256(CONFIG.read_bytes()).hexdigest()))

    @contextmanager
    def runtime(self, now=NOW, load_day=None):
        # Each case gets empty state, including cases within a subTest loop.
        self.assertFalse(STATE.exists())
        class ControlledDate(date):
            @classmethod
            def today(cls):
                return load_day or now.date()
        with patch('api_profiles.date', ControlledDate):
            client = None
            try:
                client, profile, contract = load_runtime('ONDS', environ={
                    'LABBET_API_CONFIG': str(CONFIG), 'ALPHAVANTAGE_API_KEY': KEY,
                })
                client.wall = lambda: now.timestamp()
                client.monotonic = lambda: 0.0
                client.sleep = Mock(side_effect=AssertionError('No retry sleep expected'))
                client.transport = Mock(side_effect=AssertionError('Simulated response required'))
                yield client, profile, contract
            finally:
                if client is not None:
                    client.credentials.close()
                    client.state.close()
                # Only our Docker tmpfs, never host or production data.
                for name in ('state.sqlite', 'state.sqlite-wal', 'state.sqlite-shm',
                             'state.sqlite-journal'):
                    (STATE.parent / name).unlink(missing_ok=True)

    def body(self, contract, now=NOW):
        # Independent selection from the saved close instants, not the method
        # under test. Hard-coded expected last dates are checked below.
        days = [day.isoformat() for day, close in contract.sessions if close <= now][-100:]
        self.assertEqual(len(days), 100)
        return {'Meta Data': {'2. Symbol': 'ONDS', '3. Last Refreshed': days[-1],
                              '5. Time Zone': 'America/New_York'},
                'Time Series (Daily)': {day: dict(ROW) for day in days}}

    def send(self, client, profile, contract, body):
        raw = json.dumps(body).encode('utf-8')
        client.transport.return_value = TransportResponse(200, {'content-type': 'application/json'}, raw)
        client.transport.side_effect = None
        batch = client.fetch(profile, contract)
        self.assertEqual(batch.payload_hash, hashlib.sha256(raw).hexdigest())
        client.transport.assert_called_once()
        return batch

    def reject(self, client, profile, contract, body, code):
        with self.assertRaises(ApiFailure) as caught:
            self.send(client, profile, contract, body)
        self.assertEqual(caught.exception.code, code)
        client.transport.assert_called_once()
        client.sleep.assert_not_called()

    def test_saved_values_reach_runtime_without_configuration_changes(self):
        with self.runtime() as (client, profile, contract):
            saved = json.loads(CONFIG.read_bytes())
            entry = saved['sources'][0]
            for field, value in entry['profile'].items():
                self.assertEqual(getattr(profile, field), value)
            self.assertEqual(asdict(client.policy), entry['fetch_policy'])
            raw = entry['instruments'][0]
            for field, value in raw.items():
                actual = getattr(contract, field)
                if field.startswith('calendar_valid_'):
                    value = date.fromisoformat(value)
                elif field == 'sessions':
                    value = tuple((date.fromisoformat(d), datetime.fromisoformat(c)) for d, c in value)
                elif field == 'accepted_timezones':
                    value = tuple(value)
                self.assertEqual(actual, value)
            self.assertIsInstance(contract, DailyContract)
            self.assertEqual((contract.minimum_sessions, contract.max_rows,
                              contract.max_age_seconds, len(contract.sessions)), (100, 100, 604800, 251))
            self.assertEqual(client.policy.max_attempts, 1)
            self.assertEqual(str(client.state.path), saved['state_path'])
            self.assertEqual(client.credentials.snapshot(profile, NOW.timestamp()).secret, KEY)

    def test_complete_100_rows_and_canonical_timezone_accepted(self):
        with self.runtime() as (client, profile, contract):
            body = self.body(contract)
            self.assertEqual(max(body['Time Series (Daily)']), '2026-10-16')
            batch = self.send(client, profile, contract, body)
            self.assertEqual(set(batch.prices), set(body['Time Series (Daily)']))
            self.assertEqual(len(batch.prices), 100)
            self.assertEqual((batch.symbol, batch.instrument_id, batch.source_id),
                             ('ONDS', 'XNAS:ONDS', 'alphavantage'))
            self.assertEqual(batch.observed_at, datetime(2026, 10, 16, 20, tzinfo=timezone.utc))
            self.assertEqual(batch.received_at, NOW)
            query = parse_qs(urlsplit(client.transport.call_args.args[0]).query)
            self.assertEqual(query, {'function': ['TIME_SERIES_DAILY'], 'symbol': ['ONDS'],
                                     'outputsize': ['compact'], 'apikey': [KEY]})
            self.assertFalse(client.transport.call_args.kwargs['policy'].test_mode)

    def test_explicit_us_eastern_alias_accepted(self):
        with self.runtime() as (client, profile, contract):
            body = self.body(contract)
            body['Meta Data']['5. Time Zone'] = 'US/Eastern'
            self.assertEqual(len(self.send(client, profile, contract, body).prices), 100)

    def test_99_rows_rejected(self):
        with self.runtime() as (client, profile, contract):
            body = self.body(contract)
            body['Time Series (Daily)'].pop(min(body['Time Series (Daily)']))
            self.reject(client, profile, contract, body, 'incomplete_series')

    def test_101_rows_rejected(self):
        with self.runtime() as (client, profile, contract):
            body = self.body(contract)
            days = [d.isoformat() for d, c in contract.sessions if c <= NOW]
            body['Time Series (Daily)'][days[-101]] = dict(ROW)
            self.reject(client, profile, contract, body, 'row_limit')

    def test_100_rows_with_required_day_replaced_by_older_day_rejected(self):
        with self.runtime() as (client, profile, contract):
            body = self.body(contract)
            days = [d.isoformat() for d, c in contract.sessions if c <= NOW]
            body['Time Series (Daily)'].pop(days[-50])
            body['Time Series (Daily)'][days[-101]] = dict(ROW)
            self.assertEqual(len(body['Time Series (Daily)']), 100)
            self.reject(client, profile, contract, body, 'incomplete_series')

    def test_wrong_symbol_and_unapproved_timezone_rejected(self):
        for field, value, code in (('2. Symbol', 'OTHER', 'instrument_mismatch'),
                                   ('5. Time Zone', 'Europe/Stockholm', 'timezone_mismatch')):
            with self.subTest(field=field), self.runtime() as (client, profile, contract):
                body = self.body(contract)
                body['Meta Data'][field] = value
                self.reject(client, profile, contract, body, code)

    def test_missing_metadata_and_explicit_conflicts_rejected(self):
        for field, value, code in ((None, None, 'missing_metadata'),
                                   ('currency', 'EUR', 'metadata_mismatch'),
                                   ('exchange', 'OTHER', 'metadata_mismatch'),
                                   ('price_basis', 'adjusted', 'metadata_mismatch')):
            with self.subTest(field=field), self.runtime() as (client, profile, contract):
                body = self.body(contract)
                if field is None:
                    del body['Meta Data']
                else:
                    body['Meta Data'][field] = value
                self.reject(client, profile, contract, body, code)

    def test_last_refreshed_old_future_missing_or_invalid_rejected(self):
        for refreshed, code in (('2026-10-15', 'stale_or_future_data'),
                                 ('2026-10-19', 'stale_or_future_data'),
                                 (None, 'invalid_date'), ('2026-10-16 16:00:00', 'invalid_date')):
            with self.subTest(refreshed=refreshed), self.runtime() as (client, profile, contract):
                body = self.body(contract)
                if refreshed is None:
                    del body['Meta Data']['3. Last Refreshed']
                else:
                    body['Meta Data']['3. Last Refreshed'] = refreshed
                self.reject(client, profile, contract, body, code)

    def test_old_or_future_latest_price_date_rejected(self):
        for future in (False, True):
            with self.subTest(future=future), self.runtime() as (client, profile, contract):
                body = self.body(contract)
                prices = body['Time Series (Daily)']
                if future:
                    prices.pop(min(prices))
                    prices['2026-10-19'] = dict(ROW)
                else:
                    prices.pop(max(prices))
                    days = [d.isoformat() for d, c in contract.sessions if c <= NOW]
                    prices[days[-101]] = dict(ROW)
                body['Meta Data']['3. Last Refreshed'] = max(prices)
                self.reject(client, profile, contract, body, 'incomplete_series')

    def test_close_boundary_weekend_and_exchange_holiday(self):
        for text, expected in (
            ('2026-10-16T19:59:59.999999+00:00', '2026-10-15'),
            ('2026-10-16T20:00:00+00:00', '2026-10-16'),
            ('2026-10-16T20:00:00.000001+00:00', '2026-10-16'),
            ('2026-10-17T22:00:00+00:00', '2026-10-16'),
            ('2026-10-18T22:00:00+00:00', '2026-10-16'),
            ('2026-09-07T22:00:00+00:00', '2026-09-04'),
        ):
            now = datetime.fromisoformat(text)
            # September holiday predates rights approval. Test the real contract
            # and client at that instant; load at the reviewed October date.
            # Client still validates actual rights and would reject September.
            with self.subTest(now=text), self.runtime(now=NOW) as (client, profile, contract):
                self.assertEqual(contract.completed_sessions(now)[-1][0].isoformat(), expected)
                if now.date() < date(2026, 10, 15):
                    continue
                client.wall = lambda: now.timestamp()
                batch = self.send(client, profile, contract, self.body(contract, now))
                self.assertEqual(max(batch.prices), expected)

    def test_calendar_validity_boundaries_without_rights_masking(self):
        # Direct real contract: rights expire before end of calendar year.
        with self.runtime() as (_, _, contract):
            for text in ('2026-01-01T04:59:59.999999+00:00', '2027-01-01T05:00:00+00:00'):
                with self.subTest(now=text), self.assertRaises(ApiFailure) as caught:
                    contract.completed_sessions(datetime.fromisoformat(text))
                self.assertEqual(caught.exception.code, 'calendar_expired')
            self.assertEqual(contract.completed_sessions(datetime.fromisoformat(
                '2027-01-01T04:59:59.999999+00:00'))[-1][0], date(2026, 12, 31))
            with self.assertRaises(ApiFailure) as caught:
                contract.completed_sessions(datetime.fromisoformat('2026-01-01T05:00:00+00:00'))
            self.assertEqual(caught.exception.code, 'no_completed_session')

    def test_rights_due_day_accepted_next_day_rejected_in_loader_and_client(self):
        due = datetime(2026, 11, 15, 23, 59, 59, tzinfo=timezone.utc)
        with self.runtime(due) as (client, profile, contract):
            self.assertEqual(len(self.send(client, profile, contract, self.body(contract, due)).prices), 100)
        expired = datetime(2026, 11, 16, tzinfo=timezone.utc)
        with self.assertRaises(ApiFailure) as caught:
            with self.runtime(expired):
                self.fail('Expired registry loaded')
        self.assertEqual(caught.exception.code, 'rights_review_expired')
        self.assertFalse(STATE.exists())
        with self.runtime() as (client, profile, contract):
            client.wall = lambda: expired.timestamp()
            with self.assertRaises(ApiFailure) as caught:
                client.fetch(profile, contract)
            self.assertEqual(caught.exception.code, 'rights_review_expired')
            client.transport.assert_not_called()

    def test_synthetic_calendar_gap_exact_604800_seconds_and_just_over(self):
        # Deliberate IN-MEMORY variant: remove all sessions after October 16.
        # Both times remain inside actual rights and calendar validity windows.
        for extra, code in ((0, None), (1, 'stale_calendar')):
            now = datetime(2026, 10, 23, 20, tzinfo=timezone.utc) + timedelta(microseconds=extra)
            with self.subTest(extra_microseconds=extra), self.runtime(now) as (client, profile, contract):
                variant = replace(contract, sessions=tuple((d, c) for d, c in contract.sessions if c <= NOW))
                self.assertEqual(variant.max_age_seconds, 604800)
                self.assertEqual(contract.sessions[-1][0], date(2026, 12, 31))
                if code:
                    with self.assertRaises(ApiFailure) as caught:
                        client.fetch(profile, variant)
                    self.assertEqual(caught.exception.code, code)
                    client.transport.assert_not_called()
                else:
                    batch = self.send(client, profile, variant, self.body(variant, now))
                    self.assertEqual((now - batch.observed_at).total_seconds(), 604800)

    def test_one_attempt_even_on_transient_transport_error(self):
        with self.runtime() as (client, profile, contract):
            client.transport.side_effect = TransportError('network')
            with self.assertRaises(ApiFailure) as caught:
                client.fetch(profile, contract)
            self.assertEqual(caught.exception.code, 'network')
            client.transport.assert_called_once()
            client.sleep.assert_not_called()


if __name__ == '__main__':
    unittest.main()
