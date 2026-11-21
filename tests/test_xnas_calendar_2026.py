"""Offline calendar checks against Nasdaq's reviewed 2026 equity schedule.

Reference (reviewed 2026-10-16):
https://www.nasdaq.com/market-activity/stock-market-holiday-schedule
Expected dates/times are independent of the generator's constants.
"""
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import importlib.util
import io
import json
from pathlib import Path
import socket
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts' / 'build_xnas_calendar_2026.py'
sys.path.insert(0, str(ROOT / 'ingestion'))
from api_contracts import ApiFailure, DailyContract

HOLIDAYS = frozenset((
    '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03',
    '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07',
    '2026-11-26', '2026-12-25',
))
EARLY = frozenset(('2026-11-27', '2026-12-24'))


class XnasCalendar2026Tests(unittest.TestCase):
    def setUp(self):
        guard = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        guard.start()
        self.addCleanup(guard.stop)
        spec = importlib.util.spec_from_file_location('calendar_under_test', SCRIPT)
        self.calendar = importlib.util.module_from_spec(spec)
        self.import_out, self.import_err = io.StringIO(), io.StringIO()
        with redirect_stdout(self.import_out), redirect_stderr(self.import_err):
            spec.loader.exec_module(self.calendar)
        self.sessions = self.calendar.build_sessions()
        # Same ISO conversion as api_runtime, without loading any drift config.
        self.contract = DailyContract(
            symbol='TEST', instrument_id='XNAS:TEST', currency='USD',
            exchange='XNAS', timezone='America/New_York', price_basis='raw',
            calendar_version='xnas-2026-v1', calendar_valid_from=date(2026, 1, 1),
            calendar_valid_until=date(2026, 12, 31),
            sessions=tuple((date.fromisoformat(day), datetime.fromisoformat(close))
                           for day, close in self.sessions),
        )

    def assert_code(self, code, contract, now):
        with self.assertRaises(ApiFailure) as caught:
            contract.completed_sessions(now)
        self.assertEqual(caught.exception.code, code)

    def test_exact_complete_sorted_unique_trading_days(self):
        days = [day for day, _ in self.sessions]
        # Scan the year's dates independently; never use CLOSED_DATES from script.
        expected = sorted(
            day.isoformat() for ordinal in range(date(2026, 1, 1).toordinal(),
                                                date(2027, 1, 1).toordinal())
            if (day := date.fromordinal(ordinal)).weekday() < 5
            and day.isoformat() not in HOLIDAYS
        )
        self.assertEqual(len(days), 251)
        self.assertEqual(len(set(days)), 251)
        self.assertEqual(days, expected)
        self.assertEqual((days[0], days[-1]), ('2026-01-02', '2026-12-31'))

    def test_all_close_times_and_offsets(self):
        for day, value in self.sessions:
            with self.subTest(day=day):
                close = datetime.fromisoformat(value)
                self.assertEqual(close.date().isoformat(), day)
                self.assertEqual((close.hour, close.minute, close.second, close.microsecond),
                                 (13 if day in EARLY else 16, 0, 0, 0))
                # US DST in 2026: March 8 through October 31 inclusive.
                offset = -4 if '2026-03-08' <= day < '2026-11-01' else -5
                self.assertEqual(close.utcoffset(), timedelta(hours=offset))
                self.assertEqual(close.astimezone(timezone.utc).hour,
                                 (13 if day in EARLY else 16) - offset)
                self.assertEqual(value, close.isoformat())

    def test_both_dst_transitions_have_expected_utc_closes(self):
        values = dict(self.sessions)
        for day, expected in (
            ('2026-03-06', '2026-03-06T21:00:00+00:00'),
            ('2026-03-09', '2026-03-09T20:00:00+00:00'),
            ('2026-10-30', '2026-10-30T20:00:00+00:00'),
            ('2026-11-02', '2026-11-02T21:00:00+00:00'),
            ('2026-11-27', '2026-11-27T18:00:00+00:00'),
            ('2026-12-24', '2026-12-24T18:00:00+00:00'),
        ):
            with self.subTest(day=day):
                self.assertEqual(datetime.fromisoformat(values[day]).astimezone(
                    timezone.utc).isoformat(), expected)

    def test_import_is_silent_in_current_and_fresh_process(self):
        self.assertEqual(self.import_out.getvalue(), '')
        self.assertEqual(self.import_err.getvalue(), '')
        result = subprocess.run(
            [sys.executable, '-B', '-c',
             'import runpy, socket; from unittest.mock import patch; '
             'guard = patch.object(socket, "socket", side_effect=AssertionError("Network forbidden")); '
             'guard.start(); runpy.run_path(' + repr(str(SCRIPT)) + ')'],
            capture_output=True, text=True, timeout=10, check=True,
        )
        self.assertEqual((result.stdout, result.stderr), ('', ''))

    def test_cli_is_valid_json_and_repeatable(self):
        outputs = []
        for _ in range(2):
            result = subprocess.run([sys.executable, '-B', str(SCRIPT)],
                                    capture_output=True, text=True, timeout=10, check=True)
            self.assertEqual(result.stderr, '')
            self.assertEqual(json.loads(result.stdout), self.sessions)
            outputs.append(result.stdout)
        self.assertEqual(outputs[0], outputs[1])

    def test_repeated_calls_are_independent_and_stable(self):
        baseline = json.dumps(self.sessions)
        second = self.calendar.build_sessions()
        self.assertEqual(second, self.sessions)
        self.assertIsNot(second, self.sessions)
        second[0][0] = 'mutated'
        second.pop()
        self.assertEqual(json.dumps(self.calendar.build_sessions()), baseline)
        self.assertEqual(json.dumps(self.sessions), baseline)

    def test_daily_contract_accepts_all_sessions_and_stable_fingerprint(self):
        self.assertEqual(len(self.contract.sessions), 251)
        self.assertEqual(self.contract.fingerprint, replace(self.contract).fingerprint)
        completed = self.contract.completed_sessions(
            datetime.fromisoformat('2026-12-31T21:00:00+00:00'))
        self.assertEqual(len(completed), 251)
        self.assertTrue(all(close.tzinfo == timezone.utc for _, close in completed))

    def test_every_close_before_exactly_at_and_after(self):
        for index, (day, close) in enumerate(self.contract.sessions):
            with self.subTest(day=day):
                before = close - timedelta(microseconds=1)
                if index == 0:
                    self.assert_code('no_completed_session', self.contract, before)
                else:
                    completed = self.contract.completed_sessions(before)
                    self.assertEqual(len(completed), index)
                    self.assertEqual(completed[-1][0], self.contract.sessions[index - 1][0])
                for now in (close, close + timedelta(microseconds=1)):
                    completed = self.contract.completed_sessions(now)
                    self.assertEqual(len(completed), index + 1)
                    self.assertEqual(completed[-1], (day, close.astimezone(timezone.utc)))

    def test_calendar_interval_uses_market_date_and_inclusive_last_day(self):
        for now in ('2026-01-01T04:59:59+00:00', '2027-01-01T05:00:00+00:00'):
            with self.subTest(now=now):
                self.assert_code('calendar_expired', self.contract, datetime.fromisoformat(now))
        self.assert_code('no_completed_session', self.contract,
                         datetime.fromisoformat('2026-01-01T05:00:00+00:00'))
        self.assertEqual(len(self.contract.completed_sessions(
            datetime.fromisoformat('2027-01-01T04:59:59+00:00'))), 251)

    def test_freshness_minimum_sessions_and_naive_time_boundaries(self):
        close = self.contract.sessions[0][1]
        short = replace(self.contract, max_age_seconds=60)
        self.assertEqual(len(short.completed_sessions(close + timedelta(seconds=60))), 1)
        self.assert_code('stale_calendar', short,
                         close + timedelta(seconds=60, microseconds=1))
        two = replace(self.contract, minimum_sessions=2)
        self.assert_code('no_completed_session', two, close)
        self.assertEqual(len(two.completed_sessions(self.contract.sessions[1][1])), 2)
        self.assert_code('invalid_time', self.contract, datetime(2026, 1, 2, 16))


if __name__ == '__main__':
    unittest.main()
