"""Legacy policy encoding, retained quota and rejection before transactions."""
from contextlib import closing
from dataclasses import replace
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import PropertyMock, patch

from ingestion.api_profiles import QuotaLimit, SourceProfile
from ingestion.api_state import StateRejected, StateStore, _quota_policy
from test_api_state import profile


class StateQuotaInterfaceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'state.sqlite'
        self.store = StateStore(self.path)
        self.addCleanup(self.store.close)

    def snapshot(self):
        with closing(sqlite3.connect(self.path)) as db:
            return tuple(db.iterdump())

    def test_legacy_policy_encoding_is_byte_identical(self):
        for number, credits, seconds, expected in (
                (1, 10, 60, b'[10,60.0,4]'),
                (2, 8, 60.0, b'[8,60.0,4]'),
                (3, 800, 86400, b'[800,86400.0,4]')):
            p = profile(quota_scope=f'scope-{number}', max_attempts_per_window=credits,
                        quota_window_seconds=seconds)
            lease = self.store.acquire(p, f'request-{number}', 100)
            self.store.release(lease, 100)
            with closing(sqlite3.connect(self.path)) as db:
                saved = db.execute('SELECT policy FROM scopes WHERE scope=?',
                                   (p.quota_scope,)).fetchone()[0]
            self.assertEqual(saved.encode('utf-8'), expected)

    def test_acquire_calls_helper_and_persists_legacy_policy_bytes(self):
        for number, credits, seconds, concurrency, expected in (
                (1, 10, 60, 4, b'[10,60.0,4]'),
                (2, 10, 60.0, 4, b'[10,60.0,4]'),
                (3, 8, 60, 1, b'[8,60.0,1]'),
                (4, 800, 86400, 3, b'[800,86400.0,3]'),
                (5, 1, 0.5, 2, b'[1,0.5,2]')):
            p = profile(quota_scope=f'helper-{number}', max_attempts_per_window=credits,
                        quota_window_seconds=seconds, max_concurrency=concurrency)
            with self.subTest(credits=credits, seconds=seconds, concurrency=concurrency), \
                    patch('ingestion.api_state._quota_policy', wraps=_quota_policy) as helper:
                lease = self.store.acquire(p, f'helper-{number}', 100, cost=credits)
                helper.assert_called_once_with(p.quota_limits, concurrency)
                self.store.release(lease, 100)
                with closing(sqlite3.connect(self.path)) as db:
                    saved = db.execute('SELECT policy FROM scopes WHERE scope=?',
                                       (p.quota_scope,)).fetchone()[0]
                    self.assertEqual(saved.encode('utf-8'), expected)
                    self.assertEqual(db.execute('SELECT cost FROM attempts WHERE scope=?',
                                                (p.quota_scope,)).fetchall(), [(credits,)])

    def test_unsupported_and_oversized_inputs_stop_before_helper_and_transaction(self):
        p = profile()
        self.store.release(self.store.acquire(p, 'first', 100), 100)
        before = self.snapshot()
        for limits, cost, code in (
                ((), 1, 'unsupported_quota_policy'),
                ((QuotaLimit(8, 60), QuotaLimit(800, 86400)), 9, 'cost_exceeds_quota'),
                ((QuotaLimit(10, 60),), 11, 'cost_exceeds_quota')):
            with self.subTest(limits=limits, cost=cost), \
                    patch.object(SourceProfile, 'quota_limits', new_callable=PropertyMock,
                                 return_value=limits), \
                    patch('ingestion.api_state._quota_policy') as helper, \
                    patch.object(self.store, '_transaction') as transaction, \
                    patch.object(self.store, '_connect') as connect:
                with self.assertRaises(StateRejected) as caught:
                    self.store.acquire(p, 'rejected', 170, cost=cost)
                self.assertEqual(caught.exception.code, code)
                self.assertEqual(caught.exception.retry_after_seconds, 0)
                helper.assert_not_called()
                transaction.assert_not_called()
                connect.assert_not_called()
            self.assertEqual(self.snapshot(), before)

    def test_reopening_preserves_spent_quota_and_policy(self):
        p = profile(max_attempts_per_window=2)
        self.store.release(self.store.acquire(p, 'first', 100, cost=2), 100)
        before = self.snapshot()
        self.store.close()
        reopened = StateStore(self.path, existing_only=True)
        self.addCleanup(reopened.close)
        self.assertEqual(self.snapshot(), before)
        with self.assertRaises(StateRejected) as caught:
            reopened.acquire(p, 'second', 101)
        self.assertEqual(caught.exception.code, 'quota')
        self.assertEqual(caught.exception.retry_after_seconds, 59)
        self.assertEqual(self.snapshot(), before)
        reopened.release(reopened.acquire(p, 'after-window', 160), 160)

    def test_changed_existing_policy_still_rejected_without_mutation(self):
        p = profile()
        self.store.release(self.store.acquire(p, 'first', 100), 100)
        before = self.snapshot()
        for changes in ({'max_attempts_per_window': 11}, {'quota_window_seconds': 61},
                        {'max_concurrency': 3}):
            with self.subTest(changes=changes), self.assertRaises(StateRejected) as caught:
                self.store.acquire(replace(p, **changes), 'changed', 101)
            self.assertEqual(caught.exception.code, 'quota_policy_mismatch')
            self.assertEqual(self.snapshot(), before)

    def test_empty_windows_stop_before_transaction_or_connection(self):
        p = profile()
        # Existing attempts/jobs must also remain untouched on rejection.
        self.store.release(self.store.acquire(p, 'first', 100), 100)
        before = self.snapshot()
        for limits in ((), []):
            with self.subTest(limits=limits), \
                    patch.object(SourceProfile, 'quota_limits', new_callable=PropertyMock,
                                 return_value=limits), \
                    patch.object(self.store, '_transaction') as transaction, \
                    patch.object(self.store, '_connect') as connect:
                with self.assertRaises(StateRejected) as caught:
                    self.store.acquire(p, 'unsupported', 101)
                self.assertEqual(caught.exception.code, 'unsupported_quota_policy')
                transaction.assert_not_called()
                connect.assert_not_called()
            self.assertEqual(self.snapshot(), before)

    def test_unsupported_policy_creates_no_scope_attempt_or_job_on_empty_state(self):
        p = profile()
        before = self.snapshot()
        for limits in ((), []):
            with self.subTest(limits=limits), patch.object(SourceProfile, 'quota_limits',
                    new_callable=PropertyMock, return_value=limits):
                with self.assertRaises(StateRejected) as caught:
                    self.store.acquire(p, 'unsupported', 100)
                self.assertEqual(caught.exception.code, 'unsupported_quota_policy')
            self.assertEqual(self.snapshot(), before)
        with closing(sqlite3.connect(self.path)) as db:
            for table in ('scopes', 'attempts', 'jobs'):
                self.assertEqual(db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0], 0)
