"""Single-window credit accounting with synthetic clocks and private SQLite files."""
from contextlib import closing
from dataclasses import replace
from pathlib import Path
import math
import sqlite3
import tempfile
import unittest
from unittest.mock import PropertyMock, patch

from ingestion.api_profiles import QuotaLimit, SourceProfile
from ingestion.api_state import StateRejected, StateStore
from test_api_state import profile


class StateQuotaAccountingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'state.sqlite'
        self.store = StateStore(self.path)
        self.addCleanup(self.store.close)

    def query(self, sql, parameters=()):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute(sql, parameters).fetchall()

    def snapshot(self):
        with closing(sqlite3.connect(self.path)) as db:
            return tuple(db.iterdump())

    def spend(self, p, request, now, cost):
        lease = self.store.acquire(p, request, now, cost=cost)
        self.store.release(lease, now)

    def reject_unchanged(self, p, request, now, cost, code='quota', delay=None):
        before = self.snapshot()
        with self.assertRaises(StateRejected) as caught:
            self.store.acquire(p, request, now, cost=cost)
        self.assertEqual(caught.exception.code, code)
        if delay is not None:
            self.assertAlmostEqual(caught.exception.retry_after_seconds, delay)
        self.assertEqual(self.snapshot(), before)

    def test_mixed_costs_sum_to_limit_with_one_attempt_and_job_per_admission(self):
        p = profile(max_attempts_per_window=10)
        for number, cost in enumerate((2, 3, 5), 1):
            self.spend(p, f'accepted-{number}', 100, cost)
            self.assertEqual(self.query('SELECT cost FROM attempts ORDER BY rowid'),
                             [(c,) for c in (2, 3, 5)[:number]])
            self.assertEqual(self.query('SELECT COUNT(*) FROM jobs'), [(number,)])
        for number in range(3):
            self.reject_unchanged(p, f'rejected-{number}', 101, 1, delay=59)
        self.assertEqual(self.query('SELECT SUM(cost) FROM attempts'), [(10,)])

    def test_staggered_attempts_expire_exactly_at_each_window_boundary(self):
        p = profile(max_attempts_per_window=7)
        self.spend(p, 'first', 100, 3)
        self.spend(p, 'second', 110, 4)
        self.reject_unchanged(p, 'before-first-expiry', 159.999, 3, delay=0.001)
        self.spend(p, 'at-first-expiry', 160, 3)
        self.assertEqual(self.query('SELECT attempted_at,cost FROM attempts ORDER BY rowid'),
                         [(110, 4), (160, 3)])
        self.reject_unchanged(p, 'before-second-expiry', 169.999, 4, delay=0.001)
        self.spend(p, 'at-second-expiry', 170, 4)
        self.assertEqual(self.query('SELECT attempted_at,cost FROM attempts ORDER BY rowid'),
                         [(160, 3), (170, 4)])

    def test_fractional_window_preserves_strict_start_boundary(self):
        p = profile(max_attempts_per_window=3, quota_window_seconds=0.5)
        self.spend(p, 'first', 100, 3)
        self.reject_unchanged(p, 'before-expiry', 100.499, 1, delay=0.001)
        self.spend(p, 'at-expiry', 100.5, 3)
        self.assertEqual(self.query('SELECT attempted_at,cost FROM attempts'), [(100.5, 3)])

    def test_quota_rejects_even_when_float_rounding_makes_delay_zero(self):
        p = profile(max_attempts_per_window=1, quota_window_seconds=0.3)
        attempted_at = math.nextafter(0.7, 1.0)
        now = 1.0
        # Still active by the SQL boundary, though the computed delay rounds to zero.
        self.assertGreater(attempted_at, now - p.quota_window_seconds)
        self.assertEqual(attempted_at + p.quota_window_seconds - now, 0)
        self.spend(p, 'first', attempted_at, 1)
        self.reject_unchanged(p, 'zero-delay', now, 1, delay=0)
        self.assertEqual(self.query('SELECT COUNT(*) FROM jobs'), [(1,)])
        after_expiry = math.nextafter(now, math.inf)
        self.spend(p, 'after-expiry', after_expiry, 1)
        self.assertEqual(self.query('SELECT attempted_at,cost FROM attempts'), [(after_expiry, 1)])
        self.assertEqual(self.query('SELECT COUNT(*) FROM jobs'), [(2,)])

    def test_shared_scope_sums_sources_and_other_scope_is_not_counted_or_pruned(self):
        a = profile(max_attempts_per_window=5)
        b = replace(a, source_id='source-b', credential_env='SOURCE_B_KEY')
        other = replace(a, source_id='other', quota_scope='other-scope')
        self.spend(other, 'other-old', 100, 5)
        self.spend(a, 'a', 100, 2)
        self.spend(b, 'b', 100, 3)
        self.reject_unchanged(a, 'shared-full', 101, 1, delay=59)
        self.spend(a, 'shared-expiry', 160, 5)
        self.assertEqual(self.query('SELECT scope,attempted_at,cost FROM attempts ORDER BY rowid'),
                         [('other-scope', 100, 5), (a.quota_scope, 160, 5)])
        self.spend(other, 'other-expiry', 160, 5)
        self.assertEqual(self.query('SELECT scope,attempted_at,cost FROM attempts ORDER BY rowid'),
                         [(a.quota_scope, 160, 5), ('other-scope', 160, 5)])

    def test_quota_limits_controls_credits_window_retention_and_policy(self):
        # Deliberately different legacy fields: only the test property is replaced.
        p = profile(max_attempts_per_window=1000, quota_window_seconds=3600)
        with patch.object(SourceProfile, 'quota_limits', new_callable=PropertyMock,
                          return_value=(QuotaLimit(7, 0.5),)):
            self.spend(p, 'first', 100, 7)
            self.assertEqual(self.query('SELECT policy FROM scopes'), [('[7,0.5,4]',)])
            self.reject_unchanged(p, 'blocked', 100.25, 1, delay=0.25)
            self.spend(p, 'expiry', 100.5, 7)
            self.assertEqual(self.query('SELECT attempted_at,cost FROM attempts'), [(100.5, 7)])

    def test_rejection_rolls_back_pruning_clock_and_expired_job_recovery(self):
        p = profile(max_attempts_per_window=4)
        self.spend(p, 'old', 100, 1)
        self.store.acquire(p, 'expires', 150, lease_seconds=10, cost=3)
        # At 160, one attempt is prunable and the running job is expired, but
        # the remaining cost leaves only one credit. All changes roll back.
        self.reject_unchanged(p, 'no-room', 160, 2, delay=50)
        self.assertEqual(self.query('SELECT attempted_at,cost FROM attempts ORDER BY rowid'),
                         [(100, 1), (150, 3)])
        self.assertEqual(self.query("SELECT status FROM jobs WHERE request_id='expires'"),
                         [('running',)])
        self.spend(p, 'after-expiry', 210, 4)
        self.assertEqual(self.query('SELECT attempted_at,cost FROM attempts'), [(210, 4)])
        self.assertNotEqual(self.query("SELECT status FROM jobs WHERE request_id='expires'"),
                            [('running',)])

    def test_cost_larger_than_limit_creates_no_scope_attempt_or_job(self):
        p = profile(max_attempts_per_window=3)
        for number in range(2):
            self.reject_unchanged(p, f'too-large-{number}', 100, 4,
                                  code='cost_exceeds_quota', delay=0)
        for table in ('scopes', 'attempts', 'jobs'):
            self.assertEqual(self.query('SELECT COUNT(*) FROM ' + table), [(0,)])
        self.spend(p, 'fits', 100, 3)

    def test_completion_failure_and_cancellation_never_refund_or_charge_again(self):
        p = profile(max_attempts_per_window=10)
        cancelled = self.store.acquire(p, 'cancelled', 100, cost=2)
        self.store.release(cancelled, 100)
        self.store.release(cancelled, 100)
        succeeded = self.store.acquire(p, 'succeeded', 100, cost=3)
        self.store.success(succeeded, 100, rows=1)
        failed = self.store.acquire(p, 'failed', 100, cost=5)
        self.store.failure(failed, 100, category='permission')
        self.assertEqual(self.query('SELECT cost FROM attempts ORDER BY rowid'), [(2,), (3,), (5,)])
        self.assertEqual(self.query('SELECT COUNT(*) FROM jobs'), [(3,)])
        self.reject_unchanged(p, 'still-full', 101, 1, delay=59)

    def test_retry_after_waits_until_sufficient_credit_expires(self):
        # Exact for the recorded attempts, provided no new cost is admitted.
        p = profile(max_attempts_per_window=10)
        self.spend(p, 'small', 100, 1)
        self.spend(p, 'large', 110, 9)
        self.reject_unchanged(p, 'first-retry', 159, 5, delay=11)
        self.reject_unchanged(p, 'still-too-expensive', 160, 5, delay=10)
        self.reject_unchanged(p, 'just-before', 169.999, 5, delay=0.001)
        self.spend(p, 'finally-fits', 170, 5)

    def test_retry_after_accumulates_multiple_expirations(self):
        p = profile(max_attempts_per_window=10)
        for request, now, cost in (('a', 100, 2), ('b', 110, 3), ('c', 120, 5)):
            self.spend(p, request, now, cost)
        for now, delay in ((150, 30), (160, 20), (170, 10), (179.999, 0.001)):
            with self.subTest(now=now):
                self.reject_unchanged(p, 'needs-six', now, 6, delay=delay)
        self.spend(p, 'fits-six', 180, 6)
        self.assertEqual(self.query('SELECT attempted_at,cost FROM attempts'), [(180, 6)])

    def test_retry_after_handles_equal_timestamps_and_exact_credit_threshold(self):
        p = profile(max_attempts_per_window=10)
        self.spend(p, 'same-a', 100, 2)
        self.spend(p, 'same-b', 100, 3)
        self.spend(p, 'later', 110, 5)
        self.reject_unchanged(p, 'needs-five', 150, 5, delay=10)
        self.reject_unchanged(p, 'needs-six', 150, 6, delay=20)
        self.reject_unchanged(p, 'just-before', 159.999, 5, delay=0.001)
        self.spend(p, 'exactly-five-free', 160, 5)
        self.assertEqual(self.query('SELECT attempted_at,cost FROM attempts ORDER BY rowid'),
                         [(110, 5), (160, 5)])

    def test_retry_after_stops_when_oldest_attempt_alone_is_sufficient(self):
        p = profile(max_attempts_per_window=10)
        self.spend(p, 'oldest', 100, 6)
        self.spend(p, 'later', 110, 4)
        for cost in (5, 6):
            with self.subTest(cost=cost):
                self.reject_unchanged(p, 'oldest-enough', 150, cost, delay=10)
        self.spend(p, 'full-cost-fits', 160, 6)
        self.assertEqual(self.query('SELECT attempted_at,cost FROM attempts ORDER BY rowid'),
                         [(110, 4), (160, 6)])

    def test_retry_after_accumulates_costs_with_fractional_times_and_window(self):
        p = profile(max_attempts_per_window=7, quota_window_seconds=0.5)
        self.spend(p, 'small', 100, 2)
        self.spend(p, 'large', 100.125, 5)
        self.reject_unchanged(p, 'needs-three', 100.25, 3, delay=0.375)
        self.reject_unchanged(p, 'first-expired', 100.5, 3, delay=0.125)
        self.reject_unchanged(p, 'just-before', 100.624, 3, delay=0.001)
        self.spend(p, 'fits', 100.625, 3)

    def test_retry_after_shares_scope_across_sources_and_ignores_other_scope(self):
        p = profile(max_attempts_per_window=10)
        other = replace(p, source_id='other', quota_scope='other-scope')
        shared = replace(p, source_id='shared', credential_env='SHARED_KEY')
        self.spend(p, 'small', 100, 1)
        self.spend(shared, 'large', 110, 9)
        self.spend(other, 'independent', 120, 10)
        other_before = self.query('SELECT * FROM attempts WHERE scope=?', (other.quota_scope,))
        self.reject_unchanged(p, 'needs-five', 159, 5, delay=11)
        self.reject_unchanged(shared, 'same-delay', 159, 5, delay=11)
        self.spend(p, 'fits', 170, 5)
        self.assertEqual(self.query('SELECT * FROM attempts WHERE scope=?', (other.quota_scope,)),
                         other_before)

    def test_retry_after_survives_reopening_without_resetting_cost_or_policy(self):
        p = profile(max_attempts_per_window=10)
        self.spend(p, 'small', 100, 1)
        self.spend(p, 'large', 110, 9)
        before = self.snapshot()
        self.store.close()
        self.store = StateStore(self.path, existing_only=True)
        self.addCleanup(self.store.close)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.query('SELECT policy FROM scopes'), [('[10,60.0,4]',)])
        self.reject_unchanged(p, 'reopened-reject', 159, 5, delay=11)
        self.spend(p, 'reopened-accept', 170, 5)

    def test_oversized_cost_has_zero_delay_without_mutating_populated_state(self):
        p = profile(max_attempts_per_window=10)
        self.spend(p, 'small', 100, 1)
        self.spend(p, 'large', 110, 9)
        for now in (159, 160, 170, 1000):
            with self.subTest(now=now):
                self.reject_unchanged(p, 'too-large', now, 11,
                                      code='cost_exceeds_quota', delay=0)

    def test_oversized_cost_stops_before_transaction_or_connection(self):
        p = profile(max_attempts_per_window=10)
        for populated in (False, True):
            if populated:
                # Would require both pruning and expired-job recovery at 170.
                self.store.acquire(p, 'expires', 100, lease_seconds=10, cost=10)
            for cost in (11, 10**100):
                with self.subTest(populated=populated, cost=cost), \
                        patch.object(self.store, '_transaction') as transaction, \
                        patch.object(self.store, '_connect') as connect:
                    self.reject_unchanged(p, 'oversized', 170, cost,
                                          code='cost_exceeds_quota', delay=0)
                    transaction.assert_not_called()
                    connect.assert_not_called()

    def test_exact_limit_uses_quota_delay_and_fits_at_expiry(self):
        p = profile(max_attempts_per_window=10)
        self.spend(p, 'full-limit', 100, 10)
        self.reject_unchanged(p, 'limit-plus-one', 101, 11,
                              code='cost_exceeds_quota', delay=0)
        self.reject_unchanged(p, 'full-limit-blocked', 101, 10, delay=59)
        self.reject_unchanged(p, 'full-limit-before-expiry', 159.999, 10, delay=0.001)
        self.spend(p, 'full-limit-at-expiry', 160, 10)
        self.assertEqual(self.query('SELECT attempted_at,cost FROM attempts'), [(160, 10)])
        self.assertEqual(self.query('SELECT COUNT(*) FROM jobs'), [(2,)])

    def test_invalid_cost_and_unsupported_policy_take_precedence_before_transaction(self):
        p = profile()
        for limits in ((), (QuotaLimit(8, 60), QuotaLimit(800, 86400))):
            for cost, code in ((0, 'invalid_cost'), (True, 'invalid_cost'),
                               (1.0, 'invalid_cost'),
                               (10**100, 'cost_exceeds_quota' if limits else 'unsupported_quota_policy')):
                with self.subTest(limits=limits, cost=cost), \
                        patch.object(SourceProfile, 'quota_limits', new_callable=PropertyMock,
                                     return_value=limits), \
                        patch.object(self.store, '_transaction') as transaction, \
                        patch.object(self.store, '_connect') as connect:
                    self.reject_unchanged(p, 'precedence', 100, cost, code=code, delay=0)
                    transaction.assert_not_called()
                    connect.assert_not_called()

    def test_invalid_costs_leave_existing_state_unchanged(self):
        p = profile()
        self.spend(p, 'first', 100, 2)
        for cost in (True, False, 0, -1, 1.0, None, '1'):
            with self.subTest(cost=cost):
                self.reject_unchanged(p, 'invalid', 101, cost, code='invalid_cost')
