"""Real SQLite admission with synthetic limits; no profile/config rollout."""
from contextlib import closing
import json
import math
import multiprocessing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from ingestion.api_profiles import QuotaLimit
from ingestion.api_state import StateRejected, StateStore
from test_api_state import profile


class SyntheticProfile:
    def __init__(self, limits, **changes):
        self.base = profile(**changes)
        self.quota_limits = tuple(limits)

    def __getattr__(self, name):
        return getattr(self.base, name)


def race_worker(path, barrier, results, number, now):
    limits = (QuotaLimit(3, 60), QuotaLimit(5, 180))
    p = SyntheticProfile(limits if number % 2 else limits[::-1],
                         source_id=f'source-{number % 2}', max_concurrency=8)
    store = StateStore(path, existing_only=True)
    try:
        barrier.wait(timeout=10)
        try:
            lease = store.acquire(p, f'race-{now}-{number}', now)
        except StateRejected as error:
            results.put((error.code, error.retry_after_seconds))
        else:
            store.release(lease, now)
            results.put(('accepted', 0))
    finally:
        store.close()


class MultiWindowStateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.counter = 0

    def new_store(self, **options):
        self.counter += 1
        store = StateStore(Path(self.tmp.name) / f'state-{self.counter}.sqlite', **options)
        self.addCleanup(store.close)
        return store

    def query(self, store, sql, parameters=()):
        with closing(sqlite3.connect(store.path)) as db:
            return db.execute(sql, parameters).fetchall()

    def snapshot(self, store):
        with closing(sqlite3.connect(store.path)) as db:
            return tuple(db.iterdump())

    def spend(self, store, p, request, now, cost):
        store.release(store.acquire(p, request, now, cost=cost), now)

    def reject(self, store, p, now, cost, delay, code='quota'):
        before = self.snapshot(store)
        with self.assertRaises(StateRejected) as caught:
            store.acquire(p, 'rejected', now, cost=cost)
        self.assertEqual(caught.exception.code, code)
        self.assertAlmostEqual(caught.exception.retry_after_seconds, delay)
        self.assertEqual(self.snapshot(store), before)

    def test_short_long_and_both_block_with_maximum_delay_in_either_order(self):
        cases = (
            ('short-only', (QuotaLimit(3, 10), QuotaLimit(10, 60)),
             ((100, 3),), 105, 1, 5, 110),
            ('long-only', (QuotaLimit(4, 10), QuotaLimit(6, 60)),
             ((100, 3), (110, 3)), 121, 1, 39, 160),
            ('both-longer-long', (QuotaLimit(4, 10), QuotaLimit(6, 60)),
             ((100, 2), (110, 2), (115, 2)), 116, 1, 44, 160),
            ('both-longer-short', (QuotaLimit(4, 10), QuotaLimit(5, 60)),
             ((100, 2), (155, 3)), 159, 2, 6, 165),
        )
        for name, limits, attempts, now, cost, delay, accepted_at in cases:
            for ordered in (limits, limits[::-1]):
                with self.subTest(case=name, limits=ordered):
                    store, p = self.new_store(), SyntheticProfile(ordered)
                    for number, (at, spent) in enumerate(attempts):
                        self.spend(store, p, f'first-{number}', at, spent)
                    self.reject(store, p, now, cost, delay)
                    self.reject(store, p, accepted_at - 0.001, cost, 0.001)
                    self.spend(store, p, 'fits', accepted_at, cost)
                    self.assertEqual(self.query(store, 'SELECT COUNT(*) FROM jobs'),
                                     [(len(attempts) + 1,)])

    def test_equal_timestamps_require_multiple_costs_to_expire(self):
        limits = (QuotaLimit(10, 60), QuotaLimit(12, 120))
        for ordered in (limits, limits[::-1]):
            store, p = self.new_store(), SyntheticProfile(ordered)
            for number, (now, cost) in enumerate(((100, 2), (100, 3), (110, 5))):
                self.spend(store, p, f'first-{number}', now, cost)
            self.reject(store, p, 150, 6, 70)
            self.reject(store, p, 170, 6, 50)
            self.reject(store, p, 219.999, 6, 0.001)
            self.spend(store, p, 'fits', 220, 6)
            self.assertEqual(self.query(store, 'SELECT attempted_at,cost FROM attempts ORDER BY rowid'),
                             [(110, 5), (220, 6)])

    def test_fractional_boundaries_and_longest_delay(self):
        limits = (QuotaLimit(3, 0.5), QuotaLimit(5, 1))
        for ordered in (limits, limits[::-1]):
            store, p = self.new_store(), SyntheticProfile(ordered)
            for number, (now, cost) in enumerate(((100, 2), (100.125, 1), (100.5, 2))):
                self.spend(store, p, f'first-{number}', now, cost)
            for now, delay in ((100.624, 0.376), (100.625, 0.375), (100.999, 0.001)):
                self.reject(store, p, now, 1, delay)
            self.spend(store, p, 'fits', 101, 1)
            self.assertEqual(self.query(store, 'SELECT attempted_at,cost FROM attempts ORDER BY rowid'),
                             [(100.125, 1), (100.5, 2), (101, 1)])

    def test_zero_delay_is_rejected_even_when_other_window_allows(self):
        limits = (QuotaLimit(1, 0.3), QuotaLimit(2, 1))
        for ordered in (limits, limits[::-1]):
            store, p = self.new_store(), SyntheticProfile(ordered)
            first = math.nextafter(0.7, 1.0)
            self.assertGreater(first, 1 - 0.3)
            self.assertEqual(first + 0.3 - 1, 0)
            self.spend(store, p, 'first', first, 1)
            self.reject(store, p, 1, 1, 0)
            after = math.nextafter(1, math.inf)
            self.spend(store, p, 'fits', after, 1)
            self.assertEqual(self.query(store, 'SELECT attempted_at,cost FROM attempts ORDER BY rowid'),
                             [(first, 1), (after, 1)])

    def test_longest_retention_boundary_preserves_other_scope(self):
        limits = (QuotaLimit(4, 10), QuotaLimit(8, 60))
        for ordered in (limits, limits[::-1]):
            store, p = self.new_store(), SyntheticProfile(ordered)
            other = SyntheticProfile(ordered, source_id='other', quota_scope='other')
            self.spend(store, other, 'independent', 90, 2)
            self.spend(store, p, 'first', 100, 2)
            self.spend(store, p, 'short-expired', 110, 2)
            self.spend(store, p, 'long-not-expired', 159.999, 2)
            self.assertEqual(self.query(store, 'SELECT attempted_at,cost FROM attempts WHERE scope=?',
                                        (p.quota_scope,)), [(100, 2), (110, 2), (159.999, 2)])
            self.spend(store, p, 'long-expired', 160, 2)
            self.assertEqual(self.query(store, 'SELECT attempted_at,cost FROM attempts WHERE scope=?',
                                        (p.quota_scope,)), [(110, 2), (159.999, 2), (160, 2)])
            self.assertEqual(self.query(store, 'SELECT attempted_at,cost FROM attempts WHERE scope=?',
                                        (other.quota_scope,)), [(90, 2)])

    def test_each_window_rejection_rolls_back_pruning_clock_and_expired_job(self):
        for short, long, running_cost, requested, delay in ((5, 6, 2, 2, 50), (4, 10, 1, 4, 5)):
            limits = (QuotaLimit(short, 10), QuotaLimit(long, 60))
            for ordered in (limits, limits[::-1]):
                store, p = self.new_store(), SyntheticProfile(ordered)
                self.spend(store, p, 'prunable', 100, 1)
                self.spend(store, p, 'recent', 150, 3)
                store.acquire(p, 'expired-job', 155, lease_seconds=5, cost=running_cost)
                self.reject(store, p, 160, requested, delay)
                self.assertEqual(self.query(store, "SELECT status FROM jobs WHERE request_id='expired-job'"),
                                 [('running',)])
                self.assertEqual(self.query(store, 'SELECT COUNT(*) FROM attempts'), [(3,)])
                self.spend(store, p, 'later', 220, requested)
                self.assertEqual(self.query(store, "SELECT status FROM jobs WHERE request_id='expired-job'"),
                                 [('abandoned',)])
                self.assertEqual(self.query(store, 'SELECT attempted_at,cost FROM attempts'), [(220, requested)])

    def test_one_charge_per_admission_and_no_refund_on_completion(self):
        store = self.new_store()
        p = SyntheticProfile((QuotaLimit(6, 10), QuotaLimit(10, 60)))
        cancelled = store.acquire(p, 'cancelled', 100, cost=2)
        store.release(cancelled, 100)
        store.release(cancelled, 100)
        succeeded = store.acquire(p, 'success', 100, cost=1)
        store.success(succeeded, 100, rows=1)
        failed = store.acquire(p, 'failure', 100, cost=3)
        store.failure(failed, 100, category='permission')
        self.reject(store, p, 101, 1, 9)
        self.spend(store, p, 'long-full', 110, 4)
        self.reject(store, p, 111, 1, 49)
        self.assertEqual(self.query(store, 'SELECT cost FROM attempts ORDER BY rowid'), [(2,), (1,), (3,), (4,)])
        self.assertEqual(self.query(store, 'SELECT COUNT(*) FROM jobs'), [(4,)])

    def test_reverse_order_shares_policy_and_consumption_across_sources(self):
        store = self.new_store()
        limits = (QuotaLimit(4, 10), QuotaLimit(6, 60))
        a = SyntheticProfile(limits)
        b = SyntheticProfile(limits[::-1], source_id='second', credential_env='SECOND_KEY')
        self.spend(store, a, 'a', 100, 2)
        self.spend(store, b, 'b', 100, 2)
        for p in (a, b):
            self.reject(store, p, 101, 1, 9)
        self.spend(store, b, 'b-later', 110, 2)
        for p in (a, b):
            self.reject(store, p, 121, 1, 39)
        policies = self.query(store, 'SELECT policy FROM scopes')
        self.assertEqual(len(policies), 1)
        self.assertEqual(json.loads(policies[0][0]),
                         {'version': 2, 'limits': [[4, 10.0], [6, 60.0]], 'max_concurrency': 4})

    def test_reopen_preserves_both_window_constraints_and_policy(self):
        store = self.new_store()
        limits = (QuotaLimit(4, 10), QuotaLimit(6, 60))
        p = SyntheticProfile(limits)
        self.spend(store, p, 'first', 100, 4)
        before = self.snapshot(store)
        store.close()
        reopened = StateStore(store.path, existing_only=True)
        self.addCleanup(reopened.close)
        self.assertEqual(self.snapshot(reopened), before)
        reverse = SyntheticProfile(limits[::-1])
        self.reject(reopened, reverse, 101, 1, 9)
        self.spend(reopened, reverse, 'long-full', 110, 2)
        self.reject(reopened, p, 121, 1, 39)
        self.spend(reopened, p, 'old-long-expired', 160, 4)
        self.assertEqual(self.query(reopened, 'SELECT SUM(cost) FROM attempts'), [(6,)])

    def test_policy_changes_reject_without_mutation_or_reset(self):
        store = self.new_store()
        limits = (QuotaLimit(4, 10), QuotaLimit(6, 60))
        p = SyntheticProfile(limits)
        self.spend(store, p, 'first', 100, 3)
        for changed in (
                (QuotaLimit(5, 10), limits[1]), (QuotaLimit(4, 11), limits[1]),
                (limits[0], QuotaLimit(7, 60)), (limits[0], QuotaLimit(6, 61)),
                (limits[0],), (*limits, QuotaLimit(20, 120))):
            self.reject(store, SyntheticProfile(changed), 170, 1, 0, 'quota_policy_mismatch')
        self.reject(store, SyntheticProfile(limits, max_concurrency=3), 170, 1, 0, 'quota_policy_mismatch')
        self.reject(store, p, 101, 2, 9)

    def test_single_and_multi_window_scopes_never_automatically_migrate(self):
        limits = (QuotaLimit(4, 10), QuotaLimit(6, 60))
        for reverse in (False, True):
            store = self.new_store()
            single = profile(max_attempts_per_window=6)
            multi = SyntheticProfile(limits)
            original, changed = (multi, single) if reverse else (single, multi)
            self.spend(store, original, 'first', 100, 3)
            self.reject(store, changed, 170, 1, 0, 'quota_policy_mismatch')
            self.reject(store, original, 101, 2 if reverse else 4, 9 if reverse else 59)

    def test_cost_over_any_total_limit_stops_before_transaction(self):
        for limits in ((QuotaLimit(5, 10), QuotaLimit(8, 60)),
                       (QuotaLimit(8, 10), QuotaLimit(5, 60))):
            for ordered in (limits, limits[::-1]):
                store, p = self.new_store(), SyntheticProfile(ordered)
                self.spend(store, p, 'first', 100, 1)
                for cost in (6, 10**100):
                    with patch.object(store, '_transaction') as transaction, \
                            patch.object(store, '_connect') as connect:
                        self.reject(store, p, 170, cost, 0, 'cost_exceeds_quota')
                        transaction.assert_not_called()
                        connect.assert_not_called()

    def test_synchronized_processes_never_overbook_either_window(self):
        store = self.new_store(max_workers=8)
        context = multiprocessing.get_context('spawn')
        for now, accepted, delay, total in ((100, 3, 60, 3), (160, 2, 120, 5), (220, 0, 60, 5)):
            barrier = context.Barrier(6)
            results = context.Queue()
            workers = [context.Process(target=race_worker,
                                       args=(store.path, barrier, results, number, now))
                       for number in range(6)]
            before = self.snapshot(store)
            try:
                for worker in workers:
                    worker.start()
                outcomes = [results.get(timeout=15) for _ in workers]
                for worker in workers:
                    worker.join(timeout=5)
                    self.assertEqual(worker.exitcode, 0)
                self.assertEqual(outcomes.count(('accepted', 0)), accepted)
                self.assertEqual(outcomes.count(('quota', delay)), 6 - accepted)
            finally:
                for worker in workers:
                    if worker.is_alive():
                        worker.terminate()
                        worker.join(timeout=5)
                results.close()
                results.join_thread()
            self.assertEqual(self.query(store, 'SELECT COUNT(*),SUM(cost) FROM attempts'), [(total, total)])
            self.assertEqual(self.query(store, 'SELECT COUNT(*) FROM jobs'), [(total,)])
            self.assertEqual(self.query(store, "SELECT COUNT(*) FROM jobs WHERE status='running'"), [(0,)])
            for limit in (QuotaLimit(3, 60), QuotaLimit(5, 180)):
                used = self.query(store, 'SELECT COALESCE(SUM(cost),0) FROM attempts WHERE attempted_at>?',
                                  (now - limit.window_seconds,))[0][0]
                self.assertLessEqual(used, limit.max_credits)
            if not accepted:
                self.assertEqual(self.snapshot(store), before)
