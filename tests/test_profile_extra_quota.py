"""Real profile validation and admission through real SQLite; no JSON loading."""
from contextlib import closing
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
import sqlite3
import tempfile
import unittest

from ingestion.api_profiles import ProfileError, QuotaLimit
from ingestion.api_state import StateRejected, StateStore
from test_api_profiles import profile


class DerivedLimit(QuotaLimit):
    pass


class TupleSubclass(tuple):
    pass


class ExtraQuotaProfileTests(unittest.TestCase):
    def test_invalid_container_and_element_types_are_rejected(self):
        for extras in (None, [], {}, '', 1, True, TupleSubclass(),
                       (None,), ({'max_credits': 8, 'window_seconds': 60},),
                       (8,), (True,), (DerivedLimit(8, 60),),
                       (QuotaLimit(8, 60), None)):
            with self.subTest(extras=extras), self.assertRaises(ProfileError) as caught:
                profile(extra_quota_limits=extras)
            self.assertEqual(caught.exception.code, 'invalid_quota_limits')

    def test_stable_sort_duplicates_and_hash_are_preserved(self):
        a, b = QuotaLimit(8, 60), QuotaLimit(8, 60)
        extras = (QuotaLimit(800, 86400), a, QuotaLimit(7, 60), b)
        p = profile(extra_quota_limits=extras)
        self.assertEqual(p.extra_quota_limits, (extras[2], a, b, extras[0]))
        self.assertIs(p.extra_quota_limits[1], a)
        self.assertIs(p.extra_quota_limits[2], b)
        self.assertEqual(p.config_hash, profile(extra_quota_limits=extras[::-1]).config_hash)
        self.assertNotEqual(p.config_hash, profile(extra_quota_limits=(extras[2], a, extras[0])).config_hash)
        self.assertIs(extras[0], p.extra_quota_limits[-1])

    def test_all_limits_are_forwarded_with_primary_first(self):
        for extras in ((), (QuotaLimit(800, 86400),),
                       (QuotaLimit(800, 86400), QuotaLimit(8, 60), QuotaLimit(8, 60))):
            with self.subTest(extras=extras):
                p = profile(extra_quota_limits=extras)
                self.assertEqual(p.quota_limits, (QuotaLimit(10, 60),) + p.extra_quota_limits)

    def test_profile_replacement_and_reading_do_not_mutate(self):
        p = profile(extra_quota_limits=(QuotaLimit(800, 86400),))
        before = (p.metadata(), p.config_hash, p.quota_limits)
        changed = replace(p, extra_quota_limits=(QuotaLimit(8, 60),))
        self.assertNotEqual(changed.config_hash, p.config_hash)
        self.assertEqual((p.metadata(), p.config_hash, p.quota_limits), before)
        with self.assertRaises(FrozenInstanceError):
            p.extra_quota_limits = ()

    def test_each_of_three_windows_can_block_independently(self):
        # Spend 2 at t=100; only the selected window has a budget of 2.
        # All others have 100, so no other limit explains the rejection.
        for blocked in range(3):
            with self.subTest(blocked_window=blocked), tempfile.TemporaryDirectory() as temp:
                limits = tuple(QuotaLimit(2 if i == blocked else 100, seconds)
                               for i, seconds in enumerate((10, 60, 180)))
                p = profile(max_attempts_per_window=limits[0].max_credits,
                            quota_window_seconds=limits[0].window_seconds,
                            extra_quota_limits=limits[1:])
                store = StateStore(Path(temp) / 'state.sqlite')
                try:
                    lease = store.acquire(p, 'first', 100, cost=2)
                    store.release(lease, 100)
                    def snapshot():
                        with closing(sqlite3.connect(store.path)) as db:
                            return tuple(db.iterdump())
                    before = snapshot()
                    with self.assertRaises(StateRejected) as caught:
                        store.acquire(p, 'blocked', 101)
                    self.assertEqual(caught.exception.code, 'quota')
                    self.assertEqual(caught.exception.retry_after_seconds,
                                     limits[blocked].window_seconds - 1)
                    self.assertEqual(snapshot(), before)
                    at = 100 + limits[blocked].window_seconds
                    with self.assertRaises(StateRejected):
                        store.acquire(p, 'just-before', at - 0.001)
                    store.release(store.acquire(p, 'boundary', at), at)
                finally:
                    store.close()

    def test_cost_over_extra_limit_rejected_before_transaction(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as temp:
            store = StateStore(Path(temp) / 'state.sqlite')
            try:
                p = profile(extra_quota_limits=(QuotaLimit(2, 180),))
                with patch.object(store, '_transaction', side_effect=AssertionError(
                        'Extra quota must reject cost before opening a transaction')) as transaction:
                    with self.assertRaises(StateRejected) as caught:
                        store.acquire(p, 'over-budget', 100, cost=3)
                    self.assertEqual(caught.exception.code, 'cost_exceeds_quota')
                    transaction.assert_not_called()
            finally:
                store.close()
