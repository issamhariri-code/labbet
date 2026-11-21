"""Standalone policy bytes for validated limits; no multi-window admission."""
from dataclasses import replace
from itertools import permutations
import json
import unittest
from unittest.mock import patch

from ingestion.api_profiles import QuotaLimit
from ingestion.api_state import StateRejected, _quota_policy


class QuotaPolicyTests(unittest.TestCase):
    def setUp(self):
        connection = patch('ingestion.api_state.sqlite3.connect',
                           side_effect=AssertionError('Policy serialization must be standalone'))
        self.connect = connection.start()
        self.addCleanup(connection.stop)
        self.addCleanup(self.connect.assert_not_called)

    def test_single_window_preserves_legacy_bytes_and_normalizes_seconds(self):
        for credits, seconds, concurrency, expected in (
                (10, 60, 4, b'[10,60.0,4]'),
                (10, 60.0, 4, b'[10,60.0,4]'),
                (8, 60, 1, b'[8,60.0,1]'),
                (800, 86400, 3, b'[800,86400.0,3]'),
                (1, 0.5, 2, b'[1,0.5,2]'),
                (100000, 31536000, 10000, b'[100000,31536000.0,10000]')):
            with self.subTest(credits=credits, seconds=seconds, concurrency=concurrency):
                result = _quota_policy((QuotaLimit(credits, seconds),), concurrency)
                self.assertEqual(result.encode('utf-8'), expected)
                self.assertEqual(json.loads(result), [credits, float(seconds), concurrency])

    def test_all_orders_of_two_or_more_windows_produce_identical_bytes(self):
        for limits, expected in (
                ((QuotaLimit(800, 86400), QuotaLimit(8, 60)),
                 b'{"limits":[[8,60.0],[800,86400.0]],"max_concurrency":4,"version":2}'),
                ((QuotaLimit(800, 86400), QuotaLimit(9, 60), QuotaLimit(8, 60)),
                 b'{"limits":[[8,60.0],[9,60.0],[800,86400.0]],"max_concurrency":4,"version":2}')):
            for ordered in permutations(limits):
                with self.subTest(limits=ordered):
                    self.assertEqual(_quota_policy(ordered, 4).encode('utf-8'), expected)

    def test_multi_window_json_has_versioned_structure_and_all_limits(self):
        policy = _quota_policy((QuotaLimit(800, 86400), QuotaLimit(8, 60)), 3)
        self.assertEqual(json.loads(policy), {
            'version': 2, 'limits': [[8, 60.0], [800, 86400.0]], 'max_concurrency': 3})

    def test_each_credit_window_and_concurrency_change_affects_policy(self):
        # Exercise every position, including a time change that reorders windows.
        for limits in ((QuotaLimit(8, 60),),
                       (QuotaLimit(8, 60), QuotaLimit(80, 3600), QuotaLimit(800, 86400))):
            original = _quota_policy(limits, 4)
            for index, limit in enumerate(limits):
                for field, value in (('max_credits', limit.max_credits + 1),
                                     ('window_seconds', limit.window_seconds + 1),
                                     ('window_seconds', 0.5)):
                    changed = list(limits)
                    changed[index] = replace(limit, **{field: value})
                    with self.subTest(limits=limits, index=index, field=field, value=value):
                        self.assertNotEqual(_quota_policy(changed, 4), original)
            self.assertNotEqual(_quota_policy(limits, 3), original)

    def test_input_list_and_tuple_remain_unchanged(self):
        for collection in (list, tuple):
            limits = collection((QuotaLimit(800, 86400), QuotaLimit(9, 60), QuotaLimit(8, 60)))
            before = tuple(limits)
            identities = tuple(map(id, limits))
            first = _quota_policy(limits, 4)
            self.assertEqual(_quota_policy(limits, 4), first)
            self.assertEqual(tuple(limits), before)
            self.assertEqual(tuple(map(id, limits)), identities)

    def test_duplicate_windows_are_preserved(self):
        limit = QuotaLimit(8, 60)
        policy = _quota_policy((limit, QuotaLimit(800, 86400), limit), 4)
        self.assertEqual(json.loads(policy)['limits'], [[8, 60.0], [8, 60.0], [800, 86400.0]])
        self.assertNotEqual(policy, _quota_policy((limit, QuotaLimit(800, 86400)), 4))

    def test_empty_collections_are_rejected(self):
        for limits in ((), []):
            with self.subTest(limits=limits), self.assertRaises(StateRejected) as caught:
                _quota_policy(limits, 4)
            self.assertEqual(caught.exception.code, 'unsupported_quota_policy')
            self.assertEqual(caught.exception.retry_after_seconds, 0)
