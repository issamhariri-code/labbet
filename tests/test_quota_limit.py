"""Quota value object boundaries and fingerprints predating its introduction."""
from dataclasses import FrozenInstanceError, asdict, fields, replace
from decimal import Decimal
import math
import unittest

from ingestion.api_profiles import ProfileError, QuotaLimit
from test_api_profiles import profile


class QuotaLimitTests(unittest.TestCase):
    def reject(self, credits, seconds):
        with self.assertRaises(ProfileError) as caught:
            QuotaLimit(credits, seconds)
        self.assertEqual(caught.exception.code, 'invalid_resource_limit')
        self.assertEqual(str(caught.exception),
                         'API configuration rejected: invalid_resource_limit')

    def test_credit_boundaries(self):
        for value in (1, 8, 800, 100000):
            with self.subTest(value=value):
                self.assertEqual(QuotaLimit(value, 60).max_credits, value)
        for value in (-1, 0, 100001, 10 ** 1000):
            with self.subTest(value_type=type(value).__name__):
                self.reject(value, 60)

    def test_window_boundaries(self):
        for value in (math.nextafter(0.0, 1.0), 0.5, 1, 60, 86400,
                      math.nextafter(31536000.0, 0.0), 31536000, 31536000.0):
            with self.subTest(value=value):
                self.assertEqual(QuotaLimit(8, value).window_seconds, float(value))
        for value in (-1, -0.0, 0, math.nextafter(31536000.0, math.inf),
                      31536001, 10 ** 1000):
            with self.subTest(value_type=type(value).__name__):
                self.reject(8, value)

    def test_credit_types_are_strict(self):
        class IntegerSubclass(int):
            pass
        for value in (True, False, 1.0, '8', None, [], {}, (), Decimal('8'),
                      complex(8), IntegerSubclass(8)):
            with self.subTest(value_type=type(value).__name__):
                self.reject(value, 60)

    def test_window_types_are_strict(self):
        class IntegerSubclass(int):
            pass
        class FloatSubclass(float):
            pass
        for value in (True, False, '60', None, [], {}, (), Decimal('60'),
                      complex(60), IntegerSubclass(60), FloatSubclass(60)):
            with self.subTest(value_type=type(value).__name__):
                self.reject(8, value)

    def test_nan_and_both_infinities_are_rejected_in_both_fields(self):
        for value in (float('nan'), float('inf'), float('-inf')):
            with self.subTest(value=str(value)):
                self.reject(value, 60)
                self.reject(8, value)

    def test_window_normalization_equality_and_hashing(self):
        integer, floating = QuotaLimit(8, 60), QuotaLimit(8, 60.0)
        self.assertIs(type(integer.max_credits), int)
        self.assertIs(type(integer.window_seconds), float)
        self.assertEqual(integer, floating)
        self.assertEqual(hash(integer), hash(floating))
        self.assertEqual(asdict(integer), {'max_credits': 8, 'window_seconds': 60.0})

    def test_frozen_slotted_object_and_replace_validation(self):
        value = QuotaLimit(8, 60)
        self.assertFalse(hasattr(value, '__dict__'))
        self.assertEqual([f.name for f in fields(value)], ['max_credits', 'window_seconds'])
        for field, changed in (('max_credits', 800), ('window_seconds', 86400)):
            with self.subTest(field=field):
                with self.assertRaises(FrozenInstanceError):
                    setattr(value, field, changed)
                with self.assertRaises(FrozenInstanceError):
                    delattr(value, field)
        updated = replace(value, max_credits=800, window_seconds=86400)
        self.assertEqual(value, QuotaLimit(8, 60))
        self.assertEqual(updated, QuotaLimit(800, 86400))
        with self.assertRaises(ProfileError):
            replace(value, max_credits=0)

    def test_existing_profile_hashes_match_pre_change_snapshot(self):
        # Generated using the previously verified, immutable source snapshot:
        # startup-source-selection-20261022/run-tyqs1jpk/workspace.
        # Baseline api_profiles.py SHA is recorded in this run's report.
        cases = [
            ({}, '635398a5e367cc1327c82aa3793afdb282e5655934249f4af9941842a93cfd83'),
            ({'quota_window_seconds': 60}, '635398a5e367cc1327c82aa3793afdb282e5655934249f4af9941842a93cfd83'),
            ({'quota_window_seconds': 60.0}, '635398a5e367cc1327c82aa3793afdb282e5655934249f4af9941842a93cfd83'),
            ({'source_id': 'source-b', 'account_ref': 'account-b'},
             '20801b82b89f4ea3bc6aff547618b83c473ce5392366ea548ea943b7971ecbe4'),
            ({'allowed_runtime_ips': ('192.0.2.0/24',), 'runtime_ip': '192.0.2.10'},
             'a3dc1cfcbe9d3180c9f67bfdfd79922910b871db1aef334b84a05164d0570bb3'),
            ({'verification_reference': 'review-2026-10'},
             '1150684771502d09a4847db60c157b609acd8166ca573d4926126576578ec255'),
        ]
        for changes, expected in cases:
            with self.subTest(changes=changes):
                original = profile(**changes)
                self.assertEqual(original.config_hash, expected)
                self.assertEqual(original.cache_namespace, original.source_id + ':' + expected)
                QuotaLimit(8, 60)
                QuotaLimit(800, 86400)
                self.assertEqual(profile(**changes).config_hash, expected)
                self.assertEqual(profile(**changes).metadata(), original.metadata())
