"""Read-only legacy quota view; no additional quota policy is activated."""
from dataclasses import FrozenInstanceError, asdict, fields, replace
import math
import unittest

from ingestion.api_profiles import QuotaLimit, SourceProfile
from test_api_profiles import profile


class ProfileQuotaLimitsTests(unittest.TestCase):
    def test_values_boundaries_and_float_normalization(self):
        for credits in (1, 8, 800, 100000):
            for seconds in (math.nextafter(0.0, 1.0), 0.5, 1, 60, 60.0,
                            86400, 31536000):
                with self.subTest(credits=credits, seconds=seconds):
                    p = profile(max_attempts_per_window=credits, quota_window_seconds=seconds)
                    limits = p.quota_limits
                    self.assertIs(type(limits), tuple)
                    self.assertEqual(len(limits), 1)
                    self.assertIs(type(limits[0]), QuotaLimit)
                    self.assertEqual(limits[0].max_credits, credits)
                    self.assertEqual(limits[0].window_seconds, float(seconds))
                    self.assertIs(type(limits[0].window_seconds), float)

    def test_new_validated_profile_gets_own_limits_without_changing_original(self):
        p = profile(max_attempts_per_window=8, quota_window_seconds=60)
        metadata, fingerprint = p.metadata(), p.config_hash
        changed = replace(p, max_attempts_per_window=800, quota_window_seconds=86400)
        self.assertEqual(changed.quota_limits, (QuotaLimit(800, 86400),))
        self.assertEqual(p.quota_limits, (QuotaLimit(8, 60),))
        self.assertEqual(p.metadata(), metadata)
        self.assertEqual(p.config_hash, fingerprint)
        self.assertNotEqual(changed.config_hash, fingerprint)

    def test_property_is_read_only_and_cannot_be_deleted(self):
        descriptor = SourceProfile.quota_limits
        self.assertIsInstance(descriptor, property)
        self.assertIsNone(descriptor.fset)
        self.assertIsNone(descriptor.fdel)
        p = profile()
        expected = p.quota_limits
        with self.assertRaises((AttributeError, TypeError)):
            p.quota_limits = ()
        with self.assertRaises((AttributeError, TypeError)):
            del p.quota_limits
        self.assertEqual(p.quota_limits, expected)

    def test_returned_tuple_and_quota_objects_cannot_be_mutated(self):
        p = profile()
        limits = p.quota_limits
        with self.assertRaises(TypeError):
            limits[0] = QuotaLimit(800, 86400)
        with self.assertRaises(FrozenInstanceError):
            limits[0].max_credits = 800
        with self.assertRaises(FrozenInstanceError):
            limits[0].window_seconds = 86400
        limits += (QuotaLimit(800, 86400),)
        self.assertEqual(len(limits), 2)
        self.assertEqual(p.quota_limits, (QuotaLimit(10, 60),))

    def test_property_adds_no_dataclass_field_or_serialized_metadata(self):
        p = profile()
        before = p.metadata()
        for _ in range(3):
            self.assertEqual(p.quota_limits, (QuotaLimit(10, 60),))
        self.assertNotIn('quota_limits', {f.name for f in fields(p)})
        self.assertNotIn('quota_limits', asdict(p))
        self.assertEqual(p.metadata(), before)
        expected = asdict(p)
        expected.pop('extra_quota_limits')
        self.assertEqual(expected.pop('adapter_id'), 'alphavantage-daily-v1')
        self.assertIsNone(expected.pop('availability_rule_version'))
        self.assertEqual(p.metadata(), expected)

    def test_reading_property_preserves_fixed_legacy_fingerprint(self):
        expected = '635398a5e367cc1327c82aa3793afdb282e5655934249f4af9941842a93cfd83'
        for seconds in (60, 60.0):
            with self.subTest(seconds=seconds):
                p = profile(quota_window_seconds=seconds)
                self.assertEqual(p.config_hash, expected)
                self.assertEqual(p.quota_limits, (QuotaLimit(10, 60),))
                self.assertEqual(p.config_hash, expected)
                self.assertEqual(p.cache_namespace, 'source-a:' + expected)
