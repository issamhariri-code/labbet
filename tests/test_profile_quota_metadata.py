"""Real profile metadata compatibility, plus isolated raw serialization probes."""
from dataclasses import asdict, dataclass
import hashlib
import json
import unittest

from ingestion.api_profiles import QuotaLimit, SourceProfile
from test_api_profiles import profile


@dataclass(frozen=True, slots=True)
class RawMetadataProfile(SourceProfile):
    """Bypass validation only to test metadata serialization of invalid raw values."""

    def __post_init__(self):
        pass


def synthetic(extras=(), **changes):
    return profile(extra_quota_limits=extras, **changes)


class ProfileQuotaMetadataTests(unittest.TestCase):
    def test_existing_metadata_and_fixed_hashes_remain_unchanged(self):
        for changes, expected in (
                ({}, '635398a5e367cc1327c82aa3793afdb282e5655934249f4af9941842a93cfd83'),
                ({'quota_window_seconds': 60},
                 '635398a5e367cc1327c82aa3793afdb282e5655934249f4af9941842a93cfd83'),
                ({'source_id': 'source-b', 'account_ref': 'account-b'},
                 '20801b82b89f4ea3bc6aff547618b83c473ce5392366ea548ea943b7971ecbe4')):
            with self.subTest(changes=changes):
                p = profile(**changes)
                expected_metadata = asdict(p)
                expected_metadata.pop('extra_quota_limits')
                self.assertEqual(expected_metadata.pop('adapter_id'), 'alphavantage-daily-v1')
                self.assertIsNone(expected_metadata.pop('availability_rule_version'))
                self.assertEqual(p.metadata(), expected_metadata)
                self.assertEqual(p.config_hash, expected)
                self.assertEqual(p.cache_namespace, p.source_id + ':' + expected)

    def test_empty_tuple_is_omitted_and_preserves_legacy_hash_and_cache(self):
        for changes in ({}, {'quota_window_seconds': 60.0},
                        {'allowed_runtime_ips': ('192.0.2.0/24',), 'runtime_ip': '192.0.2.10'}):
            with self.subTest(changes=changes):
                base, extended = profile(**changes), synthetic(**changes)
                self.assertIn('extra_quota_limits', asdict(extended))
                self.assertNotIn('extra_quota_limits', extended.metadata())
                self.assertEqual(extended.metadata(), base.metadata())
                self.assertEqual(extended.config_hash, base.config_hash)
                self.assertEqual(extended.cache_namespace, base.cache_namespace)
                self.assertEqual(extended.extra_quota_limits, ())

    def test_nonempty_limits_are_fully_serialized_and_change_hash(self):
        baseline = profile()
        variants = (
            (QuotaLimit(800, 86400),),
            (QuotaLimit(801, 86400),),
            (QuotaLimit(800, 86401),),
            (QuotaLimit(800, 86400), QuotaLimit(8, 60)),
            (QuotaLimit(8, 60), QuotaLimit(800, 86400)),
            (QuotaLimit(800, 86400), QuotaLimit(800, 86400)),
        )
        fingerprints = set()
        for extras in variants:
            with self.subTest(extras=extras):
                p = synthetic(extras)
                expected = baseline.metadata()
                expected['extra_quota_limits'] = tuple(asdict(limit) for limit in p.extra_quota_limits)
                self.assertEqual(p.metadata(), expected)
                encoded = json.dumps(expected, sort_keys=True, separators=(',', ':'), allow_nan=False)
                self.assertEqual(p.config_hash, hashlib.sha256(encoded.encode()).hexdigest())
                self.assertNotEqual(p.config_hash, baseline.config_hash)
                self.assertEqual(p.cache_namespace, p.source_id + ':' + p.config_hash)
                self.assertEqual(json.loads(encoded)['extra_quota_limits'],
                                 [asdict(limit) for limit in p.extra_quota_limits])
                fingerprints.add(p.config_hash)
        # Reversed inputs normalize to the same profile; duplicates remain distinct.
        self.assertEqual(len(fingerprints), 5)

    def test_other_empty_values_are_not_silently_omitted(self):
        # These are serialization probes, not accepted production profile types.
        for extras in (None, []):
            with self.subTest(extras=extras):
                p = RawMetadataProfile(**dict(asdict(profile()), extra_quota_limits=extras))
                self.assertIn('extra_quota_limits', p.metadata())
                self.assertEqual(p.metadata()['extra_quota_limits'], extras)
                self.assertNotEqual(p.config_hash, profile().config_hash)

    def test_metadata_does_not_mutate_profile_or_extra_limits(self):
        extras = (QuotaLimit(800, 86400), QuotaLimit(8, 60))
        p = synthetic(extras)
        before = asdict(p)
        identities = tuple(map(id, p.extra_quota_limits))
        original_hash = p.config_hash
        metadata = p.metadata()
        metadata['source_id'] = 'changed-copy'
        metadata['extra_quota_limits'][0]['max_credits'] = 999
        self.assertEqual(asdict(p), before)
        self.assertEqual(tuple(map(id, p.extra_quota_limits)), identities)
        self.assertEqual(p.extra_quota_limits, tuple(sorted(extras, key=lambda x: (x.window_seconds, x.max_credits))))
        self.assertEqual(p.config_hash, original_hash)
        self.assertEqual(p.metadata()['extra_quota_limits'][0]['max_credits'], 8)
