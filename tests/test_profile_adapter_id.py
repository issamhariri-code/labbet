"""Frozen profile adapter identity before per-fetch resolver/client binding."""
from contextlib import closing
from dataclasses import asdict, dataclass, fields, FrozenInstanceError, replace
import hashlib
import json
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

from api_contracts import ApiFailure
from api_state import StateRejected, StateStore
from ingestion.api_profiles import ProfileError, QuotaLimit, SourceProfile
from test_api_profiles import profile
from test_api_client import profile as runtime_profile
import api_profiles as runtime_profiles
import test_quota_json_integration as json_tests


ALPHA = "alphavantage-daily-v1"


class StringSubclass(str):
    pass


def bad_ids():
    return ("", "unknown-adapter", "alphavantage-daily-v2", "twelve-daily-v1",
            "ALPHAVANTAGE-DAILY-V1", " " + ALPHA, ALPHA + " ",
            "FAKE-PRIVATE-ADAPTER\nDO-NOT-ECHO", "x" * 1000,
            None, True, False, 0, 1.5, b"alphavantage-daily-v1", [], {}, ())


@dataclass(frozen=True, slots=True)
class RawAdapterMetadataProfile(SourceProfile):
    """Serialization-only double; deliberately bypasses all profile validation."""

    def __post_init__(self):
        pass


class ProfileAdapterIdentityTests(unittest.TestCase):
    def test_implicit_explicit_alpha_have_identical_metadata_hash_and_cache(self):
        for extras in ((), (QuotaLimit(800, 86400),),
                       (QuotaLimit(800, 86400), QuotaLimit(8, 60), QuotaLimit(8, 60))):
            with self.subTest(extras=extras):
                implicit = profile(extra_quota_limits=extras)
                explicit = profile(adapter_id=ALPHA, extra_quota_limits=extras)
                self.assertEqual((implicit.adapter_id, explicit.adapter_id), (ALPHA, ALPHA))
                self.assertEqual(implicit.metadata(), explicit.metadata())
                self.assertNotIn("adapter_id", implicit.metadata())
                self.assertEqual(implicit.config_hash, explicit.config_hash)
                self.assertEqual(implicit.cache_namespace, explicit.cache_namespace)
                self.assertEqual(implicit.quota_limits, explicit.quota_limits)

    def test_fixed_legacy_hashes_are_preserved_for_both_forms(self):
        cases = (({}, "635398a5e367cc1327c82aa3793afdb282e5655934249f4af9941842a93cfd83"),
                 ({"quota_window_seconds": 60}, "635398a5e367cc1327c82aa3793afdb282e5655934249f4af9941842a93cfd83"),
                 ({"quota_window_seconds": 60.0}, "635398a5e367cc1327c82aa3793afdb282e5655934249f4af9941842a93cfd83"),
                 ({"source_id": "source-b", "account_ref": "account-b"},
                  "20801b82b89f4ea3bc6aff547618b83c473ce5392366ea548ea943b7971ecbe4"))
        for changes, expected in cases:
            for explicit in (False, True):
                with self.subTest(changes=changes, explicit=explicit):
                    p = profile(**changes, **({"adapter_id": ALPHA} if explicit else {}))
                    self.assertEqual(p.config_hash, expected)
                    self.assertEqual(p.cache_namespace, p.source_id + ":" + expected)

    def test_invalid_ids_and_exact_string_subclasses_fail_without_echo(self):
        invalid = bad_ids() + (StringSubclass(ALPHA), StringSubclass("FAKE-DO-NOT-ECHO"))
        # Exercise both import forms used by direct profiles and real runtime.
        for factory, error_type in ((profile, ProfileError), (runtime_profile, runtime_profiles.ProfileError)):
            for index, value in enumerate(invalid):
                with self.subTest(factory=factory.__module__, index=index, type=type(value).__name__):
                    with self.assertRaises(error_type) as caught:
                        factory(adapter_id=value)
                    self.assertEqual(caught.exception.code, "unsupported_adapter")
                    self.assertEqual(str(caught.exception), "API configuration rejected: unsupported_adapter")
                    if isinstance(value, str) and value:
                        self.assertNotIn(value, str(caught.exception))

    def test_adapter_field_is_frozen_and_replace_preserves_or_revalidates(self):
        p = profile(extra_quota_limits=(QuotaLimit(8, 60),))
        original = (p.metadata(), p.config_hash, p.cache_namespace, p.quota_limits)
        with self.assertRaises(FrozenInstanceError):
            p.adapter_id = "unknown-adapter"
        with self.assertRaises(FrozenInstanceError):
            del p.adapter_id
        changed = replace(p, config_version="2")
        self.assertEqual(changed.adapter_id, ALPHA)
        self.assertEqual(replace(p, adapter_id=ALPHA), p)
        for value in bad_ids() + (StringSubclass(ALPHA),):
            with self.subTest(type=type(value).__name__):
                with self.assertRaises(ProfileError) as caught:
                    replace(p, adapter_id=value)
                self.assertEqual(caught.exception.code, "unsupported_adapter")
        self.assertEqual((p.metadata(), p.config_hash, p.cache_namespace, p.quota_limits), original)

    def test_dataclass_exposes_field_but_default_metadata_omits_only_that_field(self):
        p = profile(extra_quota_limits=(QuotaLimit(800, 86400),))
        self.assertIn("adapter_id", {field.name for field in fields(p)})
        expected = asdict(p)
        self.assertEqual(expected.pop("adapter_id"), ALPHA)
        self.assertIsNone(expected.pop("availability_rule_version"))
        self.assertEqual(p.metadata(), expected)
        copied = p.metadata()
        copied["source_id"] = "changed"
        copied["extra_quota_limits"][0]["max_credits"] = 1
        self.assertEqual(p.metadata(), expected)

    def test_future_adapter_is_retained_only_in_raw_serialization_double(self):
        for extras in ((), (QuotaLimit(800, 86400), QuotaLimit(8, 60))):
            base = profile(extra_quota_limits=extras)
            values = {field.name: getattr(base, field.name) for field in fields(base)}
            raw_alpha = RawAdapterMetadataProfile(**values)
            self.assertEqual(raw_alpha.metadata(), base.metadata())
            hashes = set()
            for future_id in ("alphavantage-daily-v2", "future-provider-v1"):
                with self.subTest(extras=extras, future_id=future_id):
                    raw = RawAdapterMetadataProfile(**dict(values, adapter_id=future_id))
                    expected = dict(base.metadata(), adapter_id=future_id)
                    self.assertEqual(raw.metadata(), expected)
                    body = json.dumps(expected, sort_keys=True, separators=(",", ":"), allow_nan=False)
                    self.assertEqual(raw.config_hash, hashlib.sha256(body.encode()).hexdigest())
                    self.assertNotEqual(raw.config_hash, base.config_hash)
                    self.assertEqual(raw.cache_namespace, raw.source_id + ":" + raw.config_hash)
                    self.assertNotIn(raw.config_hash, hashes)
                    hashes.add(raw.config_hash)
                    with self.assertRaises(ProfileError) as caught:
                        profile(extra_quota_limits=extras, adapter_id=future_id)
                    self.assertEqual(caught.exception.code, "unsupported_adapter")


class ProfileAdapterIdentityIntegrationTests(unittest.TestCase):
    # Reuse the real JSON/SQLite fixture helpers, without inheriting/re-running
    # their 15 tests or importing their TestCase class into this module.
    setUp = json_tests.QuotaJsonIntegrationTests.setUp
    registry = json_tests.QuotaJsonIntegrationTests.registry
    load = json_tests.QuotaJsonIntegrationTests.load
    close_client = staticmethod(json_tests.QuotaJsonIntegrationTests.close_client)
    snapshot = staticmethod(json_tests.QuotaJsonIntegrationTests.snapshot)
    query = staticmethod(json_tests.QuotaJsonIntegrationTests.query)
    spend = staticmethod(json_tests.QuotaJsonIntegrationTests.spend)
    reject = json_tests.QuotaJsonIntegrationTests.reject
    assert_inventory = json_tests.QuotaJsonIntegrationTests.assert_inventory

    def registry_id(self, explicit, extras=json_tests.MISSING, **changes):
        registry = self.registry(extras, **changes)
        values = registry["sources"][0]["profile"]
        if explicit:
            values["adapter_id"] = ALPHA
        else:
            values.pop("adapter_id")
        return registry

    def test_old_and_explicit_json_reopen_keep_profile_and_consumption(self):
        for extras in (json_tests.MISSING, [json_tests.rule(5, 60)]):
            with self.subTest(extra=extras is not json_tests.MISSING):
                registry = self.registry_id(False, extras,
                    max_attempts_per_window=2, quota_window_seconds=10)
                client, p = self.load(registry)
                client.state.register(p)
                self.spend(client.state, p, "before", 100, 2)
                before = self.snapshot(client.state)
                self.close_client(client)
                registry["sources"][0]["profile"]["adapter_id"] = ALPHA
                client, q = self.load(registry)
                client.state.register(q)
                self.assertEqual(q.adapter_id, ALPHA)
                self.assertEqual((q.metadata(), q.config_hash, q.cache_namespace),
                                 (p.metadata(), p.config_hash, p.cache_namespace))
                self.assertEqual(self.snapshot(client.state), before)
                self.assert_inventory(client.state, q)
                self.reject(client.state, q, 101, 1, "quota", 9)
                self.spend(client.state, q, "after-short-window", 110, 2)
                if extras is not json_tests.MISSING:
                    self.spend(client.state, q, "long-full", 120)
                    before = self.snapshot(client.state)
                    self.close_client(client)
                    registry["sources"][0]["profile"].pop("adapter_id")
                    client, r = self.load(registry)
                    self.assertEqual(self.snapshot(client.state), before)
                    self.reject(client.state, r, 121, 1, "quota", 39)
                self.close_client(client)

    def test_inventory_reads_missing_and_explicit_alpha_after_reopen_without_writes(self):
        for extras in (json_tests.MISSING, [json_tests.rule(5, 60)]):
            for explicit in (False, True):
                with self.subTest(extra=extras is not json_tests.MISSING, explicit=explicit):
                    client, p = self.load(self.registry_id(explicit, extras,
                        max_attempts_per_window=2, quota_window_seconds=10))
                    store = client.state
                    store.register(p)
                    self.spend(store, p, "spent", 100, 2)
                    values = json.loads(self.query(store, "SELECT metadata FROM profiles")[0][0])
                    self.assertNotIn("adapter_id", values)
                    if explicit:
                        values["adapter_id"] = ALPHA
                        with closing(sqlite3.connect(store.path)) as db:
                            db.execute("UPDATE profiles SET metadata=?", (json.dumps(values),))
                            db.commit()
                    before = self.snapshot(store)
                    self.assert_inventory(store, p)
                    self.assertEqual(self.snapshot(store), before)
                    self.close_client(client)
                    reopened = StateStore(store.path, existing_only=True)
                    self.addCleanup(reopened.close)
                    self.assert_inventory(reopened, p)
                    self.assertEqual(self.snapshot(reopened), before)
                    self.reject(reopened, p, 101, 1, "quota", 9)

    def test_bad_json_id_fails_before_state_and_credentials_are_constructed(self):
        # Bytes/tuple/subclasses do not retain their Python type in JSON; the
        # direct model tests above cover those distinct Python inputs.
        values = ("", "unknown-adapter", ALPHA + " ", "FAKE-PRIVATE-ID", None, True, 0, 1.5, [], {})
        for value in values:
            with self.subTest(type=type(value).__name__):
                registry = self.registry_id(True)
                registry["sources"][0]["profile"]["adapter_id"] = value
                with patch("api_runtime.StateStore", side_effect=AssertionError("state construction forbidden")) as state, \
                        patch("api_runtime.CredentialStore", side_effect=AssertionError("credentials forbidden")) as credentials:
                    with self.assertRaises(ApiFailure) as caught:
                        self.load(registry)
                    self.assertEqual(caught.exception.code, "unsupported_adapter")
                    self.assertEqual(str(caught.exception), "API-hämtningen stoppades: unsupported_adapter")
                    state.assert_not_called()
                    credentials.assert_not_called()
                self.assertFalse(Path(registry["state_path"]).exists())

    def test_bad_later_profile_preserves_all_existing_state_and_credentials(self):
        registry = self.registry_id(False, [json_tests.rule(5, 60)],
                                    max_attempts_per_window=2, quota_window_seconds=10)
        client, p = self.load(registry)
        client.state.register(p)
        self.spend(client.state, p, "spent", 100, 2)
        before = self.snapshot(client.state)
        for value in ("", "FAKE-PRIVATE-ID", None, True, 1, []):
            with self.subTest(type=type(value).__name__):
                bad = json.loads(json.dumps(registry, default=str))
                other = json.loads(json.dumps(bad["sources"][0]))
                other["profile"].update(source_id="bad-source", credential_env="KEY_B", adapter_id=value)
                bad["sources"].append(other)
                with patch("api_runtime.StateStore", side_effect=AssertionError("state construction forbidden")) as state, \
                        patch("api_runtime.CredentialStore", side_effect=AssertionError("credentials forbidden")) as credentials:
                    with self.assertRaises(ApiFailure) as caught:
                        self.load(bad, source_id="source-a")
                    self.assertEqual(caught.exception.code, "unsupported_adapter")
                    state.assert_not_called()
                    credentials.assert_not_called()
                self.assertEqual(self.snapshot(client.state), before)
        self.reject(client.state, p, 101, 1, "quota", 9)

    def test_inventory_rejects_unknown_id_without_partial_changes(self):
        client, p = self.load(self.registry_id(False, [json_tests.rule(5, 60)]))
        client.state.register(p)
        for value in ("", "unknown-adapter", None, True, 0, []):
            with self.subTest(type=type(value).__name__):
                values = json.loads(json.dumps(p.metadata()))
                values["adapter_id"] = value
                with closing(sqlite3.connect(client.state.path)) as db:
                    db.execute("UPDATE profiles SET metadata=?", (json.dumps(values),))
                    db.commit()
                before = self.snapshot(client.state)
                with self.assertRaises(StateRejected) as caught:
                    client.state.inventory()
                self.assertEqual(caught.exception.code, "invalid_inventory")
                self.assertEqual(self.snapshot(client.state), before)
