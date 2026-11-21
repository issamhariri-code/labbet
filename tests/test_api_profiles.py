"""Synthetic credentials: no provider, key or region facts are assumed."""

from dataclasses import FrozenInstanceError, replace
from datetime import date
from pathlib import Path
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ingestion.api_profiles import CredentialStore, ProfileError, SourceProfile
from ingestion.api_state import StateStore


def profile(**changes):
    values = dict(source_id="source-a", account_ref="account-a", environment="test",
                  endpoint="https://a.example.test/query", credential_env="SOURCE_A_KEY",
                  credential_version="v1", quota_scope="account-a-quota",
                  rights_verified=True, rights_review_date="2026-01-01",
                  review_due_date="2027-01-01", timezone="UTC", currency="USD",
                  exchange="TEST", max_attempts_per_window=10)
    values.update(changes)
    return SourceProfile(**values)


class SourceProfileTests(unittest.TestCase):
    def test_frozen_profile_and_nested_policies(self):
        value = profile()
        with self.assertRaises(FrozenInstanceError):
            value.endpoint = "https://b.example.test"
        for change in ({"allowed_runtime_ips": []}, {"allowed_account_regions": []}):
            with self.subTest(change=change), self.assertRaises(ProfileError):
                profile(**change)

    def test_endpoint_rejects_credentials_cleartext_query_and_controls(self):
        for endpoint in ("http://a.example.test", "https://u:secret@a.example.test", "https://a.example.test/?key=secret",
                         "https://a.example.test/#key", "https://a.example.test\n.evil", "https://a.example.test\\evil",
                         "https://a.example.test:70000", "https:///query", "file:///data"):
            with self.subTest(endpoint=endpoint), self.assertRaises(ProfileError) as caught:
                profile(endpoint=endpoint)
            self.assertNotIn(endpoint, str(caught.exception))

    def test_invalid_metadata_and_unbounded_policy_fail(self):
        for change in ({"source_id": "a\nforged-log"}, {"environment": "live"}, {"credential_env": "secret=value"},
                       {"timezone": "Definitely/NotAZone"}, {"currency": "usd"}, {"price_basis": "guess"},
                       {"purpose": "place-orders"}, {"max_concurrency": True}, {"quota_window_seconds": float("nan")},
                       {"circuit_reset_seconds": float("inf")}, {"max_attempts_per_window": 0}):
            with self.subTest(change=change), self.assertRaises(ProfileError):
                profile(**change)

    def test_rights_unverified_expired_future_or_wrong_environment_fail(self):
        for change in ({"rights_verified": False}, {"rights_review_date": None}, {"review_due_date": None},
                       {"review_due_date": "2026-02-01"}, {"rights_review_date": "2026-12-01"}):
            with self.subTest(change=change), self.assertRaises(ProfileError):
                profile(**change).assert_ready(today=date(2026, 10, 15))
        with self.assertRaisesRegex(ProfileError, "environment_mismatch"):
            profile().assert_ready(today=date(2026, 10, 15), expected_environment="production")
        profile().assert_ready(today=date(2026, 10, 15), expected_environment="test")

    def test_region_policy_keeps_account_endpoint_and_market_separate(self):
        value = profile(required_regions=True, account_region="US", endpoint_region="EU",
                        allowed_account_regions=("US",), allowed_endpoint_regions=("EU",),
                        exchange="JP-TEST", currency="JPY")
        value.assert_ready(today=date(2026, 10, 15))
        for change in ({"account_region": None}, {"endpoint_region": None}, {"account_region": "CA"}, {"endpoint_region": "US"}):
            with self.subTest(change=change), self.assertRaises(ProfileError):
                replace(value, **change).assert_ready(today=date(2026, 10, 15))

    def test_outbound_ip_allowlist_requires_known_allowed_address(self):
        value = profile(allowed_runtime_ips=("192.0.2.0/24", "2001:db8::/32"), runtime_ip="192.0.2.10")
        value.assert_ready(today=date(2026, 10, 15))
        replace(value, runtime_ip="2001:db8::1").assert_ready(today=date(2026, 10, 15))
        for ip in (None, "198.51.100.1"):
            with self.subTest(ip=ip), self.assertRaises(ProfileError):
                replace(value, runtime_ip=ip).assert_ready(today=date(2026, 10, 15))

    def test_fingerprint_and_cache_namespace_separate_account_schema_and_prices(self):
        base = profile()
        self.assertEqual(base.config_hash, profile().config_hash)
        for change in ({"source_id": "b"}, {"account_ref": "b"}, {"schema_version": "2"}, {"price_basis": "adjusted"}, {"credential_version": "v2"}):
            with self.subTest(change=change):
                other = replace(base, **change)
                self.assertNotEqual(base.config_hash, other.config_hash)
                self.assertNotEqual(base.cache_namespace, other.cache_namespace)

    def test_optional_rights_evidence_reference_rejects_signed_or_multiline_text(self):
        for value in ("review-2026-10", "https://docs.example.test/rights#read-only"):
            self.assertEqual(profile(verification_reference=value).metadata()["verification_reference"], value)
        for value in ("https://docs.example.test/?key=FAKE-KEY", "review\nforged", "https://secret@docs.example.test", "a" * 257):
            with self.subTest(value=value), self.assertRaises(ProfileError):
                profile(verification_reference=value)


class CredentialTests(unittest.TestCase):
    def test_missing_blank_and_control_key_fail_without_echo(self):
        store = CredentialStore()
        for value in (None, "", "   ", "FAKE\nKEY", "FAKE\x7fKEY", "FAKE\u0085KEY", "FAKE\u200bKEY", 123):
            environ = {} if value is None else {"SOURCE_A_KEY": value}
            with self.subTest(value=value), self.assertRaises(ProfileError) as caught:
                store.install(profile(), environ)
            if isinstance(value, str) and value:
                self.assertNotIn(value, str(caught.exception))

    def test_valid_key_preserved_exactly_and_hidden_from_repr(self):
        key = " FAKE +/%?=KEY "
        store = CredentialStore()
        value = store.install(profile(), {"SOURCE_A_KEY": key})
        self.assertEqual(value.secret, key)
        self.assertNotIn(key, repr(value))
        self.assertIs(store.snapshot(profile(), 100), value)

    def test_expiry_and_same_version_replacement_rejected(self):
        store = CredentialStore()
        original = store.install(profile(), {"SOURCE_A_KEY": "FAKE-ONE"}, expires_at=200)
        store.assert_active(original, 199)
        with self.assertRaisesRegex(ProfileError, "credential_expired"):
            store.assert_active(original, 200)
        with self.assertRaisesRegex(ProfileError, "credential_version_reused"):
            store.install(profile(), {"SOURCE_A_KEY": "FAKE-TWO"}, expires_at=200)

    def test_registry_configured_expiry_applies_without_install_override(self):
        store = CredentialStore()
        configured = profile(credential_expires_at=200)
        value = store.install(configured, {"SOURCE_A_KEY": "FAKE-A"})
        store.assert_active(value, 199)
        with self.assertRaisesRegex(ProfileError, "credential_expired"):
            store.snapshot(configured, 200)
        with self.assertRaisesRegex(ProfileError, "credential_expiry_mismatch"):
            store.install(configured, {"SOURCE_A_KEY": "FAKE-A"}, expires_at=300)

    def test_rotation_leaves_old_snapshot_fixed_and_new_jobs_versioned(self):
        store = CredentialStore()
        old = store.install(profile(), {"SOURCE_A_KEY": "FAKE-OLD"})
        updated = store.rotate(profile(), "v2", {"SOURCE_A_KEY": "FAKE-NEW"}, now=100)
        new = store.snapshot(updated, 100)
        self.assertNotEqual(updated.config_version, profile().config_version)
        self.assertEqual((old.secret, new.secret), ("FAKE-OLD", "FAKE-NEW"))
        store.assert_active(old, 101)
        with self.assertRaisesRegex(ProfileError, "credential_superseded"):
            store.snapshot(profile(), 101)
        store.revoke("source-a", "v1", now=102)
        with self.assertRaisesRegex(ProfileError, "credential_revoked"):
            store.assert_active(old, 103)
        store.assert_active(new, 103)

    def test_failed_rotation_preserves_original(self):
        store = CredentialStore()
        old = store.install(profile(), {"SOURCE_A_KEY": "FAKE-OLD"})
        with self.assertRaises(ProfileError):
            store.rotate(profile(), "v2", {"SOURCE_A_KEY": ""}, now=100)
        self.assertIs(store.snapshot(profile(), 101), old)

    def test_identical_configuration_reload_preserves_existing_snapshot(self):
        store = CredentialStore()
        old = store.install(profile(), {"SOURCE_A_KEY": "FAKE-OLD"})
        self.assertIs(store.install(profile(), {"SOURCE_A_KEY": "FAKE-OLD"}), old)
        store.assert_active(old, 100)

    def test_rotation_active_version_survives_restart_and_old_config_reload(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "state.sqlite"
            first = CredentialStore(StateStore(path))
            first.install(profile(), {"SOURCE_A_KEY": "FAKE-OLD"})
            updated = first.rotate(profile(), "v2", {"SOURCE_A_KEY": "FAKE-NEW"}, now=100)
            reloaded = CredentialStore(StateStore(path))
            reloaded.install(profile(), {"SOURCE_A_KEY": "FAKE-OLD"})
            with self.assertRaisesRegex(ProfileError, "credential_superseded"):
                reloaded.snapshot(profile(), 101)
            reloaded.install(updated, {"SOURCE_A_KEY": "FAKE-NEW"})
            self.assertEqual(reloaded.snapshot(updated, 101).secret, "FAKE-NEW")

    def test_same_version_changed_secret_rejected_after_state_restart(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "state.sqlite"
            first = CredentialStore(StateStore(path))
            first.install(profile(), {"SOURCE_A_KEY": "FAKE-ORIGINAL"})
            restarted = CredentialStore(StateStore(path))
            with self.assertRaisesRegex(ProfileError, "credential_version_reused"):
                restarted.install(profile(), {"SOURCE_A_KEY": "FAKE-CHANGED"})
            original = restarted.install(profile(), {"SOURCE_A_KEY": "FAKE-ORIGINAL"})
            restarted.assert_active(original, 100)
            updated = restarted.rotate(profile(), "v2", {"SOURCE_A_KEY": "FAKE-CHANGED"}, now=101)
            self.assertEqual(restarted.snapshot(updated, 102).secret, "FAKE-CHANGED")
            self.assertNotIn(b"FAKE-ORIGINAL", path.read_bytes())
            self.assertNotIn(b"FAKE-CHANGED", path.read_bytes())

    def test_revocation_survives_process_state_reload_and_config_change(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "state.sqlite"
            state = StateStore(path)
            first = CredentialStore(state)
            first.install(profile(), {"SOURCE_A_KEY": "FAKE-OLD"})
            first.revoke("source-a", "v1", now=100)
            first.close()
            state.close()
            reopened = CredentialStore(StateStore(path))
            with self.assertRaisesRegex(ProfileError, "credential_revoked"):
                reopened.install(profile(config_version="2"), {"SOURCE_A_KEY": "FAKE-OLD"})
            self.assertNotIn(b"FAKE-OLD", path.read_bytes())

    def test_revocation_before_installation_cannot_be_undone(self):
        store = CredentialStore()
        store.revoke("source-a", "v1", now=100)
        with self.assertRaisesRegex(ProfileError, "credential_revoked"):
            store.install(profile(), {"SOURCE_A_KEY": "FAKE-OLD"})

    def test_overlapping_source_snapshots_cannot_cross_contaminate(self):
        store = CredentialStore()
        first = profile()
        second = profile(source_id="source-b", account_ref="account-b", credential_env="SOURCE_B_KEY",
                         endpoint="https://b.example.test/query", quota_scope="b-quota")
        env = {"SOURCE_A_KEY": "FAKE-A", "SOURCE_B_KEY": "FAKE-B"}
        store.install(first, env)
        store.install(second, env)
        barrier = threading.Barrier(2)
        results = {}

        def fetch(value):
            snapshot = store.snapshot(value, 100)
            barrier.wait(timeout=3)
            env[value.credential_env] = "FAKE-EXTERNAL-CHANGE"
            results[value.source_id] = (snapshot.secret, value.endpoint)

        threads = [threading.Thread(target=fetch, args=(value,)) for value in (first, second)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=4)
            self.assertFalse(thread.is_alive())
        self.assertEqual(results, {"source-a": ("FAKE-A", first.endpoint), "source-b": ("FAKE-B", second.endpoint)})

    def test_close_prevents_new_secret_installation(self):
        store = CredentialStore()
        store.install(profile(), {"SOURCE_A_KEY": "FAKE-A"})
        store.close()
        with self.assertRaises(ProfileError):
            store.install(profile(), {"SOURCE_A_KEY": "FAKE-B"})


if __name__ == "__main__":
    unittest.main()
