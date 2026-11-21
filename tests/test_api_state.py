"""Durable coordination with controlled clocks, synthetic identities and temp DBs."""

from dataclasses import replace
from contextlib import closing
from pathlib import Path
import os
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from ingestion.api_profiles import CredentialStore, ProfileError, SourceProfile
from ingestion.api_state import StateRejected, StateStore


def profile(**changes):
    values = dict(source_id="source-a", account_ref="a", environment="test",
                  endpoint="https://a.example.test/query", credential_env="SOURCE_A_KEY",
                  credential_version="v1", quota_scope="shared-account",
                  max_attempts_per_window=10, quota_window_seconds=60,
                  max_concurrency=4)
    values.update(changes)
    return SourceProfile(**values)


class StateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "api.sqlite"
        self.store = StateStore(self.path)
        self.addCleanup(self.store.close)

    def test_shared_quota_counts_all_attempts_and_different_keys(self):
        first = profile(max_attempts_per_window=2)
        second = replace(first, source_id="source-b", credential_env="SOURCE_B_KEY")
        self.store.release(self.store.acquire(first, "a1", 100), 100)
        self.store.release(self.store.acquire(second, "b1", 100), 100)
        with self.assertRaisesRegex(StateRejected, "quota") as caught:
            self.store.acquire(first, "a2", 100)
        self.assertEqual(caught.exception.retry_after_seconds, 60)
        self.store.acquire(first, "a3", 160)

    def test_atomic_acquisition_under_concurrent_threads(self):
        value = profile(max_attempts_per_window=3)
        barrier = threading.Barrier(8)
        results = []
        lock = threading.Lock()

        def run(number):
            barrier.wait(timeout=5)
            try:
                self.store.acquire(value, "job-" + str(number), 100)
                outcome = "accepted"
            except StateRejected as error:
                outcome = error.code
            with lock:
                results.append(outcome)

        threads = [threading.Thread(target=run, args=(number,)) for number in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=8)
            self.assertFalse(thread.is_alive())
        self.assertEqual(results.count("accepted"), 3)
        self.assertEqual(results.count("quota"), 5)

    def test_independent_process_observes_consumed_quota(self):
        value = profile(max_attempts_per_window=1)
        self.store.release(self.store.acquire(value, "first", 100), 100)
        script = """import sys
from ingestion.api_profiles import SourceProfile
from ingestion.api_state import StateStore, StateRejected
p=SourceProfile(source_id='source-a',account_ref='a',environment='test',endpoint='https://a.example.test/query',credential_env='SOURCE_A_KEY',credential_version='v1',quota_scope='shared-account',max_attempts_per_window=1,quota_window_seconds=60,max_concurrency=4)
try:
    StateStore(sys.argv[1]).acquire(p,'child',100)
except StateRejected as error:
    print(error.code)
else:
    raise SystemExit(3)
"""
        result = subprocess.run([sys.executable, "-B", "-c", script, str(self.path)], cwd=ROOT,
                                capture_output=True, text=True, timeout=10)
        self.assertEqual((result.returncode, result.stdout.strip()), (0, "quota"), result.stderr)

    def test_cooldown_survives_restart_and_does_not_block_independent_source(self):
        first = profile()
        lease = self.store.acquire(first, "a1", 100)
        self.store.failure(lease, 101, category="quota", retry_after_seconds=30)
        reloaded = StateStore(self.path)
        self.addCleanup(reloaded.close)
        with self.assertRaisesRegex(StateRejected, "cooldown") as caught:
            reloaded.acquire(first, "a2", 110)
        self.assertEqual(caught.exception.retry_after_seconds, 21)
        independent = profile(source_id="b", quota_scope="independent")
        reloaded.acquire(independent, "b1", 110)
        reloaded.acquire(first, "a3", 131)

    def test_rotation_never_resets_quota(self):
        first = profile(max_attempts_per_window=1)
        self.store.release(self.store.acquire(first, "a1", 100), 100)
        changed = replace(first, credential_version="v2", config_version="2")
        with self.assertRaisesRegex(StateRejected, "quota"):
            self.store.acquire(changed, "a2", 101)

    def test_shared_scope_requires_consistent_policy(self):
        first = profile()
        self.store.acquire(first, "a1", 100)
        with self.assertRaisesRegex(StateRejected, "quota_policy_mismatch"):
            self.store.acquire(replace(first, max_attempts_per_window=100), "a2", 100)

    def test_equivalent_integer_and_float_windows_share_one_policy(self):
        integer = profile(quota_window_seconds=60, max_attempts_per_window=1)
        decimal = profile(quota_window_seconds=60.0, max_attempts_per_window=1)
        self.assertEqual(integer.config_hash, decimal.config_hash)
        self.store.release(self.store.acquire(integer, "a", 100), 100)
        with self.assertRaisesRegex(StateRejected, "quota$"):
            self.store.acquire(decimal, "b", 100)

    def test_cost_is_reserved_without_refund_on_cancellation(self):
        value = profile(max_attempts_per_window=3)
        lease = self.store.acquire(value, "first", 100, cost=3)
        self.store.release(lease, 100)
        with self.assertRaisesRegex(StateRejected, "quota"):
            self.store.acquire(value, "second", 101)

    def test_capacity_recovers_after_lease_expiry_but_late_result_fails(self):
        value = profile(max_concurrency=1)
        old = self.store.acquire(value, "first", 100, lease_seconds=10)
        with self.assertRaisesRegex(StateRejected, "capacity"):
            self.store.acquire(value, "second", 109)
        new = self.store.acquire(value, "third", 110)
        with self.assertRaisesRegex(StateRejected, "late_result"):
            self.store.success(old, 111)
        self.store.success(new, 111, rows=2)
        self.assertEqual(self.store.health(value.source_id)["accepted_rows"], 2)

    def test_global_capacity_persists_and_inconsistent_limit_fails(self):
        for number in range(4):
            self.store.acquire(profile(source_id="s" + str(number), quota_scope="q" + str(number)), "job", 100)
        with self.assertRaisesRegex(StateRejected, "capacity"):
            self.store.acquire(profile(source_id="s5", quota_scope="q5"), "job", 100)
        with self.assertRaisesRegex(StateRejected, "capacity_policy_mismatch"):
            StateStore(self.path, max_workers=50)

    def test_circuit_opens_then_admits_only_one_probe(self):
        value = profile(circuit_failure_threshold=2, circuit_reset_seconds=10)
        for number in (0, 1):
            lease = self.store.acquire(value, "bad-" + str(number), 100 + number)
            self.store.failure(lease, 100 + number, category="transport", transient=True)
        with self.assertRaisesRegex(StateRejected, "circuit_open"):
            self.store.acquire(value, "too-early", 110)
        probe = self.store.acquire(value, "probe", 111)
        with self.assertRaisesRegex(StateRejected, "circuit_probe_running"):
            self.store.acquire(value, "probe-2", 111)
        self.store.success(probe, 112, rows=1)
        self.store.acquire(value, "normal", 112)

    def test_circuit_state_persists_and_cancelled_probe_does_not_leak(self):
        value = profile(circuit_failure_threshold=1, circuit_reset_seconds=5)
        lease = self.store.acquire(value, "bad", 100)
        self.store.failure(lease, 100, category="transport", transient=True)
        other = StateStore(self.path)
        self.addCleanup(other.close)
        with self.assertRaisesRegex(StateRejected, "circuit_open"):
            other.acquire(value, "early", 101)
        probe = other.acquire(value, "probe", 105)
        other.release(probe, 105)
        other.acquire(value, "new-probe", 105)

    def test_older_success_does_not_close_newly_opened_circuit(self):
        value = profile(circuit_failure_threshold=1, circuit_reset_seconds=10)
        old = self.store.acquire(value, "old", 100)
        failing = self.store.acquire(value, "failing", 100)
        self.store.failure(failing, 101, category="transport", transient=True)
        self.store.success(old, 102, rows=1)
        self.assertEqual(self.store.health(value.source_id)["status"], "failed")
        with self.assertRaisesRegex(StateRejected, "circuit_open"):
            self.store.acquire(value, "new", 103)

    def test_health_deduplicates_failure_and_reports_recovery(self):
        value = profile()
        first = self.store.acquire(value, "a", 100)
        self.assertTrue(self.store.failure(first, 100, category="permission"))
        second = self.store.acquire(value, "b", 101)
        self.assertFalse(self.store.failure(second, 101, category="permission"))
        good = self.store.acquire(value, "c", 102)
        self.assertTrue(self.store.success(good, 102, rows=3, data_age_seconds=10))
        health = self.store.health(value.source_id)
        self.assertEqual((health["status"], health["last_success"], health["last_failure"]), ("healthy", 102, 101))
        self.assertEqual((health["accepted_rows"], health["rejected_jobs"], health["transitions"]), (3, 2, 2))
        self.assertEqual(health["active_jobs"], 0)

    def test_silence_stale_data_and_recovery_are_observable_without_alarm_storm(self):
        self.assertTrue(self.store.observe("source-a", "stale", 100, category="stale_data"))
        self.assertFalse(self.store.observe("source-a", "stale", 101, category="stale_data"))
        self.assertTrue(self.store.observe("source-a", "silent", 102, category="missing_data"))
        self.assertTrue(self.store.observe("source-a", "healthy", 103))
        self.assertEqual(self.store.health("source-a")["transitions"], 3)

    def test_storage_health_tracks_commit_separately_and_deduplicates_failure(self):
        value = profile()
        lease = self.store.acquire(value, 'job:0', 100)
        self.store.success(lease, 100, rows=1, request_id='job')
        pending = self.store.health(value.source_id)
        self.assertEqual((pending['status'], pending['storage_status'], pending['last_success_request_id']), ('healthy', 'pending', 'job'))
        self.assertTrue(self.store.record_storage_failure(value.source_id, 'job', 101))
        self.assertFalse(self.store.record_storage_failure(value.source_id, 'job', 102))
        self.assertTrue(self.store.record_storage_success(value.source_id, 'job', 103))
        health = self.store.health(value.source_id)
        self.assertEqual((health['storage_status'], health['last_storage_success'], health['last_storage_failure']), ('stored', 103, 102))
        self.assertEqual(health['status'], 'healthy')
        self.assertEqual(health['storage_transitions'], 2)
        self.assertEqual(health['storage_request_id'], 'job')

    def test_profile_version_cannot_silently_change_endpoint(self):
        value = profile()
        self.store.register(value)
        self.store.register(value)
        with self.assertRaisesRegex(StateRejected, "config_version_reused"):
            self.store.register(replace(value, endpoint="https://b.example.test/query"))
        self.store.register(replace(value, endpoint="https://b.example.test/query", config_version="2"))

    def test_large_clock_rollback_fails_closed_small_call_reordering_is_safe(self):
        value = profile()
        lease = self.store.acquire(value, "a", 100)
        with self.assertRaisesRegex(StateRejected, "clock_regression"):
            self.store.acquire(value, "b", 98)
        self.store.acquire(value, "c", 99.9)
        self.store.release(lease, 100)

    def test_cancellation_releases_capacity_once_and_cannot_later_succeed(self):
        value = profile(max_concurrency=1)
        lease = self.store.acquire(value, "a", 100)
        self.store.release(lease, 101)
        self.store.release(lease, 101)
        with self.assertRaisesRegex(StateRejected, "late_result"):
            self.store.success(lease, 102)
        self.store.acquire(value, "b", 102)

    def test_inflight_revocation_persists_through_new_store_and_late_result_check(self):
        value = profile()
        credentials = CredentialStore(self.store)
        snapshot = credentials.install(value, {"SOURCE_A_KEY": "FAKE-REVOCATION-KEY"})
        lease = self.store.acquire(value, "a", 100)
        separate = CredentialStore(StateStore(self.path))
        separate.revoke(value.source_id, "v1", now=101)
        with self.assertRaisesRegex(ProfileError, "credential_revoked"):
            credentials.assert_active(snapshot, 102)
        self.store.release(lease, 102)
        with self.assertRaisesRegex(ProfileError, "credential_revoked"):
            separate.install(value, {"SOURCE_A_KEY": "FAKE-REVOCATION-KEY"})
        self.assertNotIn(b"FAKE-REVOCATION-KEY", self.path.read_bytes())

    def test_composed_quota_rotation_restart_and_old_response(self):
        value = profile()
        credentials = CredentialStore(self.store)
        old = credentials.install(value, {"SOURCE_A_KEY": "FAKE-OLD"})
        older_job = self.store.acquire(value, "old", 100)
        limited = self.store.acquire(value, "limit", 100)
        self.store.failure(limited, 101, category="quota", retry_after_seconds=20)
        updated = credentials.rotate(value, "v2", {"SOURCE_A_KEY": "FAKE-NEW"}, now=102)
        credentials.revoke(value.source_id, "v1", now=102)
        restarted = StateStore(self.path)
        self.addCleanup(restarted.close)
        with self.assertRaisesRegex(StateRejected, "cooldown"):
            restarted.acquire(updated, "new", 110)
        with self.assertRaisesRegex(ProfileError, "credential_revoked"):
            credentials.assert_active(old, 110)
        restarted.release(older_job, 110)
        latest = restarted.acquire(updated, "new", 121)
        restarted.success(latest, 122, rows=1)
        self.assertEqual(restarted.health(value.source_id)["accepted_rows"], 1)

    def test_closed_store_and_bad_path_fail_without_path_disclosure(self):
        self.store.close()
        with self.assertRaisesRegex(StateRejected, "closed"):
            self.store.acquire(profile(), "a", 100)
        bad = Path(self.temp.name) / "missing-secret-directory" / "api.sqlite"
        with self.assertRaises(StateRejected) as caught:
            StateStore(bad)
        self.assertNotIn("missing-secret-directory", str(caught.exception))

    @unittest.skipIf(os.name == 'nt', 'POSIX mode protection; Windows uses directory ACLs')
    def test_database_and_actual_transaction_journal_have_private_mode(self):
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT INTO settings VALUES ('permission-test','value')")
            journal = Path(str(self.path) + '-journal')
            self.assertTrue(journal.is_file())
            self.assertEqual(stat.S_IMODE(journal.stat().st_mode), 0o600)
            db.rollback()

    @unittest.skipIf(os.name == 'nt', 'POSIX mode protection; Windows uses directory ACLs')
    def test_group_readable_database_and_sidecars_fail_closed(self):
        os.chmod(self.path, 0o640)
        with self.assertRaisesRegex(StateRejected, 'state_permissions'):
            StateStore(self.path)
        os.chmod(self.path, 0o600)
        sidecar = Path(str(self.path) + '-journal')
        sidecar.write_bytes(b'placeholder')
        os.chmod(sidecar, 0o644)
        try:
            with self.assertRaisesRegex(StateRejected, 'state_permissions'):
                StateStore(self.path)
        finally:
            sidecar.unlink()


if __name__ == "__main__":
    unittest.main()
