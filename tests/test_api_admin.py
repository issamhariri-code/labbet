"""Operator commands use the real temporary SQLite state and controlled time."""

from contextlib import closing, redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))
from api_admin import main
from api_profiles import CredentialStore, ProfileError, SourceProfile
from api_state import StateStore


class AdminTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "state.sqlite"
        self.state = StateStore(self.path)
        self.addCleanup(self.state.close)
        self.profile = SourceProfile(source_id="test-source", account_ref="test-account",
            environment="test", endpoint="https://test.example.test/query",
            credential_env="TEST_KEY", credential_version="v1", quota_scope="test-account",
            max_attempts_per_window=20, verification_reference="operator-review-2026-10")
        self.state.register(self.profile)

    def invoke(self, arguments, now=100, path=None):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(["--state", str(self.path if path is None else path)] + arguments, wall=lambda: now)
        self.assertEqual(stderr.getvalue(), "")
        return code, json.loads(stdout.getvalue()), stdout.getvalue()

    def successful_fetch(self, request_id, now=100, age=0, *, stored=True):
        lease = self.state.acquire(self.profile, request_id, now)
        self.state.success(lease, now, rows=2, data_age_seconds=age)
        if stored:
            self.state.record_storage_success(self.profile.source_id, request_id, now)

    def health(self, now):
        return self.invoke(["health", "--source", "test-source", "--max-silence", "10", "--max-data-age", "20"], now)

    def test_stale_health_deduplicates_then_success_reports_recovery(self):
        first = self.health(100)
        second = self.health(101)
        self.assertEqual((first[0], first[1]['stale'], first[1]['changed']), (1, True, True))
        self.assertEqual((second[0], second[1]['stale'], second[1]['changed']), (1, True, False))
        self.successful_fetch("recovery", 102)
        recovered = self.health(103)
        self.assertEqual((recovered[0], recovered[1]['healthy']), (0, True))
        self.assertEqual(recovered[1]['health']['status'], 'healthy')

    def test_data_age_stops_healthy_status_despite_recent_download(self):
        self.successful_fetch("old-data", 100, age=30)
        code, result, _ = self.health(101)
        self.assertEqual(code, 1)
        self.assertTrue(result['stale'])
        self.assertFalse(result['healthy'])

    def test_recent_permission_failure_is_unhealthy_even_after_recent_success(self):
        self.successful_fetch("good", 100)
        failing = self.state.acquire(self.profile, "bad", 101)
        self.state.failure(failing, 101, category="permission_unknown")
        code, result, _ = self.health(102)
        self.assertEqual(code, 1)
        self.assertFalse(result['stale'])
        self.assertFalse(result['healthy'])

    def test_successful_fetch_with_failed_database_is_not_green(self):
        self.successful_fetch("bad-db", 100, stored=False)
        self.state.record_storage_failure(self.profile.source_id, "bad-db", 100)
        code, result, _ = self.health(101)
        self.assertEqual(code, 1)
        self.assertFalse(result['healthy'])
        self.assertFalse(result['storage_healthy'])
        self.assertEqual(result['health']['status'], 'healthy')  # fetch succeeded
        self.assertEqual(result['health']['storage_status'], 'failed')
        self.successful_fetch("recovered", 102)
        self.assertEqual(self.health(103)[0], 0)

    def test_new_fetch_without_storage_outcome_cannot_reuse_previous_green(self):
        self.successful_fetch("old", 100)
        self.successful_fetch("new", 101, stored=False)
        code, result, _ = self.health(102)
        self.assertEqual(code, 1)
        self.assertEqual(result['health']['storage_status'], 'pending')
        self.assertNotEqual(result['health']['last_success_request_id'], result['health']['storage_request_id'])
        self.state.record_storage_success(self.profile.source_id, "new", 103)
        self.assertEqual(self.health(103)[0], 0)

    def test_old_reverse_order_commit_cannot_impersonate_latest_fetch(self):
        self.successful_fetch("old", 100, stored=False)
        self.successful_fetch("new", 101)
        self.state.record_storage_success(self.profile.source_id, "old", 102)
        code, result, _ = self.health(103)
        self.assertEqual(code, 1)
        self.assertFalse(result['storage_healthy'])

    def test_revocation_persists_and_blocks_reloaded_credentials_before_fetch(self):
        store = CredentialStore(self.state)
        snapshot = store.install(self.profile, {"TEST_KEY": "FAKE-NEVER-LOG-KEY"})
        self.assertEqual(self.invoke(["revoke", "--source", "test-source", "--version", "v1"])[0], 0)
        with self.assertRaisesRegex(ProfileError, "credential_revoked"):
            store.assert_active(snapshot, 101)
        reopened_state = StateStore(self.path)
        self.addCleanup(reopened_state.close)
        with self.assertRaisesRegex(ProfileError, "credential_revoked"):
            CredentialStore(reopened_state).install(self.profile, {"TEST_KEY": "FAKE-NEVER-LOG-KEY"})

    def test_inventory_has_reviewed_config_versions_but_no_key_values(self):
        CredentialStore(self.state).install(self.profile, {"TEST_KEY": "FAKE-NEVER-LOG-KEY"})
        code, result, raw = self.invoke(["inventory"])
        self.assertEqual(code, 0)
        self.assertNotIn("FAKE-NEVER-LOG-KEY", raw)
        row = result['sources'][0]
        self.assertEqual(row['config_hash'], self.profile.config_hash)
        self.assertEqual(row['profile']['verification_reference'], "operator-review-2026-10")
        self.assertEqual(row['profile']['credential_env'], 'TEST_KEY')  # reference, never value
        with closing(sqlite3.connect(self.path)) as db, db:
            fingerprint = db.execute('SELECT fingerprint FROM credentials').fetchone()[0]
            salt = db.execute("SELECT value FROM settings WHERE name='credential_salt'").fetchone()[0]
        self.assertNotIn(fingerprint, raw)
        self.assertNotIn(salt, raw)

    def test_inventory_rejects_corrupted_metadata_before_printing_it(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE profiles SET metadata=?", ('{"secret":"FAKE-NEVER-LOG-KEY"}',))
        code, result, raw = self.invoke(["inventory"])
        self.assertEqual((code, result['error']), (1, 'invalid_inventory'))
        self.assertNotIn("FAKE-NEVER-LOG-KEY", raw)

    def test_missing_file_is_not_created_and_relative_path_is_rejected(self):
        missing = Path(self.temp.name) / "secret-path.sqlite"
        code, result, raw = self.invoke(["inventory"], path=missing)
        self.assertEqual((code, result['error']), (1, 'existing_state_required'))
        self.assertFalse(missing.exists())
        self.assertNotIn("secret-path", raw)
        self.assertEqual(self.invoke(["inventory"], path="relative-state.sqlite")[1]['error'], 'existing_state_required')

    def test_unrelated_sqlite_file_is_not_initialized_as_api_state(self):
        unrelated = Path(self.temp.name) / "unrelated.sqlite"
        with closing(sqlite3.connect(unrelated)) as db, db:
            db.execute("CREATE TABLE unrelated(value TEXT)")
        self.assertEqual(self.invoke(["inventory"], path=unrelated)[0], 1)
        with closing(sqlite3.connect(unrelated)) as db, db:
            tables = db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        self.assertEqual(tables, [('unrelated',)])

    def test_bad_command_and_arguments_never_echo_their_values(self):
        cases = [["FAKE-NEVER-LOG-KEY"], ["health", "--source", "test-source"],
                 ["inventory", "--unknown", "FAKE-NEVER-LOG-KEY"],
                 ["health", "--source", "source\nFAKE-NEVER-LOG-KEY", "--max-silence", "1", "--max-data-age", "1"],
                 ["health", "--source", "FAKE-NEVER-LOG-KEY", "--max-silence", "1", "--max-data-age", "1"],
                 ["revoke", "--source", "test-source", "--version", "FAKE-NEVER-LOG-KEY"],
                 ["health", "--source", "test-source", "--max-silence", "nan", "--max-data-age", "1"]]
        for arguments in cases:
            with self.subTest(arguments=arguments):
                code, result, raw = self.invoke(arguments)
                self.assertEqual(code, 1)
                self.assertIn('error', result)
                self.assertNotIn("FAKE-NEVER-LOG-KEY", raw)

    def test_admin_preserves_custom_global_capacity(self):
        path = Path(self.temp.name) / "custom.sqlite"
        custom = StateStore(path, max_workers=2)
        self.addCleanup(custom.close)
        self.assertEqual(self.invoke(["inventory"], path=path)[0], 0)
        with closing(sqlite3.connect(path)) as db, db:
            self.assertEqual(db.execute("SELECT value FROM settings WHERE name='max_workers'").fetchone()[0], '2')


if __name__ == '__main__':
    unittest.main()
