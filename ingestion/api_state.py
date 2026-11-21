"""Durable API coordination. SQLite stores identities and outcomes, never keys.

Every admission uses BEGIN IMMEDIATE: different threads/processes share the same
quota and worker count. Wall-clock rollback fails closed. Expired worker leases
recover capacity after a crash but never refund already-spent attempt quota.
"""

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
import uuid

try:
    from .api_profiles import ProfileError, SourceProfile, quota_limits_from_json
except ImportError:
    from api_profiles import ProfileError, SourceProfile, quota_limits_from_json


class StateRejected(RuntimeError):
    def __init__(self, code, retry_after_seconds=0):
        self.code = code
        self.retry_after_seconds = max(0.0, float(retry_after_seconds))
        super().__init__("API state rejected: " + code)


def _finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _label(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", value)


def _identity(values):
    return hashlib.sha256(json.dumps(values, separators=(",", ":")).encode()).hexdigest()


def _quota_policy(limits, max_concurrency):
    if not limits:
        raise StateRejected("unsupported_quota_policy")

    ordered = sorted(
        limits, key=lambda limit: (limit.window_seconds, limit.max_credits)
    )
    windows = [(limit.max_credits, limit.window_seconds) for limit in ordered]

    if len(windows) == 1:
        payload = (*windows[0], max_concurrency)
    else:
        payload = {
            "version": 2,
            "limits": windows,
            "max_concurrency": max_concurrency,
        }

    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    )

@dataclass(frozen=True, slots=True)
class Lease:
    token: str
    source_id: str
    request_id: str
    quota_scope: str
    expires_at: float
    circuit_failure_threshold: int
    circuit_reset_seconds: float


class StateStore:
    def __init__(self, path, *, max_workers=4, busy_timeout_seconds=2.0, existing_only=False):
        if type(max_workers) is not int or not 1 <= max_workers <= 10000:
            raise StateRejected("invalid_capacity")
        if not _finite(busy_timeout_seconds) or not 0 < busy_timeout_seconds <= 30:
            raise StateRejected("invalid_busy_timeout")
        self.path = str(path)
        if self.path == ":memory:":
            raise StateRejected("persistent_state_required")
        self.max_workers = max_workers
        self.busy_timeout_seconds = busy_timeout_seconds
        self._existing_only = existing_only
        self._closed = False
        # Parent directory is deliberately operator-managed, never silently /tmp.
        try:
            if not existing_only:
                try:
                    descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
                except FileExistsError:
                    pass
                else:
                    os.close(descriptor)
            self._check_permissions()
            db = self._connect()
            try:
                if existing_only:
                    marker = db.execute("SELECT value FROM settings WHERE name='api_schema_version'").fetchone()
                    if marker is None or marker[0] != '1':
                        raise StateRejected("state_schema")
                db.executescript("""
                    CREATE TABLE IF NOT EXISTS settings (name TEXT PRIMARY KEY, value TEXT NOT NULL);
                    CREATE TABLE IF NOT EXISTS profiles (
                        source_id TEXT NOT NULL, version TEXT NOT NULL,
                        fingerprint TEXT NOT NULL, metadata TEXT NOT NULL,
                        PRIMARY KEY(source_id, version));
                    CREATE TABLE IF NOT EXISTS credentials (
                        identity TEXT NOT NULL, source_id TEXT NOT NULL,
                        version TEXT NOT NULL, expires_at REAL,
                        revoked_at REAL, fingerprint TEXT NOT NULL,
                        PRIMARY KEY(identity, version));
                    CREATE TABLE IF NOT EXISTS revocations (
                        source_id TEXT NOT NULL, version TEXT NOT NULL,
                        revoked_at REAL NOT NULL, PRIMARY KEY(source_id,version));
                    CREATE TABLE IF NOT EXISTS active_credentials (
                        identity TEXT PRIMARY KEY, version TEXT NOT NULL);
                    CREATE TABLE IF NOT EXISTS scopes (
                        scope TEXT PRIMARY KEY, policy TEXT NOT NULL,
                        cooldown_until REAL NOT NULL DEFAULT 0);
                    CREATE TABLE IF NOT EXISTS attempts (
                        scope TEXT NOT NULL, attempted_at REAL NOT NULL, cost INTEGER NOT NULL);
                    CREATE INDEX IF NOT EXISTS attempts_scope_time ON attempts(scope, attempted_at);
                    CREATE TABLE IF NOT EXISTS circuits (
                        source_id TEXT PRIMARY KEY, failures INTEGER NOT NULL DEFAULT 0,
                        open_until REAL NOT NULL DEFAULT 0, probe_token TEXT,
                        generation INTEGER NOT NULL DEFAULT 0);
                    CREATE TABLE IF NOT EXISTS jobs (
                        token TEXT PRIMARY KEY, source_id TEXT NOT NULL,
                        request_id TEXT NOT NULL, scope TEXT NOT NULL,
                        status TEXT NOT NULL, started_at REAL NOT NULL,
                        ended_at REAL, expires_at REAL NOT NULL,
                        category TEXT, rows_accepted INTEGER NOT NULL DEFAULT 0,
                        circuit_generation INTEGER NOT NULL DEFAULT 0);
                    CREATE INDEX IF NOT EXISTS jobs_active ON jobs(status, expires_at);
                    CREATE TABLE IF NOT EXISTS health (
                        source_id TEXT PRIMARY KEY, status TEXT NOT NULL,
                        changed_at REAL NOT NULL, last_success REAL,
                        last_failure REAL, category TEXT,
                        accepted_rows INTEGER NOT NULL DEFAULT 0,
                        rejected_jobs INTEGER NOT NULL DEFAULT 0,
                        data_age_seconds REAL,
                        transitions INTEGER NOT NULL DEFAULT 0,
                        last_success_request_id TEXT,
                        last_storage_success REAL,
                        last_storage_failure REAL,
                        storage_status TEXT,
                        storage_request_id TEXT,
                        storage_transitions INTEGER NOT NULL DEFAULT 0);
                """)
            finally:
                db.close()
            with self._transaction() as db:
                previous = db.execute("SELECT value FROM settings WHERE name='max_workers'").fetchone()
                if previous is not None and int(previous[0]) != max_workers and not existing_only:
                    raise StateRejected("capacity_policy_mismatch")
                if existing_only and previous is not None:
                    self.max_workers = int(previous[0])
                db.execute("INSERT OR IGNORE INTO settings VALUES ('max_workers', ?)", (str(max_workers),))
                db.execute("INSERT OR IGNORE INTO settings VALUES ('api_schema_version', '1')")
                db.execute("INSERT OR IGNORE INTO settings VALUES ('credential_salt', ?)", (secrets.token_hex(32),))
        except (sqlite3.Error, OSError):
            raise StateRejected("state_unavailable") from None

    def _check_permissions(self):
        # POSIX SQLite journals inherit database mode. Check existing sidecars
        # as well; never silently relax permissions or expose derived key hashes.
        if os.name != 'nt':
            for suffix in ('', '-wal', '-shm', '-journal'):
                path = Path(self.path + suffix)
                if path.exists() and stat.S_IMODE(path.stat().st_mode) & 0o077:
                    raise StateRejected("state_permissions")

    def _connect(self):
        if self._closed:
            raise StateRejected("closed")
        self._check_permissions()
        if self._existing_only:
            location = Path(self.path).resolve().as_uri() + "?mode=rw"
            db = sqlite3.connect(location, uri=True, timeout=self.busy_timeout_seconds, isolation_level=None)
        else:
            db = sqlite3.connect(self.path, timeout=self.busy_timeout_seconds, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
        return db

    @contextmanager
    def _transaction(self):
        db = None
        try:
            db = self._connect()
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except sqlite3.Error:
            if db is not None:
                db.rollback()
            raise StateRejected("state_unavailable") from None
        except BaseException:
            if db is not None:
                db.rollback()
            raise
        finally:
            if db is not None:
                db.close()

    @staticmethod
    def _clock(db, now):
        if not _finite(now) or now < 0:
            raise StateRejected("invalid_clock")
        row = db.execute("SELECT value FROM settings WHERE name='clock_floor'").fetchone()
        # Concurrent callers can take their timestamps in a different order
        # from SQLite's write lock. Small reordering cannot shorten any pause.
        if row is not None and now < float(row[0]):
            if float(row[0]) - now > 1:
                raise StateRejected("clock_regression")
            now = float(row[0])
        db.execute("INSERT INTO settings VALUES ('clock_floor', ?) ON CONFLICT(name) DO UPDATE SET value=excluded.value", (str(now),))

    @staticmethod
    def _recover(db, now):
        db.execute("UPDATE jobs SET status='abandoned', ended_at=?, category='lease_expired' WHERE status='running' AND expires_at<=?", (now, now))
        db.execute("UPDATE circuits SET probe_token=NULL WHERE probe_token IS NOT NULL AND NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.token=circuits.probe_token AND jobs.status='running')")

    @staticmethod
    def _observe(db, source_id, status, now, category=None):
        existing = db.execute("SELECT status,category FROM health WHERE source_id=?", (source_id,)).fetchone()
        changed = existing is None or existing[0] != status or existing[1] != category
        if existing is None:
            db.execute("INSERT INTO health(source_id,status,changed_at,category,transitions) VALUES (?,?,?,?,1)", (source_id, status, now, category))
        elif changed:
            db.execute("UPDATE health SET status=?,changed_at=?,category=?,transitions=transitions+1 WHERE source_id=?", (status, now, category, source_id))
        return changed

    def register(self, profile):
        metadata = json.dumps(profile.metadata(), sort_keys=True, separators=(",", ":"), allow_nan=False)
        with self._transaction() as db:
            old = db.execute("SELECT fingerprint FROM profiles WHERE source_id=? AND version=?", (profile.source_id, profile.config_version)).fetchone()
            if old is not None and old[0] != profile.config_hash:
                raise StateRejected("config_version_reused")
            db.execute("INSERT OR IGNORE INTO profiles VALUES (?,?,?,?)", (profile.source_id, profile.config_version, profile.config_hash, metadata))

    def register_credential(self, profile, *, secret, expires_at=None):
        if not isinstance(secret, str) or not secret:
            raise ProfileError("invalid_credential")
        identity = _identity(profile.credential_identity)
        with self._transaction() as db:
            salt = db.execute("SELECT value FROM settings WHERE name='credential_salt'").fetchone()
            if salt is None or not isinstance(salt[0], str) or not re.fullmatch('[a-f0-9]{64}', salt[0]):
                raise StateRejected("credential_metadata_invalid")
            message = json.dumps((profile.credential_identity, profile.credential_version, secret), ensure_ascii=True, separators=(',', ':')).encode()
            fingerprint = hmac.new(bytes.fromhex(salt[0]), message, hashlib.sha256).hexdigest()
            if db.execute("SELECT 1 FROM revocations WHERE source_id=? AND version=?", (profile.source_id, profile.credential_version)).fetchone():
                raise ProfileError("credential_revoked")
            old = db.execute("SELECT expires_at,revoked_at,fingerprint FROM credentials WHERE identity=? AND version=?", (identity, profile.credential_version)).fetchone()
            if old is not None:
                if old[1] is not None:
                    raise ProfileError("credential_revoked")
                if old[0] != expires_at or not hmac.compare_digest(old[2], fingerprint):
                    raise ProfileError("credential_version_reused")
            db.execute("INSERT OR IGNORE INTO credentials VALUES (?,?,?,?,NULL,?)", (identity, profile.source_id, profile.credential_version, expires_at, fingerprint))
            if old is None:
                db.execute("INSERT INTO active_credentials VALUES (?,?) ON CONFLICT(identity) DO UPDATE SET version=excluded.version", (identity, profile.credential_version))
            else:
                db.execute("INSERT OR IGNORE INTO active_credentials VALUES (?,?)", (identity, profile.credential_version))
            return db.execute("SELECT version FROM active_credentials WHERE identity=?", (identity,)).fetchone()[0]

    def assert_credential_active(self, identity, version, now, *, require_current=False):
        with self._transaction() as db:
            self._clock(db, now)
            row = db.execute("SELECT expires_at,revoked_at FROM credentials WHERE identity=? AND version=?", (_identity(identity), version)).fetchone()
            if row is None:
                raise ProfileError("credential_not_installed")
            if row[1] is not None:
                raise ProfileError("credential_revoked")
            if row[0] is not None and now >= row[0]:
                raise ProfileError("credential_expired")
            if require_current:
                active = db.execute("SELECT version FROM active_credentials WHERE identity=?", (_identity(identity),)).fetchone()
                if active is None or active[0] != version:
                    raise ProfileError("credential_superseded")

    def revoke_credential(self, source_id, version, now):
        with self._transaction() as db:
            self._clock(db, now)
            db.execute("INSERT INTO revocations VALUES (?,?,?) ON CONFLICT(source_id,version) DO UPDATE SET revoked_at=MIN(revoked_at,excluded.revoked_at)", (source_id, version, now))
            db.execute("UPDATE credentials SET revoked_at=? WHERE source_id=? AND version=?", (now, source_id, version))

    def acquire(self, profile, request_id, wall_now, lease_seconds=60, *, cost=1):
        if not _label(request_id):
            raise StateRejected("invalid_request_id")
        if not _finite(lease_seconds) or not 0 < lease_seconds <= 3600:
            raise StateRejected("invalid_lease")
        if type(cost) is not int or cost < 1:
            raise StateRejected("invalid_cost")
        limits = profile.quota_limits
        if not limits:
            raise StateRejected("unsupported_quota_policy")

        if any(cost > limit.max_credits for limit in limits):
            raise StateRejected("cost_exceeds_quota")

        policy = _quota_policy(limits, profile.max_concurrency)
        token = uuid.uuid4().hex
        with self._transaction() as db:
            self._clock(db, wall_now)
            self._recover(db, wall_now)
            db.execute("INSERT OR IGNORE INTO scopes(scope,policy) VALUES (?,?)", (profile.quota_scope, policy))
            scope = db.execute("SELECT policy,cooldown_until FROM scopes WHERE scope=?", (profile.quota_scope,)).fetchone()
            if scope[0] != policy:
                raise StateRejected("quota_policy_mismatch")
            if scope[1] > wall_now:
                raise StateRejected("cooldown", scope[1] - wall_now)
            circuit = db.execute("SELECT * FROM circuits WHERE source_id=?", (profile.source_id,)).fetchone()
            probing = circuit is not None and circuit['failures'] >= profile.circuit_failure_threshold
            if probing and circuit['open_until'] > wall_now:
                raise StateRejected("circuit_open", circuit['open_until'] - wall_now)
            if probing and circuit['probe_token'] is not None:
                raise StateRejected("circuit_probe_running")
            if db.execute("SELECT COUNT(*) FROM jobs WHERE status='running'").fetchone()[0] >= self.max_workers:
                raise StateRejected("capacity")
            if db.execute("SELECT COUNT(*) FROM jobs WHERE status='running' AND scope=?", (profile.quota_scope,)).fetchone()[0] >= profile.max_concurrency:
                raise StateRejected("capacity")
            if db.execute("SELECT 1 FROM jobs WHERE source_id=? AND request_id=? AND status='running'", (profile.source_id, request_id)).fetchone():
                raise StateRejected("request_already_running")
            retention_start = wall_now - max(
                limit.window_seconds for limit in limits
            )
            db.execute(
                "DELETE FROM attempts WHERE scope=? AND attempted_at<=?",
                (profile.quota_scope, retention_start),
            )
            quota_delays = []
            for limit in limits:
                window_start = wall_now - limit.window_seconds
                used = db.execute(
                    "SELECT COALESCE(SUM(cost),0) "
                    "FROM attempts WHERE scope=? AND attempted_at>?",
                    (profile.quota_scope, window_start),
                ).fetchone()[0]

                if used + cost > limit.max_credits:
                    required = used + cost - limit.max_credits
                    delay = limit.window_seconds
                    for attempted_at, spent in db.execute(
                        "SELECT attempted_at, cost FROM attempts "
                        "WHERE scope=? AND attempted_at>? "
                        "ORDER BY attempted_at",
                        (profile.quota_scope, window_start),
                    ):
                        required -= spent
                        if required <= 0:
                            delay = (
                                attempted_at
                                + limit.window_seconds
                                - wall_now
                            )
                            break
                    quota_delays.append(delay)

            if quota_delays:
                raise StateRejected("quota", max(quota_delays))
            db.execute("INSERT INTO attempts VALUES (?,?,?)", (profile.quota_scope, wall_now, cost))
            generation = circuit['generation'] if circuit is not None else 0
            db.execute("INSERT INTO jobs(token,source_id,request_id,scope,status,started_at,expires_at,circuit_generation) VALUES (?,?,?,?,'running',?,?,?)", (token, profile.source_id, request_id, profile.quota_scope, wall_now, wall_now + lease_seconds, generation))
            if probing:
                db.execute("UPDATE circuits SET probe_token=? WHERE source_id=?", (token, profile.source_id))
        return Lease(token, profile.source_id, request_id, profile.quota_scope, wall_now + lease_seconds, profile.circuit_failure_threshold, profile.circuit_reset_seconds)

    @staticmethod
    def _active(db, lease):
        row = db.execute("SELECT * FROM jobs WHERE token=?", (lease.token,)).fetchone()
        if row is None or row['source_id'] != lease.source_id or row['request_id'] != lease.request_id or row['scope'] != lease.quota_scope:
            raise StateRejected("unknown_lease")
        return row

    def success(self, lease, wall_now, *, rows=0, data_age_seconds=None, request_id=None):
        if type(rows) is not int or rows < 0 or (data_age_seconds is not None and (not _finite(data_age_seconds) or data_age_seconds < 0)):
            raise StateRejected("invalid_observation")
        request_id = lease.request_id if request_id is None else request_id
        if not _label(request_id):
            raise StateRejected("invalid_request_id")
        with self._transaction() as db:
            self._clock(db, wall_now)
            job = self._active(db, lease)
            if job['status'] != 'running' or wall_now >= job['expires_at']:
                raise StateRejected("late_result")
            db.execute("UPDATE jobs SET status='succeeded',ended_at=?,rows_accepted=? WHERE token=?", (wall_now, rows, lease.token))
            circuit = db.execute("SELECT generation FROM circuits WHERE source_id=?", (lease.source_id,)).fetchone()
            current = circuit is None or circuit[0] == job['circuit_generation']
            # A late success from a job started before newer failures cannot
            # silently close a circuit that those failures opened.
            if current:
                db.execute("INSERT INTO circuits(source_id) VALUES (?) ON CONFLICT(source_id) DO UPDATE SET failures=0,open_until=0,probe_token=NULL", (lease.source_id,))
                changed = self._observe(db, lease.source_id, 'healthy', wall_now)
            else:
                changed = False
                if db.execute("SELECT 1 FROM health WHERE source_id=?", (lease.source_id,)).fetchone() is None:
                    self._observe(db, lease.source_id, 'unknown', wall_now)
            db.execute("UPDATE health SET last_success=?,accepted_rows=accepted_rows+?,data_age_seconds=?,last_success_request_id=?,storage_status='pending' WHERE source_id=?", (wall_now, rows, data_age_seconds, request_id, lease.source_id))
            return changed

    def _record_storage(self, source_id, request_id, now, *, succeeded):
        if not _label(source_id) or not _label(request_id):
            raise StateRejected("invalid_identifier")
        with self._transaction() as db:
            self._clock(db, now)
            row = db.execute("SELECT storage_status FROM health WHERE source_id=?", (source_id,)).fetchone()
            if row is None:
                self._observe(db, source_id, 'unknown', now)
            status = 'stored' if succeeded else 'failed'
            changed = row is None or row[0] != status
            column = 'last_storage_success' if succeeded else 'last_storage_failure'
            db.execute("UPDATE health SET " + column + "=?,storage_status=?,storage_request_id=?,storage_transitions=storage_transitions+? WHERE source_id=?", (now, status, request_id, int(changed), source_id))
            return changed

    def record_storage_success(self, source_id, request_id, now):
        """Call only after the outer database transaction actually committed."""
        return self._record_storage(source_id, request_id, now, succeeded=True)

    def record_storage_failure(self, source_id, request_id, now):
        return self._record_storage(source_id, request_id, now, succeeded=False)

    def failure(self, lease, wall_now, *, category, retry_after_seconds=0, transient=False):
        if not _label(category) or not _finite(retry_after_seconds) or retry_after_seconds < 0 or type(transient) is not bool:
            raise StateRejected("invalid_observation")
        with self._transaction() as db:
            self._clock(db, wall_now)
            job = self._active(db, lease)
            if job['status'] != 'running':
                return False
            db.execute("UPDATE jobs SET status='failed',ended_at=?,category=? WHERE token=?", (wall_now, category, lease.token))
            if retry_after_seconds:
                db.execute("UPDATE scopes SET cooldown_until=MAX(cooldown_until,?) WHERE scope=?", (wall_now + retry_after_seconds, lease.quota_scope))
            if transient:
                db.execute("INSERT INTO circuits(source_id,failures,generation) VALUES (?,1,1) ON CONFLICT(source_id) DO UPDATE SET failures=failures+1,probe_token=NULL,generation=generation+1", (lease.source_id,))
                count = db.execute("SELECT failures FROM circuits WHERE source_id=?", (lease.source_id,)).fetchone()[0]
                if count >= lease.circuit_failure_threshold:
                    db.execute("UPDATE circuits SET open_until=? WHERE source_id=?", (wall_now + lease.circuit_reset_seconds, lease.source_id))
            else:
                db.execute("UPDATE circuits SET probe_token=NULL WHERE source_id=? AND probe_token=?", (lease.source_id, lease.token))
            changed = self._observe(db, lease.source_id, 'failed', wall_now, category)
            db.execute("UPDATE health SET last_failure=?,rejected_jobs=rejected_jobs+1 WHERE source_id=?", (wall_now, lease.source_id))
            return changed

    def release(self, lease, wall_now):
        with self._transaction() as db:
            self._clock(db, wall_now)
            job = self._active(db, lease)
            if job['status'] == 'running':
                db.execute("UPDATE jobs SET status='cancelled',ended_at=?,category='cancelled' WHERE token=?", (wall_now, lease.token))
                db.execute("UPDATE circuits SET probe_token=NULL WHERE probe_token=?", (lease.token,))

    def observe(self, source_id, status, wall_now, *, category=None):
        if not _label(source_id) or not _label(status) or (category is not None and not _label(category)):
            raise StateRejected("invalid_observation")
        with self._transaction() as db:
            self._clock(db, wall_now)
            return self._observe(db, source_id, status, wall_now, category)

    def health(self, source_id):
        if not _label(source_id):
            raise StateRejected("invalid_source")
        with self._transaction() as db:
            row = db.execute("SELECT * FROM health WHERE source_id=?", (source_id,)).fetchone()
            if row is None:
                return None
            result = dict(row)
            result['active_jobs'] = db.execute("SELECT COUNT(*) FROM jobs WHERE source_id=? AND status='running'", (source_id,)).fetchone()[0]
            return result

    def inventory(self, *, limit=1000):
        """Validated source configuration history; no credential rows or values."""
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise StateRejected("invalid_inventory_limit")
        with self._transaction() as db:
            rows = db.execute("SELECT source_id,version,fingerprint,metadata FROM profiles ORDER BY source_id,rowid LIMIT ?", (limit + 1,)).fetchall()
        if len(rows) > limit:
            raise StateRejected("inventory_limit")
        result = []
        for row in rows:
            try:
                values = json.loads(row['metadata'])
                for name in ('allowed_runtime_ips', 'allowed_account_regions', 'allowed_endpoint_regions'):
                    if name in values:
                        values[name] = tuple(values[name])
                if "extra_quota_limits" in values:
                    values["extra_quota_limits"] = quota_limits_from_json(
                        values["extra_quota_limits"]
                    )
                profile = SourceProfile(**values)
                if (profile.source_id, profile.config_version, profile.config_hash) != (row['source_id'], row['version'], row['fingerprint']):
                    raise ValueError
            except (ValueError, TypeError, KeyError):
                raise StateRejected("invalid_inventory") from None
            result.append({"source_id": profile.source_id, "config_version": profile.config_version,
                           "config_hash": profile.config_hash, "profile": profile.metadata()})
        return result

    def known_credential(self, source_id, version):
        """Only an identity existence check; no credential metadata is returned."""
        if not _label(source_id) or not _label(version):
            raise StateRejected("invalid_identifier")
        with self._transaction() as db:
            return db.execute("SELECT 1 FROM credentials WHERE source_id=? AND version=? LIMIT 1", (source_id, version)).fetchone() is not None

    def close(self):
        # Connections are short lived; subsequent calls fail rather than reopen.
        self._closed = True
