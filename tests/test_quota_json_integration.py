"""Real JSON files, runtime loader and SQLite inventory/admission; no network."""
from contextlib import closing
from copy import deepcopy
from dataclasses import asdict
import json
import math
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from api_contracts import ApiFailure
from api_profiles import QuotaLimit
from api_runtime import load_runtime
from api_state import StateRejected, StateStore
from test_api_client import contract, profile


MISSING = object()


def rule(credits, seconds):
    return {"max_credits": credits, "window_seconds": seconds}


def invalid_json_rules():
    for value in (None, {}, "[]", 8, 0.5, True,
                  [None], [[]], ["rule"], [8], [True],
                  [{}], [{"max_credits": 8}], [{"window_seconds": 60}],
                  [dict(rule(8, 60), extra=1)]):
        yield value, "invalid_quota_limits"
    for credits in (0, -1, 100001, 10**1000, 8.0, True, "8", None):
        yield [rule(credits, 60)], "invalid_resource_limit"
    for seconds in (0, -1, 31536001, 10**1000, True, "60", None):
        yield [rule(8, seconds)], "invalid_resource_limit"
    # These travel through actual JSON text. Runtime's strict JSON decoder
    # rejects nonfinite numbers before the quota helper is reached.
    for value in (math.nan, math.inf, -math.inf):
        yield [rule(value, 60)], "nonfinite_json_number"
        yield [rule(8, value)], "nonfinite_json_number"


class QuotaJsonIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.folder = Path(self.tmp.name)
        self.counter = 0
        self.base = profile(environment="production",
                            endpoint="https://www.alphavantage.co/query",
                            rights_review_date="2000-01-01", review_due_date="2099-12-31")
        network = patch("socket.create_connection", side_effect=AssertionError("network forbidden"))
        network.start()
        self.addCleanup(network.stop)

    def registry(self, extras=MISSING, *, state_path=None, **changes):
        self.counter += 1
        values = asdict(self.base)
        values.pop("extra_quota_limits")
        values.update(changes)
        if extras is not MISSING:
            values["extra_quota_limits"] = deepcopy(extras)
        return {"version": 1, "environment": "production",
                "state_path": str(state_path or self.folder / f"state-{self.counter}.sqlite"),
                "sources": [{"adapter": "alphavantage-daily-v1", "profile": values,
                             "fetch_policy": {}, "instruments": [asdict(contract())]}]}

    def load(self, registry, **selection):
        location = self.folder / "registry.json"
        location.write_text(json.dumps(registry, default=str), encoding="utf-8")
        client, p, _ = load_runtime("TEST", {"LABBET_API_CONFIG": str(location),
                                           "KEY_A": "SYNTHETIC-KEY-A",
                                           "KEY_B": "SYNTHETIC-KEY-B"}, **selection)
        self.addCleanup(client.state.close)
        self.addCleanup(client.credentials.close)
        return client, p

    @staticmethod
    def close_client(client):
        client.credentials.close()
        client.state.close()

    @staticmethod
    def snapshot(store):
        with closing(sqlite3.connect(store.path)) as db:
            return tuple(db.iterdump())

    @staticmethod
    def query(store, sql, parameters=()):
        with closing(sqlite3.connect(store.path)) as db:
            return db.execute(sql, parameters).fetchall()

    @staticmethod
    def spend(store, p, name, now, cost=1):
        store.release(store.acquire(p, name, now, cost=cost), now)

    def reject(self, store, p, now, cost, code, delay):
        before = self.snapshot(store)
        with self.assertRaises(StateRejected) as caught:
            store.acquire(p, "denied", now, cost=cost)
        self.assertEqual(caught.exception.code, code)
        self.assertAlmostEqual(caught.exception.retry_after_seconds, delay)
        self.assertEqual(self.snapshot(store), before)

    def assert_inventory(self, store, p):
        self.assertEqual(store.inventory(), [{"source_id": p.source_id,
                         "config_version": p.config_version, "config_hash": p.config_hash,
                         "profile": p.metadata()}])
        entry = store.inventory()[0]
        self.assertEqual(entry["source_id"] + ":" + entry["config_hash"], p.cache_namespace)

    def test_old_json_without_field_and_empty_list_preserve_identity(self):
        results = []
        for extras in (MISSING, []):
            client, p = self.load(self.registry(extras))
            self.assertEqual(p.extra_quota_limits, ())
            self.assertEqual(p.metadata(), self.base.metadata())
            self.assertNotIn("extra_quota_limits", p.metadata())
            self.assertEqual(p.config_hash, self.base.config_hash)
            self.assertEqual(p.cache_namespace, self.base.cache_namespace)
            results.append((p.metadata(), p.config_hash, p.cache_namespace))
            self.close_client(client)
        self.assertEqual(results[0], results[1])

    def test_loader_preserves_all_rules_and_normalizes_order_duplicates_and_time(self):
        variants = ([rule(800, 86400)],
                    [rule(800, 86400), rule(8, 60)],
                    [rule(8, 60), rule(800, 86400)],
                    [rule(800, 86400), rule(8, 60), rule(8, 60)])
        hashes = []
        for extras in variants:
            with self.subTest(extras=extras):
                client, p = self.load(self.registry(extras))
                expected = tuple(QuotaLimit(x["max_credits"], x["window_seconds"])
                                 for x in sorted(extras, key=lambda x: (x["window_seconds"], x["max_credits"])))
                self.assertEqual(p.extra_quota_limits, expected)
                self.assertTrue(all(type(x) is QuotaLimit and type(x.window_seconds) is float
                                    for x in p.extra_quota_limits))
                self.assertEqual(p.quota_limits, (QuotaLimit(100, 60),) + expected)
                hashes.append(p.config_hash)
                self.close_client(client)
        self.assertEqual(hashes[1], hashes[2])
        self.assertEqual(len(set(hashes)), 3)

    def test_each_loaded_window_blocks_alone_with_boundary_and_full_rollback(self):
        for blocked in range(3):
            for reverse in (False, True):
                with self.subTest(blocked=blocked, reverse=reverse):
                    limits = [rule(2 if index == blocked else 100, seconds)
                              for index, seconds in enumerate((10, 60, 180))]
                    extras = limits[1:][::-1] if reverse else limits[1:]
                    client, p = self.load(self.registry(extras,
                        max_attempts_per_window=limits[0]["max_credits"], quota_window_seconds=10))
                    self.spend(client.state, p, "first", 100, 2)
                    boundary = 100 + limits[blocked]["window_seconds"]
                    self.reject(client.state, p, 101, 1, "quota", boundary - 101)
                    self.reject(client.state, p, boundary - 0.001, 1, "quota", 0.001)
                    self.spend(client.state, p, "boundary", boundary)
                    self.assertEqual(self.query(client.state, "SELECT COUNT(*) FROM jobs"), [(2,)])
                    self.close_client(client)

    def test_loaded_extra_cost_limit_rejects_before_transaction(self):
        client, p = self.load(self.registry([rule(2, 180)]))
        before = self.snapshot(client.state)
        with patch.object(client.state, "_transaction", side_effect=AssertionError("transaction forbidden")) as tx:
            with self.assertRaises(StateRejected) as caught:
                client.state.acquire(p, "too-costly", 100, cost=3)
            self.assertEqual(caught.exception.code, "cost_exceeds_quota")
            tx.assert_not_called()
        self.assertEqual(self.snapshot(client.state), before)

    def test_loaded_duplicates_charge_only_once_and_share_scope_across_sources(self):
        registry = self.registry([rule(3, 60), rule(5, 180), rule(3, 60)],
                                 max_attempts_per_window=100, quota_window_seconds=10)
        other = deepcopy(registry["sources"][0])
        other["profile"].update(source_id="source-b", credential_env="KEY_B")
        other["profile"]["extra_quota_limits"].reverse()
        registry["sources"].append(other)
        first, p = self.load(registry, source_id="source-a")
        second, q = self.load(registry, source_id="source-b")
        self.spend(first.state, p, "first", 100, 2)
        self.spend(second.state, q, "second", 100, 1)
        self.reject(first.state, p, 101, 1, "quota", 59)
        self.reject(second.state, q, 101, 1, "quota", 59)
        self.assertEqual(self.query(first.state, "SELECT COUNT(*),SUM(cost) FROM attempts"), [(2, 3)])
        self.assertEqual(self.query(first.state, "SELECT COUNT(*) FROM scopes"), [(1,)])

    def test_invalid_loader_json_has_controlled_codes_and_creates_no_state(self):
        for index, (extras, code) in enumerate(invalid_json_rules()):
            with self.subTest(index=index, code=code):
                registry = self.registry(extras)
                with self.assertRaises(ApiFailure) as caught:
                    self.load(registry)
                self.assertEqual(caught.exception.code, code)
                self.assertFalse(Path(registry["state_path"]).exists())

    def test_invalid_loader_json_preserves_existing_state_after_valid_source(self):
        good = self.registry([rule(3, 60)])
        client, p = self.load(good)
        client.state.register(p)
        self.spend(client.state, p, "existing", 100, 2)
        before = self.snapshot(client.state)
        for index, (extras, code) in enumerate(invalid_json_rules()):
            with self.subTest(index=index, code=code):
                registry = deepcopy(good)
                bad = deepcopy(registry["sources"][0])
                bad["profile"].update(source_id="source-b", credential_env="KEY_B", extra_quota_limits=extras)
                registry["sources"].append(bad)
                with self.assertRaises(ApiFailure) as caught:
                    self.load(registry, source_id="source-a")
                self.assertEqual(caught.exception.code, code)
                self.assertEqual(self.snapshot(client.state), before)

    def test_inventory_accepts_old_missing_and_explicit_empty_json(self):
        for present in (False, True):
            with self.subTest(present=present):
                client, p = self.load(self.registry())
                client.state.register(p)
                if present:
                    values = json.loads(json.dumps(p.metadata()))
                    values["extra_quota_limits"] = []
                    with closing(sqlite3.connect(client.state.path)) as db:
                        db.execute("UPDATE profiles SET metadata=?", (json.dumps(values),))
                        db.commit()
                before = self.snapshot(client.state)
                self.assert_inventory(client.state, p)
                self.assertEqual(self.snapshot(client.state), before)
                path = client.state.path
                self.close_client(client)
                reopened = StateStore(path, existing_only=True)
                self.addCleanup(reopened.close)
                self.assert_inventory(reopened, p)
                self.assertEqual(self.snapshot(reopened), before)

    def test_profile_json_inventory_round_trip_survives_reopen(self):
        for extras in ([rule(800, 86400)],
                       [rule(800, 86400), rule(8, 60)],
                       [rule(8, 60), rule(800, 86400), rule(8, 60)]):
            with self.subTest(extras=extras):
                client, p = self.load(self.registry(extras))
                store = client.state
                store.register(p)
                raw = self.query(store, "SELECT metadata FROM profiles")[0][0]
                decoded = json.loads(raw)
                self.assertIs(type(decoded["extra_quota_limits"]), list)
                self.assertEqual(decoded["extra_quota_limits"], [asdict(x) for x in p.extra_quota_limits])
                before = self.snapshot(store)
                self.assert_inventory(store, p)
                self.assertEqual(self.snapshot(store), before)
                self.close_client(client)
                reopened = StateStore(store.path, existing_only=True)
                self.addCleanup(reopened.close)
                self.assert_inventory(reopened, p)
                self.assertEqual(self.snapshot(reopened), before)

    def test_inventory_invalid_rules_fail_without_partial_state_changes(self):
        client, p = self.load(self.registry([rule(3, 60)]))
        store = client.state
        store.register(p)
        self.spend(store, p, "existing", 100, 2)
        for index, (extras, _) in enumerate(invalid_json_rules()):
            with self.subTest(index=index):
                values = json.loads(json.dumps(p.metadata()))
                values["extra_quota_limits"] = extras
                with closing(sqlite3.connect(store.path)) as db:
                    db.execute("UPDATE profiles SET metadata=?", (json.dumps(values),))
                    db.commit()
                # The malformed persisted input is a deliberate test setup;
                # the inventory call must leave all of it exactly as it was.
                before = self.snapshot(store)
                with self.assertRaises(StateRejected) as caught:
                    store.inventory()
                self.assertEqual(caught.exception.code, "invalid_inventory")
                self.assertEqual(self.snapshot(store), before)

    def test_inventory_later_invalid_profile_aborts_without_partial_result(self):
        client, p = self.load(self.registry([rule(3, 60)], source_id="a-good"))
        store = client.state
        store.register(p)
        registry = self.registry([rule(5, 180)], state_path=store.path, source_id="z-bad")
        second, q = self.load(registry)
        store.register(q)
        values = q.metadata()
        values["extra_quota_limits"] = None
        with closing(sqlite3.connect(store.path)) as db:
            db.execute("UPDATE profiles SET metadata=? WHERE source_id=?", (json.dumps(values), q.source_id))
            db.commit()
        before = self.snapshot(store)
        with self.assertRaises(StateRejected) as caught:
            second.state.inventory()
        self.assertEqual(caught.exception.code, "invalid_inventory")
        self.assertEqual(self.snapshot(store), before)

    def test_loaded_profile_consumption_survives_two_reopens_in_both_windows(self):
        registry = self.registry([rule(5, 60)], max_attempts_per_window=3, quota_window_seconds=10)
        client, p = self.load(registry)
        client.state.register(p)
        self.spend(client.state, p, "first", 100, 3)
        before = self.snapshot(client.state)
        self.close_client(client)
        client, p = self.load(registry)
        self.assertEqual(self.snapshot(client.state), before)
        self.assert_inventory(client.state, p)
        self.reject(client.state, p, 101, 1, "quota", 9)
        self.spend(client.state, p, "long-full", 110, 2)
        before = self.snapshot(client.state)
        self.close_client(client)
        client, p = self.load(registry)
        self.assertEqual(self.snapshot(client.state), before)
        self.reject(client.state, p, 121, 1, "quota", 39)
        self.spend(client.state, p, "long-boundary", 160, 3)
        self.assertEqual(self.query(client.state, "SELECT attempted_at,cost FROM attempts ORDER BY rowid"),
                         [(110, 2), (160, 3)])

    def test_loaded_quota_denial_rolls_back_pruning_clock_and_expired_job(self):
        client, p = self.load(self.registry([rule(6, 60)],
                                           max_attempts_per_window=5, quota_window_seconds=10))
        store = client.state
        self.spend(store, p, "prunable", 100)
        self.spend(store, p, "recent", 150, 3)
        store.acquire(p, "expired", 155, lease_seconds=5, cost=2)
        self.reject(store, p, 160, 2, "quota", 50)
        self.assertEqual(self.query(store, "SELECT status FROM jobs WHERE request_id='expired'"), [("running",)])
        self.assertEqual(self.query(store, "SELECT COUNT(*) FROM attempts"), [(3,)])
        self.spend(store, p, "later", 220, 2)
        self.assertEqual(self.query(store, "SELECT status FROM jobs WHERE request_id='expired'"), [("abandoned",)])
        self.assertEqual(self.query(store, "SELECT attempted_at,cost FROM attempts"), [(220, 2)])

    def test_changed_loaded_policy_after_reopen_rolls_back_without_reset(self):
        registry = self.registry([rule(6, 60)], max_attempts_per_window=5, quota_window_seconds=10)
        client, p = self.load(registry)
        client.state.register(p)
        self.spend(client.state, p, "prunable", 100)
        self.spend(client.state, p, "recent", 150, 3)
        client.state.acquire(p, "expired", 155, lease_seconds=5, cost=2)
        before = self.snapshot(client.state)
        self.close_client(client)
        client, p = self.load(registry)
        self.assertEqual(self.snapshot(client.state), before)
        variants = ({"max_attempts_per_window": 6}, {"quota_window_seconds": 11},
                    {"extra_quota_limits": [rule(7, 60)]},
                    {"extra_quota_limits": [rule(6, 61)]}, {"extra_quota_limits": []},
                    {"extra_quota_limits": [rule(6, 60), rule(20, 180)]},
                    {"max_concurrency": 3})
        for changes in variants:
            with self.subTest(changes=changes):
                changed = deepcopy(registry)
                changed["sources"][0]["profile"].update(changes, config_version="2")
                other, q = self.load(changed)
                self.assertEqual(self.snapshot(client.state), before)
                self.reject(other.state, q, 170, 1, "quota_policy_mismatch", 0)
                self.assertEqual(self.snapshot(client.state), before)
                self.close_client(other)
        self.reject(client.state, p, 160, 2, "quota", 50)
        self.assertEqual(self.snapshot(client.state), before)

    def test_reordered_json_reload_preserves_hash_policy_and_consumption(self):
        registry = self.registry([rule(5, 180), rule(3, 60), rule(3, 60)],
                                 max_attempts_per_window=100, quota_window_seconds=10)
        client, p = self.load(registry)
        client.state.register(p)
        self.spend(client.state, p, "first", 100, 3)
        before = self.snapshot(client.state)
        self.close_client(client)
        registry["sources"][0]["profile"]["extra_quota_limits"].reverse()
        client, q = self.load(registry)
        client.state.register(q)
        self.assertEqual(q.config_hash, p.config_hash)
        self.assertEqual(q.metadata(), p.metadata())
        self.assertEqual(self.snapshot(client.state), before)
        self.assert_inventory(client.state, q)
        self.reject(client.state, q, 101, 1, "quota", 59)
