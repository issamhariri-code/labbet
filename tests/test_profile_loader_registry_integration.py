"""Real profiles/loader/client with a temporary synthetic registry; no live I/O."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
from dataclasses import asdict, FrozenInstanceError, replace
import ast
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
from types import MappingProxyType
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlencode, urlsplit

import api_adapters
import api_profiles
from api_adapters import AlphaDailyAdapter, DailyAdapterSpec, resolve_daily_adapter
from api_client import FetchPolicy
from api_contracts import ApiFailure
from api_profiles import ProfileError, QuotaLimit, SourceProfile
from api_runtime import load_runtime
from api_state import StateRejected, StateStore
from api_transport import TransportError, TransportResponse
import ingestion.api_profiles as package_profiles
import test_api_client as fixtures


ALPHA = "alphavantage-daily-v1"
ALPHA_URL = "https://www.alphavantage.co/query"
SYNTHETIC = "test-only-profile-loader-v7"
SCHEMA = "fixture-price-schema-v3"
ENDPOINTS = ("https://registered-a.invalid/daily", "https://registered-b.invalid/data")
SECRETS = {"KEY_A": "SYNTHETIC-A&=+/?#", "KEY_B": "SYNTHETIC-B&=+/?#"}


class StringSubclass(str):
    def __hash__(self):
        raise AssertionError("string subclass must be rejected before registry lookup")


class SyntheticDailyAdapter:
    """Test-only class/format with distinct URL, parser and immutable result."""
    adapter_id = SYNTHETIC
    created = []

    def __init__(self):
        self.built, self.parsed = [], []
        type(self).created.append(self)

    def build_url(self, endpoint, symbol, secret):
        self.built.append((endpoint, symbol, secret))
        return endpoint + "?" + urlencode({"fixture_symbol": symbol,
            "access_token": secret, "fixture_adapter": SYNTHETIC})

    def parse(self, data, contract, received):
        self.parsed.append((deepcopy(data), contract, received))
        if set(data) != {"fixture_symbol", "fixture_close"} or data["fixture_symbol"] != contract.symbol:
            raise ApiFailure("instrument_mismatch")
        row = dict(fixtures.ROW, **{"4. close": data["fixture_close"]})
        completed = contract.completed_sessions(received)
        return MappingProxyType({completed[-1][0].isoformat(): MappingProxyType(row)}), completed[-1][1]


def synthetic_response():
    return TransportResponse(200, {}, json.dumps({"fixture_symbol": "TEST", "fixture_close": "10.25"}).encode())


class TrackedEnvironment(dict):
    def __init__(self, *args, forbid_keys=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.read_keys = []
        self.forbid_keys = forbid_keys

    def __getitem__(self, key):
        if key.startswith("KEY_"):
            self.read_keys.append(key)
            if self.forbid_keys:
                raise AssertionError("credential access before complete validation")
        return super().__getitem__(key)


class ProfileLoaderRegistryIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.folder = Path(self.tmp.name)
        self.location = self.folder / "registry.json"
        self.counter = 0
        self.original_registry = api_adapters._DAILY_ADAPTERS
        self.registry_before = tuple(self.original_registry.items())
        self.spec = DailyAdapterSpec(SyntheticDailyAdapter, ENDPOINTS,
            frozenset({("raw", ALPHA), ("raw", SCHEMA), ("adjusted", SCHEMA)}))
        self.test_registry = MappingProxyType({**self.original_registry, SYNTHETIC: self.spec})
        registration = patch.object(api_adapters, "_DAILY_ADAPTERS", self.test_registry)
        registration.start()
        self.addCleanup(registration.stop)
        self.addCleanup(self.assert_original_registry)
        SyntheticDailyAdapter.created = []
        self.addCleanup(setattr, SyntheticDailyAdapter, "created", [])
        network = patch("socket.create_connection", side_effect=AssertionError("live network forbidden"))
        network.start()
        self.addCleanup(network.stop)

    def assert_original_registry(self):
        self.assertEqual(tuple(self.original_registry.items()), self.registry_before)
        self.assertEqual(tuple(self.test_registry.items()), (*self.registry_before, (SYNTHETIC, self.spec)))

    def entry(self, adapter=ALPHA, *, explicit=False, endpoint=None, schema=None, **changes):
        synthetic = adapter == SYNTHETIC
        values = dict(environment="production", endpoint=endpoint or (ENDPOINTS[0] if synthetic else ALPHA_URL),
            source_id="source-b" if synthetic else "source-a", account_ref="account-b" if synthetic else "account-a",
            quota_scope="account-b" if synthetic else "account-a", credential_env="KEY_B" if synthetic else "KEY_A",
            adapter_id=adapter, schema_version=schema or ALPHA,
            rights_review_date="2000-01-01", review_due_date="2099-12-31")
        values.update(changes)
        p = fixtures.profile(**values)
        data = asdict(p)
        if not explicit:
            data.pop("adapter_id")
        c = fixtures.contract(schema_version=p.schema_version, price_basis=p.price_basis)
        return {"adapter": adapter, "profile": data, "fetch_policy": {}, "instruments": [asdict(c)]}

    def config(self, *entries, path=None):
        self.counter += 1
        return {"version": 1, "environment": "production",
            "state_path": str(path or self.folder / f"state-{self.counter}.sqlite"),
            "sources": list(entries or (self.entry(),))}

    def write(self, config):
        self.location.write_text(json.dumps(config, default=str), encoding="utf-8")

    def load(self, config, *, selected=None, secrets=None):
        self.write(config)
        before = self.location.read_bytes()
        original = deepcopy(config)
        env = TrackedEnvironment({"LABBET_API_CONFIG": str(self.location), **(SECRETS if secrets is None else secrets)})
        client, p, c = load_runtime("TEST", env, source_id=selected)
        self.addCleanup(client.state.close)
        self.addCleanup(client.credentials.close)
        self.assertIs(type(p), SourceProfile)
        self.assertEqual(config, original)
        self.assertEqual(self.location.read_bytes(), before)
        self.assertEqual(env.read_keys, [p.credential_env])
        return client, p, c

    @staticmethod
    def close(client):
        client.credentials.close()
        client.state.close()

    @staticmethod
    def snapshot(path):
        with closing(sqlite3.connect(path)) as db:
            return tuple(db.iterdump())

    @staticmethod
    def spend(state, p, label, now, cost=1):
        state.release(state.acquire(p, label, now, cost=cost), now)

    def deny_quota(self, state, p, now, delay):
        before = self.snapshot(state.path)
        with self.assertRaises(StateRejected) as caught:
            state.acquire(p, "denied", now)
        self.assertEqual(caught.exception.code, "quota")
        self.assertAlmostEqual(caught.exception.retry_after_seconds, delay)
        self.assertEqual(self.snapshot(state.path), before)

    def reject_early(self, config, code, *, selected="source-a"):
        path = Path(config["state_path"])
        existed = path.exists()
        before = self.snapshot(path) if existed else None
        bytes_before = path.read_bytes() if existed else None
        self.write(config)
        raw_before = self.location.read_bytes()
        env = TrackedEnvironment({"LABBET_API_CONFIG": str(self.location)}, forbid_keys=True)
        with patch("api_runtime.StateStore", side_effect=AssertionError("state construction forbidden")) as state, \
                patch("api_runtime.CredentialStore", side_effect=AssertionError("credential construction forbidden")) as keys:
            with self.assertRaises(ApiFailure) as caught:
                load_runtime("TEST", env, source_id=selected)
            self.assertEqual(caught.exception.code, code)
            self.assertEqual(str(caught.exception), "API-hämtningen stoppades: " + code)
            state.assert_not_called()
            keys.assert_not_called()
        self.assertEqual(env.read_keys, [])
        self.assertEqual(self.location.read_bytes(), raw_before)
        self.assertEqual(path.exists(), existed)
        if existed:
            self.assertEqual(path.read_bytes(), bytes_before)
            self.assertEqual(self.snapshot(path), before)

    def wire(self, client, clock=None, transport=None):
        clock = clock or fixtures.Clock()
        client.wall, client.monotonic, client.sleep = clock.wall, clock.monotonic, clock.sleep
        client.jitter = lambda: 1
        client.policy = FetchPolicy(total_seconds=20, socket_seconds=10, max_attempts=1)
        client.transport = transport or Mock(return_value=synthetic_response())
        return clock

    def pair(self, *, synthetic_schema=ALPHA):
        config = self.config(self.entry(), self.entry(SYNTHETIC, schema=synthetic_schema))
        a, p, c = self.load(config, selected="source-a", secrets={"KEY_A": SECRETS["KEY_A"]})
        b, q, d = self.load(config, selected="source-b", secrets={"KEY_B": SECRETS["KEY_B"]})
        a.credentials.install(q, SECRETS)
        b.credentials.install(p, SECRETS)
        return a, p, c, b, q, d

    def test_real_profile_accepts_registered_class_without_constructing_it(self):
        for factory in (fixtures.profile, package_profiles.SourceProfile):
            for endpoint in ENDPOINTS:
                values = asdict(fixtures.profile(adapter_id=SYNTHETIC, endpoint=endpoint, schema_version=SCHEMA))
                with self.subTest(factory=factory.__module__, endpoint=endpoint), \
                        patch.object(SyntheticDailyAdapter, "__init__", side_effect=AssertionError("profile cannot construct adapter")), \
                        patch.object(api_profiles.CredentialStore, "__init__", side_effect=AssertionError("keys forbidden")), \
                        patch.object(StateStore, "__init__", side_effect=AssertionError("state forbidden")):
                    p = factory(**values)
                    self.assertEqual(p.adapter_id, SYNTHETIC)
                    self.assertEqual(p.metadata()["adapter_id"], SYNTHETIC)
        self.assertEqual(SyntheticDailyAdapter.created, [])

    def test_unknown_and_wrong_typed_profile_ids_keep_safe_error_and_priority(self):
        invalid = ("", "FAKE-PRIVATE-ID", ALPHA + " ", SYNTHETIC.upper(), None, True, 1, 1.5, [], {}, (),
                   b"alpha", StringSubclass(ALPHA), StringSubclass(SYNTHETIC))
        for module in (api_profiles, package_profiles):
            values = asdict(fixtures.profile())
            for value in invalid:
                with self.subTest(module=module.__name__, type=type(value).__name__):
                    with self.assertRaises(module.ProfileError) as caught:
                        module.SourceProfile(**dict(values, adapter_id=value, source_id=None, environment="FAKE", credential_env=None))
                    self.assertEqual(caught.exception.code, "unsupported_adapter")
                    self.assertEqual(str(caught.exception), "API configuration rejected: unsupported_adapter")
                    self.assertIsNone(caught.exception.__cause__)
                    self.assertTrue(caught.exception.__suppress_context__)
        self.assertEqual(SyntheticDailyAdapter.created, [])

    def test_known_profile_id_keeps_remaining_validation_order(self):
        cases = (({"source_id": None, "environment": "bad"}, "invalid_identifier"),
                 ({"environment": "bad", "credential_env": None}, "invalid_environment"),
                 ({"credential_env": None, "endpoint": "bad"}, "invalid_credential_reference"),
                 ({"endpoint": "http://invalid"}, "invalid_endpoint"))
        for changes, code in cases:
            with self.subTest(code=code), self.assertRaises(ProfileError) as caught:
                fixtures.profile(adapter_id=SYNTHETIC, **changes)
            self.assertEqual(caught.exception.code, code)
        self.assertEqual(SyntheticDailyAdapter.created, [])

    def test_new_id_metadata_hash_cache_and_frozen_replace(self):
        for extras in ((), (QuotaLimit(8, 60), QuotaLimit(800, 86400))):
            p = fixtures.profile(extra_quota_limits=extras)
            explicit = replace(p, adapter_id=ALPHA)
            q = replace(p, adapter_id=SYNTHETIC)
            self.assertEqual((p.metadata(), p.config_hash, p.cache_namespace),
                             (explicit.metadata(), explicit.config_hash, explicit.cache_namespace))
            expected = dict(p.metadata(), adapter_id=SYNTHETIC)
            self.assertEqual(q.metadata(), expected)
            independent_hash = hashlib.sha256(json.dumps(expected, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
            self.assertEqual(q.config_hash, independent_hash)
            self.assertNotEqual(q.config_hash, p.config_hash)
            self.assertEqual(q.cache_namespace, q.source_id + ":" + independent_hash)
            self.assertEqual(replace(q, owner="reviewer").adapter_id, SYNTHETIC)
            for name in ("adapter_id", "endpoint", "extra_quota_limits"):
                with self.assertRaises(FrozenInstanceError):
                    setattr(q, name, None)
            with self.assertRaises(ProfileError) as caught:
                replace(q, adapter_id="unknown")
            self.assertEqual(caught.exception.code, "unsupported_adapter")

    def test_import_orders_in_fresh_docker_python_processes(self):
        orders = (("api_profiles", "api_adapters", "api_runtime", "api_client", "api_state"),
                  ("api_runtime", "api_profiles", "api_client", "api_adapters", "api_state"),
                  ("api_adapters", "api_contracts", "api_profiles", "api_runtime"),
                  ("api_client", "api_runtime", "api_profiles", "api_adapters"),
                  ("api_state", "api_profiles", "api_runtime", "api_adapters"),
                  ("ingestion.api_profiles", "api_profiles", "api_runtime"),
                  ("ingestion.api_runtime", "ingestion.api_profiles", "api_runtime"),
                  ("ingestion.api_adapters", "api_adapters", "api_profiles", "api_runtime"))
        for order in orders:
            with self.subTest(order=order):
                code = "import importlib; " + "; ".join(f"importlib.import_module({name!r})" for name in order)
                result = subprocess.run([sys.executable, "-B", "-c", code], capture_output=True, text=True, timeout=10)
                self.assertEqual((result.returncode, result.stdout, result.stderr), (0, "", ""))
        for name in ("api_profiles.py", "api_runtime.py"):
            ast.parse((Path(__file__).parents[1] / "ingestion" / name).read_text())

    def test_loader_missing_and_explicit_ids_use_registered_addresses_and_real_profile(self):
        for adapter, endpoints in ((ALPHA, (ALPHA_URL,)), (SYNTHETIC, ENDPOINTS)):
            for endpoint in endpoints:
                for explicit in (False, True):
                    schema = SCHEMA if adapter == SYNTHETIC else ALPHA
                    with self.subTest(adapter=adapter, endpoint=endpoint, explicit=explicit):
                        client, p, c = self.load(self.config(self.entry(adapter, explicit=explicit, endpoint=endpoint, schema=schema)))
                        self.assertEqual((p.adapter_id, p.endpoint, p.schema_version), (adapter, endpoint, schema))
                        self.assertIs(type(resolve_daily_adapter(p, c)), SyntheticDailyAdapter if adapter == SYNTHETIC else AlphaDailyAdapter)
                        self.assertEqual(client.credentials.snapshot(p, 1).secret, SECRETS[p.credential_env])
                        if adapter == ALPHA:
                            self.assertEqual(p.config_hash, "e4b68301d950c6cdbac903de9f8ec5c14f80b2707cbd7abd313c8e063e0d0849")
                        self.close(client)

    def test_each_source_uses_own_spec_even_when_unselected_or_reordered(self):
        for entries in ((self.entry(), self.entry(SYNTHETIC, endpoint=ENDPOINTS[1])),
                        (self.entry(SYNTHETIC, endpoint=ENDPOINTS[1]), self.entry())):
            config = self.config(*entries)
            for source, key, endpoint in (("source-a", "KEY_A", ALPHA_URL), ("source-b", "KEY_B", ENDPOINTS[1])):
                with self.subTest(order=entries[0]["adapter"], source=source):
                    client, p, _ = self.load(config, selected=source, secrets={key: SECRETS[key]})
                    self.assertEqual(p.endpoint, endpoint)
                    self.close(client)

    def test_registered_identity_conflicts_are_not_overwritten_selected_or_unselected(self):
        for adapter, other in ((ALPHA, SYNTHETIC), (SYNTHETIC, ALPHA)):
            for selected in (True, False):
                bad = self.entry(adapter, explicit=True)
                bad["profile"].update(adapter_id=other, endpoint="https://wrong.invalid/path", environment="test")
                entries = [bad] if selected else [self.entry(), dict(bad, profile=dict(bad["profile"], source_id="bad-source"))]
                with self.subTest(adapter=adapter, selected=selected):
                    source = bad["profile"]["source_id"] if selected else "source-a"
                    self.reject_early(self.config(*entries), "adapter_identity_mismatch", selected=source)

    def test_unknown_entry_and_profile_ids_reject_before_state_keys_in_all_sources(self):
        for adapter in (ALPHA, SYNTHETIC):
            for field in ("entry", "profile"):
                for value in ("", "FAKE-UNKNOWN", None, True, 1, 1.5, [], {}):
                    for selected in (True, False):
                        bad = self.entry(adapter, explicit=True)
                        if field == "entry":
                            bad["adapter"] = value
                        else:
                            bad["profile"]["adapter_id"] = value
                        bad["profile"]["source_id"] = "bad-source"
                        with self.subTest(adapter=adapter, field=field, value=value, selected=selected):
                            entries = [bad] if selected else [self.entry(), bad]
                            self.reject_early(self.config(*entries), "unsupported_adapter",
                                              selected="bad-source" if selected else "source-a")

    def test_endpoint_rejections_cover_both_registered_and_unselected_sources(self):
        for adapter in (ALPHA, SYNTHETIC):
            own = ALPHA_URL if adapter == ALPHA else ENDPOINTS[0]
            for endpoint, code in ((ENDPOINTS[0] if adapter == ALPHA else ALPHA_URL, "adapter_endpoint_mismatch"),
                    (own + "/other", "adapter_endpoint_mismatch"),
                    (own.replace("https://", "https://wrong."), "adapter_endpoint_mismatch"),
                    (own.replace(".invalid", ".invalid:443") if adapter == SYNTHETIC else own.replace(".co", ".co:443"), "adapter_endpoint_mismatch"),
                    (own + "?extra=1", "invalid_endpoint"), (own + "#fragment", "invalid_endpoint")):
                for selected in (True, False):
                    bad = self.entry(adapter)
                    bad["profile"].update(endpoint=endpoint, source_id="bad-source")
                    with self.subTest(adapter=adapter, endpoint=endpoint, selected=selected):
                        self.reject_early(self.config(*([bad] if selected else [self.entry(), bad])), code,
                                          selected="bad-source" if selected else "source-a")

    def test_environment_rejections_cover_each_source_before_state_and_keys(self):
        for adapter in (ALPHA, SYNTHETIC):
            for environment, code in (("test", "environment_mismatch"), ("invalid", "invalid_environment")):
                for selected in (True, False):
                    bad = self.entry(adapter)
                    bad["profile"].update(environment=environment, source_id="bad-source")
                    self.reject_early(self.config(*([bad] if selected else [self.entry(), bad])), code,
                                      selected="bad-source" if selected else "source-a")
        root_bad = self.config(self.entry(), self.entry(SYNTHETIC))
        root_bad["environment"] = "test"
        self.reject_early(root_bad, "configuration_schema")

    def test_loader_preserves_exact_rejection_order(self):
        bad = self.entry(SYNTHETIC, explicit=True)
        cases = []
        e = deepcopy(bad); e.update(adapter="unknown", profile=None); cases.append((e, "unsupported_adapter"))
        e = deepcopy(bad); e["profile"] = None; cases.append((e, "configuration_schema"))
        e = deepcopy(bad); e["profile"].update(adapter_id="unknown", source_id=None); cases.append((e, "unsupported_adapter"))
        e = deepcopy(bad); e["profile"].update(adapter_id=ALPHA, source_id="source-a", endpoint="https://wrong.invalid/a", environment="test"); cases.append((e, "adapter_identity_mismatch"))
        e = deepcopy(bad); e["profile"].update(source_id="source-a", endpoint="https://wrong.invalid/a", environment="test"); cases.append((e, "duplicate_source"))
        e = deepcopy(bad); e["profile"].update(endpoint="https://wrong.invalid/a", environment="test"); cases.append((e, "adapter_endpoint_mismatch"))
        e = deepcopy(bad); e["profile"].update(environment="test"); cases.append((e, "environment_mismatch"))
        for entry, code in cases:
            with self.subTest(code=code):
                self.reject_early(self.config(self.entry(), entry), code)

    def test_early_rejections_preserve_seeded_state_credentials_and_consumption(self):
        config = self.config(self.entry(), self.entry(SYNTHETIC))
        client, p, _ = self.load(config, selected="source-a")
        client.state.register(p)
        self.spend(client.state, p, "seed", 100, 2)
        for selected in ("source-a", "source-b"):
            for index in (0, 1):
                for changes, code in (({"adapter_id": "unknown"}, "unsupported_adapter"),
                        ({"adapter_id": SYNTHETIC if index == 0 else ALPHA}, "adapter_identity_mismatch"),
                        ({"endpoint": "https://wrong.invalid/query"}, "adapter_endpoint_mismatch"),
                        ({"environment": "test"}, "environment_mismatch")):
                    bad = deepcopy(config)
                    bad["sources"][index]["profile"].update(changes)
                    with self.subTest(selected=selected, index=index, code=code):
                        self.reject_early(bad, code, selected=selected)

    def test_selection_duplicate_and_ambiguity_guards_with_two_registered_adapters(self):
        config = self.config(self.entry(), self.entry(SYNTHETIC))
        for source in (None, "missing-source"):
            self.reject_early(config, "unknown_or_ambiguous_instrument", selected=source)
        duplicate = deepcopy(config)
        duplicate["sources"][1]["profile"]["source_id"] = "source-a"
        self.reject_early(duplicate, "duplicate_source")
        config["sources"][1]["instruments"][0]["symbol"] = "OTHER"
        self.reject_early(config, "unknown_or_ambiguous_instrument", selected="source-b")

    def test_real_loader_profile_resolver_client_uses_distinct_registered_url_and_parser(self):
        for adapter, endpoint, schema, close in ((ALPHA, ALPHA_URL, ALPHA, "10.50"),
                (SYNTHETIC, ENDPOINTS[0], SCHEMA, "10.25"), (SYNTHETIC, ENDPOINTS[1], ALPHA, "10.25")):
            client, p, c = self.load(self.config(self.entry(adapter, endpoint=endpoint, schema=schema)))
            self.wire(client, transport=Mock(return_value=fixtures.response() if adapter == ALPHA else synthetic_response()))
            before = (p.metadata(), p.config_hash, asdict(c))
            created_before = len(SyntheticDailyAdapter.created)
            for _ in range(2):
                batch = client.fetch(p, c)
                self.assertEqual((batch.source_id, batch.prices["2026-09-30"]["4. close"]), (p.source_id, close))
                self.assertEqual((batch.observed_at, batch.received_at), (fixtures.NOW.replace(hour=20), fixtures.NOW))
                expected_hash = hashlib.sha256((p.config_hash + c.fingerprint).encode()).hexdigest()
                self.assertEqual(batch.config_version, expected_hash)
                with self.assertRaises(TypeError):
                    batch.prices["2026-09-30"]["4. close"] = "bad"
            for call in client.transport.call_args_list:
                parsed = urlsplit(call.args[0])
                self.assertEqual(parsed.scheme + "://" + parsed.netloc + parsed.path, endpoint)
                query = parse_qs(parsed.query)
                if adapter == SYNTHETIC:
                    self.assertEqual(query, {"fixture_symbol": [c.symbol], "access_token": [SECRETS["KEY_B"]], "fixture_adapter": [SYNTHETIC]})
                else:
                    self.assertEqual(query, {"function": ["TIME_SERIES_DAILY"], "symbol": [c.symbol], "outputsize": ["compact"], "apikey": [SECRETS["KEY_A"]]})
            if adapter == SYNTHETIC:
                instances = SyntheticDailyAdapter.created[created_before:]
                self.assertEqual(len(instances), 2)
                self.assertIsNot(instances[0], instances[1])
                for instance in instances:
                    self.assertEqual(instance.built, [(endpoint, c.symbol, SECRETS["KEY_B"])])
                    self.assertEqual(instance.parsed[0][1:], (c, fixtures.NOW))
            self.assertEqual((p.metadata(), p.config_hash, asdict(c)), before)
            self.close(client)

    def test_real_cross_adapter_fallback_in_both_directions_keeps_20_to_13_budget(self):
        for reverse in (False, True):
            a, p, c, b, q, _ = self.pair()
            primary, backup, client = (q, p, b) if reverse else (p, q, a)
            calls = []
            clock = fixtures.Clock()
            def transport(url, **options):
                calls.append((url, options))
                if len(calls) == 1:
                    clock.elapsed += 7
                    raise TransportError("network")
                return fixtures.response() if backup.adapter_id == ALPHA else synthetic_response()
            self.wire(client, clock, transport)
            batch = client.fetch_with_fallback(primary, backup, c,
                approved_sources=((primary.source_id, backup.source_id),))
            self.assertEqual(batch.source_id, backup.source_id)
            self.assertEqual(batch.prices["2026-09-30"]["4. close"], "10.50" if reverse else "10.25")
            self.assertEqual([opts["total_timeout"] for _, opts in calls], [20, 13])
            self.assertEqual([opts["timeout"] for _, opts in calls], [10, 10])
            self.assertEqual([url.split("?")[0] for url, _ in calls], [primary.endpoint, backup.endpoint])
            for (url, opts), source in zip(calls, (primary, backup)):
                query = parse_qs(urlsplit(url).query)
                self.assertEqual(query["apikey" if source.adapter_id == ALPHA else "access_token"], [SECRETS[source.credential_env]])
                self.assertEqual(opts["policy"].test_mode, False)
                self.assertEqual(opts["policy"].origin, "https://" + urlsplit(source.endpoint).netloc)
            self.close(a); self.close(b)

    def test_fallback_incompatible_contracts_and_missing_approval_have_no_side_effects(self):
        a, p, c, b, q, _ = self.pair(synthetic_schema=SCHEMA)
        self.wire(a)
        before = self.snapshot(a.state.path)
        for approved, code in (((), "fallback_not_approved"), (((p.source_id, q.source_id),), "fallback_incompatible")):
            with patch.object(a.state, "register", side_effect=AssertionError("registration forbidden")), \
                    patch.object(a.credentials, "snapshot", side_effect=AssertionError("key snapshot forbidden")):
                with self.assertRaises(ApiFailure) as caught:
                    a.fetch_with_fallback(p, q, c, approved_sources=approved)
                self.assertEqual(caught.exception.code, code)
            self.assertEqual(self.snapshot(a.state.path), before)
            a.transport.assert_not_called()

    def test_permanent_errors_do_not_fallback_to_another_registered_adapter(self):
        cases = ((TransportError("http", status=401), "authentication"),
                 (TransportError("http", status=403), "permission_unknown"),
                 (TransportError("http", status=404), "http"),
                 (TransportError("http", status=429), "quota"),
                 (TransportError("tls"), "tls"), (TransportError("configuration"), "configuration"))
        for reverse in (False, True):
            for error, code in cases:
                a, p, c, b, q, _ = self.pair()
                primary, backup, client = (q, p, b) if reverse else (p, q, a)
                transport = Mock(side_effect=error)
                self.wire(client, transport=transport)
                with patch.object(client.credentials, "snapshot", wraps=client.credentials.snapshot) as snapshots:
                    with self.assertRaises(ApiFailure) as caught:
                        client.fetch_with_fallback(primary, backup, c, approved_sources=((primary.source_id, backup.source_id),))
                    self.assertEqual(caught.exception.code, code)
                    self.assertEqual([call.args[0] for call in snapshots.call_args_list], [primary])
                transport.assert_called_once()
                with closing(sqlite3.connect(client.state.path)) as db:
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM profiles WHERE source_id=?", (backup.source_id,)).fetchone()[0], 0)
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM attempts WHERE scope=?", (backup.quota_scope,)).fetchone()[0], 0)
                self.close(a); self.close(b)

    def test_inventory_json_and_consumption_survive_repeated_reopen(self):
        entry = self.entry(SYNTHETIC, schema=SCHEMA, max_attempts_per_window=2, quota_window_seconds=10,
            extra_quota_limits=(QuotaLimit(8, 180), QuotaLimit(3, 60), QuotaLimit(3, 60)))
        config = self.config(entry)
        client, p, _ = self.load(config)
        client.state.register(p)
        self.spend(client.state, p, "initial", 100, 2)
        identity = (p.metadata(), p.config_hash, p.cache_namespace, p.quota_limits)
        for now, delay, spend_at, cost in ((101, 9, 110, 1), (121, 39, 160, 2)):
            before = self.snapshot(client.state.path)
            self.close(client)
            client, q, _ = self.load(config)
            self.assertEqual(self.snapshot(client.state.path), before)
            self.assertEqual((q.metadata(), q.config_hash, q.cache_namespace, q.quota_limits), identity)
            inventory = client.state.inventory()
            self.assertEqual(inventory, [{"source_id": p.source_id, "config_version": p.config_version,
                "config_hash": p.config_hash, "profile": p.metadata()}])
            with closing(sqlite3.connect(client.state.path)) as db:
                raw = json.loads(db.execute("SELECT metadata FROM profiles").fetchone()[0])
            self.assertEqual(raw["adapter_id"], SYNTHETIC)
            self.assertEqual(raw["extra_quota_limits"], [asdict(x) for x in p.extra_quota_limits])
            self.assertEqual(self.snapshot(client.state.path), before)
            self.deny_quota(client.state, q, now, delay)
            self.spend(client.state, q, f"spend-{now}", spend_at, cost)

    def test_removed_registration_rejects_inventory_without_rewrite_or_quota_reset(self):
        config = self.config(self.entry(SYNTHETIC, max_attempts_per_window=2, quota_window_seconds=10,
            extra_quota_limits=(QuotaLimit(3, 60),)))
        client, p, _ = self.load(config)
        client.state.register(p)
        self.spend(client.state, p, "initial", 100, 2)
        before = self.snapshot(client.state.path)
        expected_inventory = client.state.inventory()
        with patch.object(api_adapters, "_DAILY_ADAPTERS", self.original_registry):
            for reopen in (False, True):
                state = StateStore(client.state.path, existing_only=True) if reopen else client.state
                try:
                    with self.assertRaises(StateRejected) as caught:
                        state.inventory()
                    self.assertEqual(caught.exception.code, "invalid_inventory")
                    self.assertEqual(str(caught.exception), "API state rejected: invalid_inventory")
                    self.assertEqual(self.snapshot(state.path), before)
                finally:
                    if reopen:
                        state.close()
            self.reject_early(config, "unsupported_adapter", selected="source-b")
        self.assertEqual(client.state.inventory(), expected_inventory)
        self.assertEqual(self.snapshot(client.state.path), before)
        self.deny_quota(client.state, p, 101, 9)

    def test_changed_registered_quota_policy_rolls_back_after_reopen(self):
        config = self.config(self.entry(SYNTHETIC, max_attempts_per_window=2, quota_window_seconds=10,
            extra_quota_limits=(QuotaLimit(3, 60),)))
        client, p, _ = self.load(config)
        client.state.register(p)
        self.spend(client.state, p, "initial", 100, 2)
        before = self.snapshot(client.state.path)
        self.close(client)
        changed = deepcopy(config)
        changed["sources"][0]["profile"].update(config_version="2", extra_quota_limits=[{"max_credits": 4, "window_seconds": 60}])
        client, q, _ = self.load(changed)
        with self.assertRaises(StateRejected) as caught:
            client.state.acquire(q, "denied", 101)
        self.assertEqual(caught.exception.code, "quota_policy_mismatch")
        self.assertEqual(self.snapshot(client.state.path), before)
        self.deny_quota(client.state, p, 101, 9)

    def test_concurrent_alpha_and_registered_adapter_keep_own_keys_and_results(self):
        a, p, c, b, q, d = self.pair(synthetic_schema=SCHEMA)
        barrier = threading.Barrier(2)
        calls = []
        def transport(url, **options):
            calls.append(url)
            barrier.wait(timeout=5)
            return fixtures.response() if url.startswith(ALPHA_URL + "?") else synthetic_response()
        self.wire(a, transport=transport)
        self.wire(b, transport=transport)
        with ThreadPoolExecutor(max_workers=2) as pool:
            first, second = pool.submit(a.fetch, p, c), pool.submit(b.fetch, q, d)
            batches = first.result(timeout=10), second.result(timeout=10)
        self.assertEqual([(v.source_id, v.prices["2026-09-30"]["4. close"]) for v in batches],
                         [("source-a", "10.50"), ("source-b", "10.25")])
        for url in calls:
            query = parse_qs(urlsplit(url).query)
            self.assertEqual(query.get("apikey", query.get("access_token")),
                             [SECRETS["KEY_A" if url.startswith(ALPHA_URL + "?") else "KEY_B"]])
        self.assertEqual((a.state.health(p.source_id)["active_jobs"], b.state.health(q.source_id)["active_jobs"]), (0, 0))

    def test_registered_adapter_rotation_keeps_inflight_snapshot_and_next_key(self):
        client, p, c = self.load(self.config(self.entry(SYNTHETIC, schema=SCHEMA)))
        rotated = replace(p, credential_version="v2", config_version="2")
        urls = []
        def transport(url, **options):
            urls.append(url)
            if len(urls) == 1:
                client.credentials.install(rotated, {"KEY_B": "SYNTHETIC-ROTATED&=+"})
            return synthetic_response()
        self.wire(client, transport=transport)
        first = client.fetch(p, c)
        second = client.fetch(rotated, c)
        self.assertEqual([parse_qs(urlsplit(url).query)["access_token"] for url in urls],
                         [[SECRETS["KEY_B"]], ["SYNTHETIC-ROTATED&=+"]])
        self.assertNotEqual(first.config_version, second.config_version)
        self.assertEqual([first.source_id, second.source_id], [p.source_id, p.source_id])


if __name__ == "__main__":
    unittest.main()
