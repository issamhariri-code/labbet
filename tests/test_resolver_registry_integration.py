"""Registry-driven resolver and actual client, before profile/loader migration."""
from contextlib import contextmanager
from dataclasses import replace
import pickle
from types import MappingProxyType
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import api_adapters
from api_adapters import AlphaDailyAdapter, DailyAdapterSpec, resolve_daily_adapter
from api_contracts import ApiFailure
from test_api_client import contract, profile, NOW
import test_adapter_binding_integration as binding_tests
import test_client_runtime_adapter_review as review_tests
import test_resolve_daily_adapter as resolver_tests


ALPHA = "alphavantage-daily-v1"
SYNTHETIC = "synthetic-registry-v9"
ADDRESSES = ("https://synthetic-a.invalid/daily", "https://synthetic-b.invalid/data")
PAIRS = frozenset({("raw", "fixture-schema-v1"), ("adjusted", "fixture-schema-v2")})


class RegisteredAdapterDouble:
    """Different, inert class used only in a temporary test registration."""
    __slots__ = ("token",)
    adapter_id = SYNTHETIC

    def __init__(self):
        self.token = object()


class ConstructionForbiddenDouble:
    """Any construction before validation fails the test immediately."""
    def __new__(cls):
        raise AssertionError("adapter construction before validation forbidden")


class RegisteredClientAdapterDouble(binding_tests.BoundAdapterDouble):
    """Only a test definition under Alpha: distinct URL and parsed close."""
    adapter_id = ALPHA
    created = []

    def __init__(self):
        super().__init__("registry-selected", "10.25")
        type(self).created.append(self)


def test_spec(adapter_type=RegisteredAdapterDouble):
    return DailyAdapterSpec(adapter_type=adapter_type,
                            production_endpoints=ADDRESSES, supported_contracts=PAIRS)


def registry_view(registry):
    return tuple((key, id(spec), spec.adapter_type, tuple(spec.production_endpoints),
                  frozenset(spec.supported_contracts)) for key, spec in registry.items())


@contextmanager
def temporary_registration(spec):
    """A synthetic ID is never inserted into the real production dictionary."""
    registry = MappingProxyType({**dict(api_adapters._DAILY_ADAPTERS), SYNTHETIC: spec})
    with patch.object(api_adapters, "_DAILY_ADAPTERS", registry):
        yield registry


def synthetic_inputs(*, basis="raw", schema="fixture-schema-v1", endpoint=ADDRESSES[0], environment="production"):
    # The real profile deliberately still rejects the synthetic ID in this
    # stage. This existing, marked double bypasses only that earlier boundary.
    p = resolver_tests.RawResolverProfileDouble(adapter_id=SYNTHETIC, endpoint=endpoint,
        environment=environment, price_basis=basis, schema_version=schema)
    c = contract(price_basis=basis, schema_version=schema)
    return p, c


class ResolverRegistryIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.original = api_adapters._DAILY_ADAPTERS
        self.before = registry_view(self.original)

    def tearDown(self):
        self.assertIs(api_adapters._DAILY_ADAPTERS, self.original)
        self.assertEqual(registry_view(self.original), self.before)
        self.assertEqual(tuple(self.original), (ALPHA,))

    def accept(self, p, c, expected_type):
        before = pickle.dumps((p, c))
        result = resolve_daily_adapter(p, c)
        self.assertIs(type(result), expected_type)
        self.assertEqual(pickle.dumps((p, c)), before)
        return result

    def reject(self, p, c, code):
        before = pickle.dumps((p, c))
        with self.assertRaises(ApiFailure) as caught:
            resolve_daily_adapter(p, c)
        self.assertEqual(caught.exception.code, code)
        self.assertEqual(str(caught.exception), "API-hämtningen stoppades: " + code)
        self.assertEqual(repr(caught.exception), "ApiFailure('API-hämtningen stoppades: " + code + "')")
        self.assertIsNone(caught.exception.status)
        self.assertEqual(pickle.dumps((p, c)), before)

    def test_alpha_looks_up_exact_id_and_constructs_fresh_registered_instance(self):
        created = []
        for environment in ("test", "production"):
            for explicit in (False, True):
                p = profile(environment=environment, endpoint="https://www.alphavantage.co/query",
                            **({"adapter_id": ALPHA} if explicit else {}))
                c = contract()
                with self.subTest(environment=environment, explicit=explicit), \
                        patch.object(api_adapters, "daily_adapter_spec", wraps=api_adapters.daily_adapter_spec) as lookup:
                    for _ in range(2):
                        created.append(self.accept(p, c, self.original[ALPHA].adapter_type))
                    self.assertEqual(lookup.call_count, 2)
                    self.assertEqual([call.args for call in lookup.call_args_list], [(ALPHA,), (ALPHA,)])
        self.assertEqual(len({id(value) for value in created}), 8)
        self.assertTrue(all(type(value) is AlphaDailyAdapter for value in created))

    def test_synthetic_class_both_addresses_and_contract_pairs_are_generic(self):
        spec = test_spec()
        created = []
        with temporary_registration(spec):
            for endpoint in ADDRESSES:
                for basis, schema in sorted(PAIRS):
                    with self.subTest(endpoint=endpoint, basis=basis, schema=schema):
                        self.assertNotEqual(schema, SYNTHETIC)
                        p, c = synthetic_inputs(basis=basis, schema=schema, endpoint=endpoint)
                        with patch.object(api_adapters, "daily_adapter_spec", wraps=api_adapters.daily_adapter_spec) as lookup:
                            for _ in range(2):
                                created.append(self.accept(p, c, RegisteredAdapterDouble))
                            self.assertEqual([call.args for call in lookup.call_args_list], [(SYNTHETIC,), (SYNTHETIC,)])
        self.assertEqual(len({id(value) for value in created}), 8)
        self.assertEqual(len({id(value.token) for value in created}), 8)
        self.assertNotIn(SYNTHETIC, self.original)

    def test_registry_endpoint_rejection_precedes_construction(self):
        with temporary_registration(test_spec(ConstructionForbiddenDouble)):
            invalid = ("https://synthetic-a.invalid/data", "https://synthetic-b.invalid/daily",
                       "https://other.invalid/daily", "https://synthetic-a.invalid:443/daily",
                       ADDRESSES[0] + "?key=FAKE-PRIVATE", ADDRESSES[0] + "#FAKE-PRIVATE")
            for endpoint in invalid:
                with self.subTest(endpoint=endpoint):
                    p, c = synthetic_inputs(endpoint=endpoint)
                    self.reject(p, c, "adapter_endpoint_mismatch")

    def test_cross_pairs_and_unregistered_schema_reject_before_construction(self):
        with temporary_registration(test_spec(ConstructionForbiddenDouble)):
            for basis, schema in (("raw", "fixture-schema-v2"), ("adjusted", "fixture-schema-v1"),
                                  ("raw", SYNTHETIC), ("raw", "other-v3")):
                with self.subTest(basis=basis, schema=schema):
                    p, c = synthetic_inputs(basis=basis, schema=schema)
                    self.reject(p, c, "adapter_contract_mismatch")

    def test_five_field_mismatches_reject_before_pair_check_and_construction(self):
        with temporary_registration(test_spec(ConstructionForbiddenDouble)):
            for name, value in (("currency", "EUR"), ("exchange", "OTHER"),
                                ("timezone", "Europe/Stockholm"), ("price_basis", "adjusted"),
                                ("schema_version", "fixture-schema-v2")):
                for side in ("profile", "contract"):
                    with self.subTest(field=name, side=side):
                        p, c = synthetic_inputs()
                        if side == "profile":
                            p = replace(p, **{name: value})
                        else:
                            c = replace(c, **{name: value})
                        self.reject(p, c, "source_contract_mismatch")

    def test_exact_error_order_survives_registry_binding(self):
        with temporary_registration(test_spec(ConstructionForbiddenDouble)):
            p, c = synthetic_inputs(basis="raw", schema="fixture-schema-v2", endpoint="https://FAKE-HOST.invalid/")
            self.reject(replace(p, adapter_id="FAKE-UNKNOWN"), replace(c, currency="EUR"), "unsupported_adapter")
            self.reject(p, replace(c, currency="EUR"), "adapter_endpoint_mismatch")
            p = replace(p, endpoint=ADDRESSES[0])
            self.reject(p, replace(c, currency="EUR"), "source_contract_mismatch")
            self.reject(p, c, "adapter_contract_mismatch")

    def test_unknown_and_wrong_type_id_read_no_later_fields(self):
        class OnlyIdProfileDouble:
            """No later profile field may be accessed on a failed lookup."""
            def __init__(self, value):
                self.adapter_id = value

            def __getattr__(self, name):
                raise AssertionError("later profile field read forbidden")
        for value in ("unknown", "", None, [], {}, resolver_tests.StringSubclass(ALPHA)):
            with self.subTest(type=type(value).__name__), self.assertRaises(ApiFailure) as caught:
                resolve_daily_adapter(OnlyIdProfileDouble(value), resolver_tests.NoContractReadDouble())
            self.assertEqual(caught.exception.code, "unsupported_adapter")
            self.assertEqual(str(caught.exception), "API-hämtningen stoppades: unsupported_adapter")

    def test_wrong_production_endpoint_does_not_read_contract(self):
        with temporary_registration(test_spec(ConstructionForbiddenDouble)):
            p, _ = synthetic_inputs(endpoint="https://FAKE-HOST.invalid/")
            before = pickle.dumps(p)
            with self.assertRaises(ApiFailure) as caught:
                resolve_daily_adapter(p, resolver_tests.NoContractReadDouble())
            self.assertEqual(caught.exception.code, "adapter_endpoint_mismatch")
            self.assertEqual(pickle.dumps(p), before)

    def test_test_environment_retains_nonregistered_synthetic_endpoint(self):
        with temporary_registration(test_spec()):
            p, c = synthetic_inputs(environment="test", endpoint="https://local-test-only.invalid/query")
            self.accept(p, c, RegisteredAdapterDouble)

    def test_alternating_inputs_registry_and_global_bindings_remain_unchanged(self):
        spec = test_spec()
        alpha, alpha_contract = profile(), contract()
        synthetic, synthetic_contract = synthetic_inputs()
        with temporary_registration(spec) as registry:
            before = registry_view(registry)
            namespace_before = {name: id(value) for name, value in vars(api_adapters).items()}
            for p, c, expected in ((alpha, alpha_contract, AlphaDailyAdapter),
                                   (synthetic, synthetic_contract, RegisteredAdapterDouble),
                                   (alpha, alpha_contract, AlphaDailyAdapter),
                                   (synthetic, synthetic_contract, RegisteredAdapterDouble)):
                self.accept(p, c, expected)
            self.reject(replace(synthetic, currency="EUR"), synthetic_contract, "source_contract_mismatch")
            self.assertEqual(registry_view(registry), before)
            self.assertEqual({name: id(value) for name, value in vars(api_adapters).items()}, namespace_before)
        self.assertEqual(resolve_daily_adapter.__dict__, {})

    def test_no_key_state_file_network_clock_or_logging_effects(self):
        good, good_contract = synthetic_inputs()
        alpha, alpha_contract = profile(), contract()
        bad_endpoint = replace(good, endpoint="https://FAKE-HOST.invalid/")
        bad_pair, bad_pair_contract = synthetic_inputs(basis="raw", schema="fixture-schema-v2")
        with temporary_registration(test_spec()), resolver_tests.resolver_side_effect_guards():
            self.accept(alpha, alpha_contract, AlphaDailyAdapter)
            self.accept(good, good_contract, RegisteredAdapterDouble)
            self.reject(replace(good, adapter_id="unknown"), good_contract, "unsupported_adapter")
            self.reject(bad_endpoint, good_contract, "adapter_endpoint_mismatch")
            self.reject(replace(good, currency="EUR"), good_contract, "source_contract_mismatch")
            self.reject(bad_pair, bad_pair_contract, "adapter_contract_mismatch")


class ClientRegisteredDefinitionTests(unittest.TestCase):
    setUp = review_tests.ClientAdapterInsertionTests.setUp
    snapshot = review_tests.ClientAdapterInsertionTests.snapshot

    def test_real_client_uses_registered_class_for_both_url_and_parse(self):
        RegisteredClientAdapterDouble.created.clear()
        self.addCleanup(RegisteredClientAdapterDouble.created.clear)
        original = api_adapters._DAILY_ADAPTERS
        spec = DailyAdapterSpec(RegisteredClientAdapterDouble,
            original[ALPHA].production_endpoints, original[ALPHA].supported_contracts)
        temporary = MappingProxyType({ALPHA: spec})
        # No patch to api_client's resolver or to resolve_daily_adapter: the
        # actual lookup -> spec -> constructor -> client path is exercised.
        with patch.object(api_adapters, "_DAILY_ADAPTERS", temporary), \
                patch.object(api_adapters, "daily_adapter_spec", wraps=api_adapters.daily_adapter_spec) as lookup:
            batches = [self.client.fetch(self.p, self.c) for _ in range(2)]
        self.assertIs(api_adapters._DAILY_ADAPTERS, original)
        self.assertEqual([call.args for call in lookup.call_args_list], [(ALPHA,), (ALPHA,)])
        self.assertEqual(len(RegisteredClientAdapterDouble.created), 2)
        first, second = RegisteredClientAdapterDouble.created
        self.assertIsNot(first, second)
        for adapter in (first, second):
            self.assertEqual(adapter.built, [(self.p.endpoint, self.c.symbol, "FAKE-SECRET-A")])
            self.assertEqual(len(adapter.parsed), 1)
            self.assertEqual(adapter.parsed[0][2], NOW)
        for call in self.transport.call_args_list:
            self.assertEqual(parse_qs(urlsplit(call.args[0]).query)["test_adapter"], ["registry-selected"])
        self.assertEqual([batch.prices["2026-09-30"]["4. close"] for batch in batches], ["10.25", "10.25"])
        self.assertEqual(self.state.health(self.p.source_id)["active_jobs"], 0)

    def test_real_client_rejects_registered_contract_before_constructor_and_state(self):
        original = api_adapters._DAILY_ADAPTERS
        spec = DailyAdapterSpec(ConstructionForbiddenDouble,
            original[ALPHA].production_endpoints, frozenset({("adjusted", "other-schema-v2")}))
        before = self.snapshot()
        with patch.object(api_adapters, "_DAILY_ADAPTERS", MappingProxyType({ALPHA: spec})), \
                patch.object(self.state, "register", side_effect=AssertionError("state registration forbidden")) as register, \
                patch.object(self.keys, "snapshot", side_effect=AssertionError("key snapshot forbidden")) as snapshot:
            with self.assertRaises(ApiFailure) as caught:
                self.client.fetch(self.p, self.c)
            self.assertEqual(caught.exception.code, "adapter_contract_mismatch")
            register.assert_not_called()
            snapshot.assert_not_called()
            self.transport.assert_not_called()
        self.assertIs(api_adapters._DAILY_ADAPTERS, original)
        self.assertEqual(self.snapshot(), before)
