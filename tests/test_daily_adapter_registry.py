"""Read-only adapter descriptions before registry/resolver integration."""
from dataclasses import FrozenInstanceError
import pickle
from types import MappingProxyType
import unittest
from unittest.mock import Mock, patch

import api_adapters
from api_adapters import AlphaDailyAdapter, DailyAdapterSpec, daily_adapter_spec
from api_contracts import ApiFailure
import test_resolve_daily_adapter as resolver_tests


ALPHA = "alphavantage-daily-v1"
SYNTHETIC = "synthetic-adapter-v7"


class StringSubclass(str):
    pass


class NoHashStringSubclass(str):
    """A bad-type probe: no hashing/comparison may occur before rejection."""
    def __hash__(self):
        raise AssertionError("string subclass hash forbidden")

    def __eq__(self, other):
        raise AssertionError("string subclass comparison forbidden")


class SyntheticAdapterMeta(type):
    def __call__(cls, *args, **kwargs):
        raise AssertionError("synthetic adapter construction forbidden")


class SyntheticAdapterDouble(metaclass=SyntheticAdapterMeta):
    """Description-only code reference; never a real registered provider."""
    pass


def synthetic_spec():
    return DailyAdapterSpec(
        adapter_type=SyntheticAdapterDouble,
        production_endpoints=("https://synthetic-a.invalid/daily", "https://synthetic-b.invalid/data"),
        supported_contracts=frozenset({("raw", "fixture-schema-v1"), ("adjusted", "fixture-schema-v2")}),
    )


class DailyAdapterRegistryTests(unittest.TestCase):
    def setUp(self):
        self.original_registry = api_adapters._DAILY_ADAPTERS
        self.original_items = tuple(self.original_registry.items())

    def tearDown(self):
        self.assertIs(api_adapters._DAILY_ADAPTERS, self.original_registry)
        self.assertEqual(tuple(api_adapters._DAILY_ADAPTERS.items()), self.original_items)
        self.assertEqual(tuple(api_adapters._DAILY_ADAPTERS), (ALPHA,))

    def reject(self, value):
        before = pickle.dumps(value)
        with self.assertRaises(ApiFailure) as caught:
            daily_adapter_spec(value)
        self.assertEqual(caught.exception.code, "unsupported_adapter")
        self.assertEqual(str(caught.exception), "API-hämtningen stoppades: unsupported_adapter")
        self.assertEqual(repr(caught.exception), "ApiFailure('API-hämtningen stoppades: unsupported_adapter')")
        self.assertIsNone(caught.exception.status)
        self.assertEqual(pickle.dumps(value), before)

    def assert_alpha(self, expected=None):
        spec = daily_adapter_spec(ALPHA)
        if expected is not None:
            self.assertIs(spec, expected)
        self.assertIs(type(spec), DailyAdapterSpec)
        self.assertIs(spec.adapter_type, AlphaDailyAdapter)
        self.assertIs(type(spec.production_endpoints), tuple)
        self.assertEqual(spec.production_endpoints, ("https://www.alphavantage.co/query",))
        self.assertIs(type(spec.supported_contracts), frozenset)
        self.assertEqual(spec.supported_contracts, frozenset({("raw", "alphavantage-daily-v1")}))
        self.assertTrue(all(type(pair) is tuple and len(pair) == 2 for pair in spec.supported_contracts))
        return spec

    def test_exact_alpha_description_class_address_and_contract_pair(self):
        self.assert_alpha()

    def test_synthetic_registration_uses_same_lookup_and_preserves_contract_pairs(self):
        alpha, synthetic = self.assert_alpha(), synthetic_spec()
        temporary = MappingProxyType({ALPHA: alpha, SYNTHETIC: synthetic})
        with patch.object(api_adapters, "_DAILY_ADAPTERS", temporary):
            self.assertIs(daily_adapter_spec(SYNTHETIC), synthetic)
            self.assert_alpha(alpha)
            self.assertIs(synthetic.adapter_type, SyntheticAdapterDouble)
            self.assertEqual(synthetic.production_endpoints,
                             ("https://synthetic-a.invalid/daily", "https://synthetic-b.invalid/data"))
            self.assertEqual(synthetic.supported_contracts,
                             frozenset({("raw", "fixture-schema-v1"), ("adjusted", "fixture-schema-v2")}))
            self.assertNotIn(("raw", "fixture-schema-v2"), synthetic.supported_contracts)
            self.assertNotIn(("adjusted", "fixture-schema-v1"), synthetic.supported_contracts)
            # The schema IDs deliberately differ from the adapter ID.
            self.assertNotIn(("raw", SYNTHETIC), synthetic.supported_contracts)
        self.reject(SYNTHETIC)
        self.assert_alpha(alpha)

    def test_unknown_empty_and_changed_ids_are_not_normalized_or_echoed(self):
        alpha = self.assert_alpha()
        invalid = ("", " ", "unknown", SYNTHETIC, ALPHA.upper(), " " + ALPHA,
                   ALPHA + " ", ALPHA + "\n", ALPHA + "\u200b", ALPHA + ".",
                   "alphavantage-daily-v2", "FAKE-PRIVATE-ID\nDO-NOT-ECHO", "x" * 1000)
        for index, value in enumerate(invalid):
            with self.subTest(index=index):
                self.reject(value)
                self.assert_alpha(alpha)

    def test_wrong_types_unhashable_values_and_string_subclasses_are_safe(self):
        invalid = (None, True, False, 0, 1.0, complex(1), b"alphavantage-daily-v1",
                   bytearray(b"FAKE"), [], [ALPHA], {}, {ALPHA: "FAKE"}, (),
                   (ALPHA,), {ALPHA}, frozenset({ALPHA}), StringSubclass(ALPHA),
                   StringSubclass("FAKE-PRIVATE-ID"), NoHashStringSubclass(ALPHA))
        for index, value in enumerate(invalid):
            with self.subTest(index=index, type=type(value).__name__):
                self.reject(value)

    def test_type_rejection_happens_before_registry_lookup_or_hashing(self):
        get = Mock(side_effect=AssertionError("registry lookup forbidden for bad type"))
        class LookupForbiddenRegisterDouble:
            """Bad-type short-circuit test only; not a production registry."""
            pass
        registry = LookupForbiddenRegisterDouble()
        registry.get = get
        with patch.object(api_adapters, "_DAILY_ADAPTERS", registry):
            for value in (None, [], {}, True, StringSubclass(ALPHA), NoHashStringSubclass(ALPHA)):
                self.reject(value)
            get.assert_not_called()

    def test_production_register_entries_cannot_be_changed_or_deleted(self):
        alpha = self.assert_alpha()
        registry = self.original_registry
        with self.assertRaises(TypeError):
            registry[ALPHA] = synthetic_spec()
        self.assert_alpha(alpha)
        with self.assertRaises(TypeError):
            registry[SYNTHETIC] = synthetic_spec()
        self.assert_alpha(alpha)
        with self.assertRaises(TypeError):
            del registry[ALPHA]
        self.assert_alpha(alpha)
        for method, args in (("clear", ()), ("update", ({SYNTHETIC: synthetic_spec()},)),
                             ("pop", (ALPHA,)), ("setdefault", (SYNTHETIC, synthetic_spec()))):
            with self.subTest(method=method), self.assertRaises(AttributeError):
                getattr(registry, method)(*args)
            self.assert_alpha(alpha)

    def test_description_fields_are_frozen_and_later_lookup_stays_identical(self):
        alpha = self.assert_alpha()
        for name, value in (("adapter_type", SyntheticAdapterDouble),
                            ("production_endpoints", ("https://changed.invalid/",)),
                            ("supported_contracts", frozenset({("adjusted", "other-v2")}))):
            with self.subTest(field=name):
                with self.assertRaises(FrozenInstanceError):
                    setattr(alpha, name, value)
                self.assert_alpha(alpha)
                with self.assertRaises(FrozenInstanceError):
                    delattr(alpha, name)
                self.assert_alpha(alpha)

    def test_nested_endpoint_and_contract_collections_are_read_only(self):
        alpha, synthetic = self.assert_alpha(), synthetic_spec()
        temporary = MappingProxyType({ALPHA: alpha, SYNTHETIC: synthetic})
        with patch.object(api_adapters, "_DAILY_ADAPTERS", temporary):
            for adapter_id in (ALPHA, SYNTHETIC):
                spec = daily_adapter_spec(adapter_id)
                before = (spec.adapter_type, spec.production_endpoints, spec.supported_contracts)
                with self.assertRaises(TypeError):
                    spec.production_endpoints[0] = "https://changed.invalid/"
                with self.assertRaises(AttributeError):
                    spec.production_endpoints.append("https://changed.invalid/")
                with self.assertRaises(FrozenInstanceError):
                    spec.production_endpoints += ("https://changed.invalid/",)
                with self.assertRaises(AttributeError):
                    spec.supported_contracts.add(("raw", "other-v2"))
                with self.assertRaises(AttributeError):
                    spec.supported_contracts.clear()
                with self.assertRaises(FrozenInstanceError):
                    spec.supported_contracts |= {("raw", "other-v2")}
                pair = next(iter(spec.supported_contracts))
                with self.assertRaises(TypeError):
                    pair[0] = "changed"
                self.assertIs(daily_adapter_spec(adapter_id), spec)
                self.assertEqual((spec.adapter_type, spec.production_endpoints, spec.supported_contracts), before)

    def test_alternating_lookups_retain_no_request_state_or_input_mutation(self):
        alpha, synthetic = self.assert_alpha(), synthetic_spec()
        temporary = MappingProxyType({ALPHA: alpha, SYNTHETIC: synthetic})
        namespace_before = {name: id(value) for name, value in vars(api_adapters).items()}
        function_before = dict(daily_adapter_spec.__dict__)
        ids = [ALPHA, SYNTHETIC, SYNTHETIC, ALPHA]
        before = pickle.dumps(ids)
        with patch.object(api_adapters, "_DAILY_ADAPTERS", temporary):
            results = [daily_adapter_spec(value) for value in ids]
            self.assertEqual(results, [alpha, synthetic, synthetic, alpha])
            self.reject("unknown")
            self.assertIs(daily_adapter_spec(ALPHA), alpha)
            self.assertIs(daily_adapter_spec(SYNTHETIC), synthetic)
        self.assertEqual(pickle.dumps(ids), before)
        self.assertEqual({name: id(value) for name, value in vars(api_adapters).items()}, namespace_before)
        self.assertEqual(daily_adapter_spec.__dict__, function_before)

    def test_lookup_never_constructs_adapter_or_calls_request_parser_methods(self):
        alpha, synthetic = self.assert_alpha(), synthetic_spec()
        temporary = MappingProxyType({ALPHA: alpha, SYNTHETIC: synthetic})
        with patch.object(api_adapters, "_DAILY_ADAPTERS", temporary), \
                patch.object(AlphaDailyAdapter, "__init__", side_effect=AssertionError("adapter construction forbidden")) as constructor, \
                patch.object(AlphaDailyAdapter, "build_url", side_effect=AssertionError("URL building forbidden")) as build, \
                patch.object(AlphaDailyAdapter, "parse", side_effect=AssertionError("parse forbidden")) as parse:
            self.assertIs(daily_adapter_spec(ALPHA), alpha)
            self.assertIs(daily_adapter_spec(SYNTHETIC), synthetic)
            self.reject("unknown")
            constructor.assert_not_called()
            build.assert_not_called()
            parse.assert_not_called()

    def test_lookup_has_no_environment_key_state_file_network_clock_or_output_effects(self):
        alpha, synthetic = self.assert_alpha(), synthetic_spec()
        temporary = MappingProxyType({ALPHA: alpha, SYNTHETIC: synthetic})
        with resolver_tests.resolver_side_effect_guards():
            self.assertIs(daily_adapter_spec(ALPHA), alpha)
            self.reject("FAKE-PRIVATE-ID")
            self.reject([])
            self.reject({})
            self.reject(StringSubclass(ALPHA))
            with patch.object(api_adapters, "_DAILY_ADAPTERS", temporary):
                self.assertIs(daily_adapter_spec(SYNTHETIC), synthetic)
                self.assertIs(daily_adapter_spec(ALPHA), alpha)
