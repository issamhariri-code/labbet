"""Pure resolver contract; raw profile doubles are explicitly labelled below."""
from collections.abc import Mapping
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass, replace
import io
import os
from pathlib import Path
import pickle
import sqlite3
import unittest
from unittest.mock import patch
from urllib.parse import parse_qsl, urlsplit

import api_adapters
from api_adapters import AlphaDailyAdapter, resolve_daily_adapter
from api_contracts import ApiFailure
from api_profiles import CredentialStore
from api_state import StateStore
from test_api_client import contract, profile
import test_alpha_daily_adapter as adapter_tests


ALPHA = "alphavantage-daily-v1"
PRODUCTION_ENDPOINT = "https://www.alphavantage.co/query"


class StringSubclass(str):
    pass


@dataclass(frozen=True, slots=True)
class RawResolverProfileDouble:
    """Resolver-only data; intentionally bypasses SourceProfile validation."""
    adapter_id: object = ALPHA
    environment: str = "production"
    endpoint: object = PRODUCTION_ENDPOINT
    currency: str = "USD"
    exchange: str = "TESTX"
    timezone: str = "UTC"
    price_basis: str = "raw"
    schema_version: str = ALPHA

    @property
    def credential_env(self):
        raise AssertionError("credential reference read forbidden")

    @property
    def credential_identity(self):
        raise AssertionError("credential identity read forbidden")

    @property
    def secret(self):
        raise AssertionError("secret read forbidden")

    @property
    def credentials(self):
        raise AssertionError("credential store read forbidden")


class NoEnvironment(Mapping):
    """Any environment read, including iteration, fails immediately."""
    def __getitem__(self, key):
        raise AssertionError("environment read forbidden")

    def __iter__(self):
        raise AssertionError("environment iteration forbidden")

    def __len__(self):
        raise AssertionError("environment length read forbidden")


class IdOnlyProfileDouble:
    """Bad-ID test double: no later field may be read after rejection."""
    adapter_id = "FAKE-UNKNOWN-ID"

    def __getattr__(self, name):
        raise AssertionError("later profile field read forbidden")


class NoContractReadDouble:
    """Bad-production-endpoint probe; contract must remain untouched."""
    def __getattr__(self, name):
        raise AssertionError("contract field read forbidden")


@contextmanager
def resolver_side_effect_guards():
    # The existing adapter guards cover DNS/socket/HTTP, time/sleep,
    # parser datetime/date reads, files, logging, print and stdout/stderr.
    # These additions cover environment, alternate file entry points,
    # credentials and durable state, without creating a database.
    with adapter_tests.no_side_effects(), ExitStack() as stack:
        for target, name in ((os, "getenv"), (os, "open"), (io, "open"), (Path, "open"),
                             (sqlite3, "connect"), (StateStore, "__init__"),
                             (StateStore, "_transaction"), (StateStore, "register"),
                             (StateStore, "register_credential"), (StateStore, "acquire"),
                             (CredentialStore, "__init__"), (CredentialStore, "snapshot"),
                             (CredentialStore, "install"), (CredentialStore, "assert_active")):
            stack.enter_context(patch.object(target, name, side_effect=AssertionError(name + " forbidden")))
        stack.enter_context(patch.object(os, "environ", NoEnvironment()))
        stack.enter_context(patch.object(api_adapters, "datetime", adapter_tests.NoClockDateTime, create=True))
        stack.enter_context(patch.object(api_adapters, "date", adapter_tests.NoClockDate, create=True))
        yield


class ResolveDailyAdapterTests(unittest.TestCase):
    def resolve(self, p, c):
        before = pickle.dumps((p, c))
        result = resolve_daily_adapter(p, c)
        self.assertEqual(pickle.dumps((p, c)), before)
        self.assertIs(type(result), AlphaDailyAdapter)
        self.assertEqual(result.adapter_id, ALPHA)
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

    def test_implicit_explicit_alpha_resolves_in_test_and_production(self):
        c = contract()
        for environment in ("test", "production"):
            for explicit in (False, True):
                with self.subTest(environment=environment, explicit=explicit):
                    endpoint = PRODUCTION_ENDPOINT if environment == "production" else "https://source-a.invalid/query"
                    p = profile(environment=environment, endpoint=endpoint,
                                **({"adapter_id": ALPHA} if explicit else {}))
                    self.resolve(p, c)

    def test_test_profiles_keep_synthetic_endpoints_without_network(self):
        for endpoint in ("https://source-a.invalid/query", "https://localhost:8443/query", "https://127.0.0.1:54321/test"):
            with self.subTest(endpoint=endpoint):
                self.resolve(profile(endpoint=endpoint), contract())

    def test_unknown_ids_types_and_string_subclasses_are_unsupported(self):
        invalid = ("", "unknown", "alphavantage-daily-v2", "twelve-daily-v1",
                   ALPHA.upper(), " " + ALPHA, ALPHA + " ", "FAKE-PRIVATE-ID\nDO-NOT-ECHO",
                   None, True, False, 0, 1.0, b"alphavantage-daily-v1", [], {}, (),
                   StringSubclass(ALPHA), StringSubclass("FAKE-UNKNOWN-ID"))
        for environment in ("test", "production"):
            for index, value in enumerate(invalid):
                with self.subTest(environment=environment, index=index, type=type(value).__name__):
                    self.reject(RawResolverProfileDouble(adapter_id=value, environment=environment),
                                contract(), "unsupported_adapter")

    def test_real_production_profiles_require_exact_host_path_and_port(self):
        invalid = ("https://example.test/query", "https://api.alphavantage.co/query",
                   "https://www.alphavantage.co.attacker.invalid/query",
                   "https://www.alphavantage.co/", "https://www.alphavantage.co/Query",
                   "https://www.alphavantage.co/query/", "https://www.alphavantage.co/v2/query",
                   "https://www.alphavantage.co/query/../query",
                   "https://www.alphavantage.co/%71uery",
                   "https://www.alphavantage.co:443/query", "https://www.alphavantage.co:8443/query",
                   "https://www.alphavantage.co:80/query")
        for endpoint in invalid:
            with self.subTest(endpoint=endpoint):
                self.reject(profile(environment="production", endpoint=endpoint), contract(), "adapter_endpoint_mismatch")

    def test_added_url_parts_reject_via_labelled_raw_profile_double(self):
        invalid = (PRODUCTION_ENDPOINT + "?", PRODUCTION_ENDPOINT + "?apikey=FAKE-PRIVATE-KEY",
                   PRODUCTION_ENDPOINT + "#", PRODUCTION_ENDPOINT + "#FAKE-PRIVATE-FRAGMENT",
                   PRODUCTION_ENDPOINT + "?x=1#fragment", PRODUCTION_ENDPOINT + ";extra",
                   "https://FAKE-PRIVATE-USER@www.alphavantage.co/query",
                   "https://www.alphavantage.co:/query", "http://www.alphavantage.co/query",
                   "//www.alphavantage.co/query", PRODUCTION_ENDPOINT + " ",
                   "https://WWW.ALPHAVANTAGE.CO/query")
        for endpoint in invalid:
            with self.subTest(endpoint=endpoint):
                self.reject(RawResolverProfileDouble(endpoint=endpoint), contract(), "adapter_endpoint_mismatch")

    def test_each_of_five_comparison_fields_rejects_in_both_directions(self):
        changes = (("currency", "EUR"), ("exchange", "OTHER"), ("timezone", "Europe/Stockholm"),
                   ("price_basis", "adjusted"), ("schema_version", "other-daily-v2"))
        for name, value in changes:
            for side in ("profile", "contract"):
                with self.subTest(field=name, side=side):
                    p = profile(**({name: value} if side == "profile" else {}))
                    c = contract(**({name: value} if side == "contract" else {}))
                    self.reject(p, c, "source_contract_mismatch")

    def test_matching_adjusted_or_other_schema_is_adapter_contract_mismatch(self):
        for environment in ("test", "production"):
            for changes in ({"price_basis": "adjusted"}, {"schema_version": "other-daily-v2"},
                            {"price_basis": "adjusted", "schema_version": "other-daily-v2"}):
                with self.subTest(environment=environment, changes=changes):
                    p = profile(environment=environment, endpoint=PRODUCTION_ENDPOINT, **changes)
                    self.reject(p, contract(**changes), "adapter_contract_mismatch")

    def test_rejection_priority_is_stable_for_multiple_invalid_inputs(self):
        c = contract(currency="EUR", price_basis="adjusted")
        self.reject(RawResolverProfileDouble(adapter_id="FAKE-BAD-ID", endpoint="https://FAKE-HOST.invalid/"),
                    c, "unsupported_adapter")
        self.reject(RawResolverProfileDouble(endpoint="https://FAKE-HOST.invalid/"), c, "adapter_endpoint_mismatch")
        self.reject(profile(price_basis="adjusted"), c, "source_contract_mismatch")

    def test_unknown_id_does_not_read_later_profile_or_contract_fields(self):
        with self.assertRaises(ApiFailure) as caught:
            resolve_daily_adapter(IdOnlyProfileDouble(), NoContractReadDouble())
        self.assertEqual(caught.exception.code, "unsupported_adapter")
        self.assertEqual(str(caught.exception), "API-hämtningen stoppades: unsupported_adapter")

    def test_wrong_production_endpoint_does_not_read_contract(self):
        p = profile(environment="production", endpoint="https://wrong.invalid/query")
        before = pickle.dumps(p)
        with self.assertRaises(ApiFailure) as caught:
            resolve_daily_adapter(p, NoContractReadDouble())
        self.assertEqual(caught.exception.code, "adapter_endpoint_mismatch")
        self.assertEqual(pickle.dumps(p), before)

    def test_alternating_profiles_are_unchanged_and_results_retain_no_request_state(self):
        p = profile(source_id="a", endpoint="https://a.invalid/query")
        c = contract(symbol="AAA", instrument_id="TESTX:AAA")
        q = profile(source_id="b", environment="production", endpoint=PRODUCTION_ENDPOINT,
                    currency="EUR", exchange="OTHER", timezone="Europe/Stockholm")
        d = contract(symbol="BBB", instrument_id="OTHER:BBB", currency="EUR",
                     exchange="OTHER", timezone="Europe/Stockholm")
        before = pickle.dumps((p, c, q, d))
        globals_before = {name: id(value) for name, value in vars(api_adapters).items()}
        class_before = {name: id(value) for name, value in vars(AlphaDailyAdapter).items()}
        function_before = dict(resolve_daily_adapter.__dict__)
        results = []
        for source, market, key in ((p, c, "FAKE-A"), (q, d, "FAKE-B"),
                                    (q, d, "FAKE-B"), (p, c, "FAKE-A")):
            adapter = self.resolve(source, market)
            self.assertFalse(hasattr(adapter, "__dict__"))
            for name in ("profile", "contract", "secret", "credentials", "last_request"):
                self.assertFalse(hasattr(adapter, name))
            url = adapter.build_url(source.endpoint, market.symbol, key)
            self.assertEqual(dict(parse_qsl(urlsplit(url).query))["symbol"], market.symbol)
            self.assertEqual(dict(parse_qsl(urlsplit(url).query))["apikey"], key)
            self.assertEqual(urlsplit(url).netloc, urlsplit(source.endpoint).netloc)
            results.append(adapter)
        self.assertEqual(len({id(value) for value in results}), 4)
        self.assertEqual(pickle.dumps((p, c, q, d)), before)
        self.assertEqual({name: id(value) for name, value in vars(api_adapters).items()}, globals_before)
        self.assertEqual({name: id(value) for name, value in vars(AlphaDailyAdapter).items()}, class_before)
        self.assertEqual(resolve_daily_adapter.__dict__, function_before)

    def test_failures_do_not_change_subsequent_valid_resolution(self):
        p, c = profile(), contract()
        first = self.resolve(p, c)
        self.reject(RawResolverProfileDouble(adapter_id="unknown"), c, "unsupported_adapter")
        self.reject(RawResolverProfileDouble(endpoint="https://other.invalid/"), c, "adapter_endpoint_mismatch")
        self.reject(replace(p, currency="EUR"), c, "source_contract_mismatch")
        second = self.resolve(p, c)
        self.assertIsNot(first, second)
        self.assertEqual(first.build_url(p.endpoint, c.symbol, "FAKE-A"),
                         second.build_url(p.endpoint, c.symbol, "FAKE-A"))

    def test_resolver_never_reads_credential_properties(self):
        # All credential-related properties on this marked double raise.
        self.resolve(RawResolverProfileDouble(), contract())
        self.resolve(RawResolverProfileDouble(environment="test", endpoint="https://test.invalid/query"), contract())
        self.reject(RawResolverProfileDouble(currency="EUR"), contract(), "source_contract_mismatch")

    def test_no_environment_clock_files_network_logging_or_state_for_all_outcomes(self):
        c = contract()
        good = (profile(), profile(environment="production", endpoint=PRODUCTION_ENDPOINT),
                RawResolverProfileDouble())
        bad = ((RawResolverProfileDouble(adapter_id="FAKE-BAD-ID"), c, "unsupported_adapter"),
               (RawResolverProfileDouble(endpoint="https://FAKE-BAD-HOST.invalid/"), c, "adapter_endpoint_mismatch"),
               (profile(currency="EUR"), c, "source_contract_mismatch"),
               (profile(price_basis="adjusted"), contract(price_basis="adjusted"), "adapter_contract_mismatch"))
        with resolver_side_effect_guards():
            for p in good:
                self.resolve(p, c)
            for p, market, code in bad:
                self.reject(p, market, code)
