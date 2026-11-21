"""Decoded JSON quota contract; loader and inventory are outside this scope."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, asdict, replace
from decimal import Decimal
from fractions import Fraction
import json
import math
import pickle
import unittest

from ingestion.api_profiles import ProfileError, QuotaLimit, quota_limits_from_json
from test_api_profiles import profile


class ListSubclass(list):
    pass


class DictSubclass(dict):
    pass


class IntSubclass(int):
    pass


class FloatSubclass(float):
    pass


def rule(credits=8, seconds=60):
    return {"max_credits": credits, "window_seconds": seconds}


class QuotaLimitsFromJsonTests(unittest.TestCase):
    @staticmethod
    def input_snapshot(values):
        # Preserve types, NaN payloads and dictionary insertion order as well
        # as contents; these are only test-owned objects, never untrusted data.
        references = tuple(map(id, values)) if isinstance(values, list) else ()
        return pickle.dumps(values), references

    def accept(self, values):
        before = self.input_snapshot(values)
        result = quota_limits_from_json(values)
        self.assertEqual(self.input_snapshot(values), before)
        self.assertIs(type(result), tuple)
        for limit in result:
            self.assertIs(type(limit), QuotaLimit)
            self.assertIs(type(limit.max_credits), int)
            self.assertIs(type(limit.window_seconds), float)
        return result

    def reject(self, values, code):
        before = self.input_snapshot(values)
        with self.assertRaises(ProfileError) as caught:
            quota_limits_from_json(values)
        self.assertEqual(caught.exception.code, code)
        self.assertEqual(str(caught.exception), "API configuration rejected: " + code)
        self.assertEqual(self.input_snapshot(values), before)

    def test_empty_list_returns_empty_tuple(self):
        self.assertEqual(self.accept([]), ())

    def test_one_rule_returns_real_quota_and_normalized_float(self):
        self.assertEqual(self.accept([rule()]), (QuotaLimit(8, 60.0),))

    def test_multiple_rules_preserve_order_and_duplicates(self):
        values = [rule(800, 86400), rule(8, 60), rule(7, 60), rule(8, 60)]
        limits = self.accept(values)
        self.assertEqual(limits, (QuotaLimit(800, 86400), QuotaLimit(8, 60),
                                  QuotaLimit(7, 60), QuotaLimit(8, 60)))
        self.assertEqual(self.accept(values[::-1]), limits[::-1])

    def test_outer_container_types_are_strict(self):
        invalid = (None, (), {}, rule(), "[]", 0, 1.5, True,
                   ListSubclass(), ListSubclass([rule()]))
        for index, values in enumerate(invalid):
            with self.subTest(index=index, type=type(values).__name__):
                self.reject(values, "invalid_quota_limits")

    def test_element_types_and_dict_subclasses_are_rejected(self):
        invalid = (None, [], (), "rule", 8, 8.0, True,
                   DictSubclass(rule()), DictSubclass())
        for index, value in enumerate(invalid):
            with self.subTest(index=index, type=type(value).__name__):
                self.reject([value], "invalid_quota_limits")

    def test_missing_extra_and_wrong_keys_are_rejected(self):
        invalid = ({}, {"max_credits": 8}, {"window_seconds": 60},
                   dict(rule(), extra=1), {"credits": 8, "window_seconds": 60},
                   {"max_credits": 8, "window_seconds": 60, 0: 1})
        for index, value in enumerate(invalid):
            with self.subTest(index=index):
                self.reject([value], "invalid_quota_limits")

    def test_credit_boundaries_are_accepted(self):
        for credits in (1, 8, 800, 100000):
            with self.subTest(credits=credits):
                self.assertEqual(self.accept([rule(credits)])[0].max_credits, credits)

    def test_out_of_range_credits_are_rejected(self):
        for index, credits in enumerate((0, -1, -100000, 100001, 10**1000)):
            with self.subTest(index=index):
                self.reject([rule(credits)], "invalid_resource_limit")

    def test_credit_types_and_numeric_subclasses_are_rejected(self):
        invalid = (1.0, 8.5, True, False, "8", None, [], {}, (),
                   Decimal("8"), Fraction(8), complex(8), IntSubclass(8),
                   FloatSubclass(8))
        for index, credits in enumerate(invalid):
            with self.subTest(index=index, type=type(credits).__name__):
                self.reject([rule(credits)], "invalid_resource_limit")

    def test_time_boundaries_and_small_fractions_are_accepted(self):
        valid = (math.nextafter(0.0, 1.0), 1e-12, 0.25, 1, 60, 60.0,
                 math.nextafter(31536000.0, 0.0), 31536000, 31536000.0)
        for seconds in valid:
            with self.subTest(seconds=seconds):
                self.assertEqual(self.accept([rule(seconds=seconds)])[0].window_seconds,
                                 float(seconds))

    def test_out_of_range_times_are_rejected(self):
        invalid = (0, 0.0, -0.0, -1, -0.25, 31536001,
                   math.nextafter(31536000.0, math.inf), 10**1000)
        for index, seconds in enumerate(invalid):
            with self.subTest(index=index):
                self.reject([rule(seconds=seconds)], "invalid_resource_limit")

    def test_time_types_and_numeric_subclasses_are_rejected(self):
        invalid = (True, False, "60", None, [], {}, (), Decimal("60"),
                   Fraction(1, 2), complex(60), IntSubclass(60), FloatSubclass(60))
        for index, seconds in enumerate(invalid):
            with self.subTest(index=index, type=type(seconds).__name__):
                self.reject([rule(seconds=seconds)], "invalid_resource_limit")

    def test_nan_and_both_infinities_are_rejected_in_both_fields(self):
        for value in (math.nan, math.inf, -math.inf):
            for field in ("max_credits", "window_seconds"):
                with self.subTest(value=str(value), field=field):
                    value_dict = rule()
                    value_dict[field] = value
                    self.reject([value_dict], "invalid_resource_limit")

    def test_accepted_input_keeps_values_types_order_and_references(self):
        first, second = rule(800, 86400), rule(8, 60)
        values = [first, second, first]
        original = deepcopy(values)
        self.accept(values)
        self.assertEqual(values, original)
        self.assertIs(values[0], first)
        self.assertIs(values[1], second)
        self.assertIs(values[2], first)
        self.assertIs(type(first["window_seconds"]), int)

    def test_mixed_valid_invalid_rules_do_not_mutate_input(self):
        for invalid, code in ((None, "invalid_quota_limits"),
                              ({"max_credits": 8}, "invalid_quota_limits"),
                              (rule(0), "invalid_resource_limit"),
                              (rule(seconds=math.nan), "invalid_resource_limit")):
            for position in range(3):
                with self.subTest(code=code, position=position, invalid_type=type(invalid).__name__):
                    values = [rule(800, 86400), rule(8, 60)]
                    values.insert(position, invalid)
                    self.reject(values, code)

    def test_later_input_mutation_cannot_change_result(self):
        values = [rule(800, 86400), rule(8, 60)]
        result = self.accept(values)
        expected = (QuotaLimit(800, 86400), QuotaLimit(8, 60))
        first, second = values
        first["max_credits"] = 1
        second["window_seconds"] = 1
        values.reverse()
        values.append(rule(2, 2))
        values.clear()
        self.assertEqual(result, expected)

    def test_result_tuple_and_quota_objects_are_immutable(self):
        result = self.accept([rule()])
        with self.assertRaises(TypeError):
            result[0] = QuotaLimit(1, 1)
        for name, value in (("max_credits", 1), ("window_seconds", 1)):
            with self.subTest(field=name):
                with self.assertRaises(FrozenInstanceError):
                    setattr(result[0], name, value)
                with self.assertRaises(FrozenInstanceError):
                    delattr(result[0], name)
        self.assertEqual(result, (QuotaLimit(8, 60),))

    def test_real_profile_sorts_only_after_helper_and_forwards_every_rule(self):
        limits = self.accept([rule(800, 86400), rule(8, 60), rule(7, 60), rule(8, 60)])
        p = profile(extra_quota_limits=limits)
        expected = (QuotaLimit(7, 60), QuotaLimit(8, 60),
                    QuotaLimit(8, 60), QuotaLimit(800, 86400))
        self.assertEqual(p.extra_quota_limits, expected)
        self.assertEqual(p.quota_limits, (QuotaLimit(10, 60),) + expected)
        self.assertEqual(limits[0], QuotaLimit(800, 86400))

    def test_empty_helper_result_preserves_legacy_metadata_hash_and_cache(self):
        cases = (
            ({}, "635398a5e367cc1327c82aa3793afdb282e5655934249f4af9941842a93cfd83"),
            ({"quota_window_seconds": 60}, "635398a5e367cc1327c82aa3793afdb282e5655934249f4af9941842a93cfd83"),
            ({"source_id": "source-b", "account_ref": "account-b"},
             "20801b82b89f4ea3bc6aff547618b83c473ce5392366ea548ea943b7971ecbe4"),
        )
        for changes, fingerprint in cases:
            with self.subTest(changes=changes):
                baseline = profile(**changes)
                p = profile(**changes, extra_quota_limits=self.accept([]))
                self.assertNotIn("extra_quota_limits", p.metadata())
                self.assertEqual(p.metadata(), baseline.metadata())
                self.assertEqual(p.config_hash, fingerprint)
                self.assertEqual(p.cache_namespace, baseline.cache_namespace)
                self.assertEqual(p.cache_namespace, p.source_id + ":" + fingerprint)

    def test_equivalent_order_and_time_have_same_hash_but_changed_rules_differ(self):
        values = [rule(800, 86400), rule(8, 60)]
        p = profile(extra_quota_limits=self.accept(values))
        reverse = profile(extra_quota_limits=self.accept(values[::-1]))
        floats = profile(extra_quota_limits=self.accept([rule(800, 86400.0), rule(8, 60.0)]))
        self.assertEqual(p.config_hash, reverse.config_hash)
        self.assertEqual(p.config_hash, floats.config_hash)
        for changed in ([rule(801, 86400), rule(8, 60)],
                        [rule(800, 86401), rule(8, 60)],
                        [rule(800, 86400)], values + [rule(8, 60)]):
            with self.subTest(changed=changed):
                other = profile(extra_quota_limits=self.accept(changed))
                self.assertNotEqual(p.config_hash, other.config_hash)
                self.assertNotEqual(p.cache_namespace, other.cache_namespace)

    def test_extra_rules_round_trip_through_json_and_helper(self):
        p = profile(extra_quota_limits=self.accept(
            [rule(800, 86400), rule(8, 60), rule(8, 60)]))
        decoded = json.loads(json.dumps(p.metadata(), allow_nan=False))
        self.assertIs(type(decoded["extra_quota_limits"]), list)
        limits = self.accept(decoded["extra_quota_limits"])
        restored = replace(p, extra_quota_limits=limits)
        self.assertEqual(tuple(asdict(limit) for limit in limits),
                         p.metadata()["extra_quota_limits"])
        self.assertEqual(restored.metadata(), p.metadata())
        self.assertEqual(restored.config_hash, p.config_hash)
        self.assertEqual(restored.cache_namespace, p.cache_namespace)
        self.assertEqual(restored.quota_limits, p.quota_limits)
