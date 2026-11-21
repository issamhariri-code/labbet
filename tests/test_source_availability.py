"""SourceAvailability representation only; no source/rule/calendar binding yet."""
from dataclasses import asdict, fields, FrozenInstanceError, replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
import subprocess
import sys
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

import api_contracts
from api_contracts import ApiFailure, SessionAvailability, SourceAvailability
from test_session_availability import no_availability_side_effects


UTC = timezone.utc
CONTRACT = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
PRIVATE = "FAKE-PRIVATE-SOURCE-AVAILABILITY"
# Handwritten canonical bytes, independently checked with external sha256sum.
# Never derive either constant from SourceAvailability.fingerprint.
REFERENCE_JSON = (
    b'{"contract_fingerprint":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",'
    b'"entries":[["2026-09-29","2026-09-30T00:00:00.000000+00:00"],'
    b'["2026-09-30","2026-10-01T00:00:00.123456+00:00"]],'
    b'"rule_version":"fixture-rule-v1","source_id":"source-a","version":1}'
)
REFERENCE_SHA256 = "9d7ae8cb4491efa7d02f97446248d49cdeb68af55c8c8ce5df9fbfdd12ecf6e0"


class StringSubclass(str):
    def __str__(self):
        raise AssertionError("string subclass must not be converted")

    __repr__ = __str__


class TupleSubclass(tuple):
    def __len__(self):
        raise AssertionError("tuple subclass must not be read")

    def __iter__(self):
        raise AssertionError("tuple subclass must not be read")


class SessionSubclass(SessionAvailability):
    pass


class HostileValue:
    def __str__(self):
        raise AssertionError("invalid input must not be stringified")

    __repr__ = __str__

    def __iter__(self):
        raise AssertionError("invalid input must not be iterated")

    @property
    def session_date(self):
        raise AssertionError("wrong entry type must stop before field reads")


def reference_entries():
    return (SessionAvailability(date(2026, 9, 29), datetime(2026, 9, 30, tzinfo=UTC)),
            SessionAvailability(date(2026, 9, 30), datetime(2026, 10, 1, microsecond=123456, tzinfo=UTC)))


def source(**changes):
    values = dict(source_id="source-a", rule_version="fixture-rule-v1",
                  contract_fingerprint=CONTRACT, entries=reference_entries())
    values.update(changes)
    return SourceAvailability(**values)


def snapshot_input(values):
    """Avoid repr/equality/access hooks on intentionally hostile wrong types."""
    result = []
    for name, value in values.items():
        if type(value) in (tuple, list):
            contents = tuple((id(entry), entry.session_date, entry.not_before)
                             if type(entry) is SessionAvailability else (id(entry),)
                             for entry in value)
            result.append((name, id(value), contents))
        elif type(value) is dict:
            result.append((name, id(value), tuple((id(k), id(v)) for k, v in value.items())))
        else:
            result.append((name, id(value)))
    return tuple(result)


class SourceAvailabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.large_entries = tuple(SessionAvailability(date(2000, 1, 1) + timedelta(days=i),
            datetime(2030, 1, 1, microsecond=i, tzinfo=UTC)) for i in range(10000))
        cls.ny = ZoneInfo("America/New_York")
        cls.stockholm = ZoneInfo("Europe/Stockholm")

    def accept(self, **changes):
        values = dict(source_id="source-a", rule_version="fixture-rule-v1",
                      contract_fingerprint=CONTRACT, entries=reference_entries())
        values.update(changes)
        before = snapshot_input(values)
        result = SourceAvailability(**values)
        self.assertIs(type(result), SourceAvailability)
        self.assertIs(result.entries, values["entries"])
        self.assertEqual(snapshot_input(values), before)
        self.assertIs(type(result.fingerprint), str)
        self.assertRegex(result.fingerprint, r"\A[0-9a-f]{64}\Z")
        self.assertEqual(snapshot_input(values), before)
        return result

    def reject(self, **changes):
        values = dict(source_id="source-a", rule_version="fixture-rule-v1",
                      contract_fingerprint=CONTRACT, entries=reference_entries())
        values.update(changes)
        before = snapshot_input(values)
        with self.assertRaises(ApiFailure) as caught:
            SourceAvailability(**values)
        self.assertIs(type(caught.exception), ApiFailure)
        self.assertEqual(caught.exception.code, "invalid_source_availability")
        self.assertEqual(str(caught.exception), "API-hämtningen stoppades: invalid_source_availability")
        self.assertEqual(repr(caught.exception), "ApiFailure('API-hämtningen stoppades: invalid_source_availability')")
        self.assertIsNone(caught.exception.status)
        self.assertNotIn(PRIVATE, str(caught.exception))
        self.assertEqual(snapshot_input(values), before)

    def test_valid_sources_rules_identifier_boundaries_and_hex_fingerprints(self):
        for sid, rule in (("source-a", "fixture-rule-v1"), ("source-b", "reviewed.rule:2_3"),
                          ("0", "A"), ("Z" * 128, "r" * 128), ("A._:-9", "rule_2")):
            for fingerprint in (CONTRACT, "0" * 64, "f" * 64):
                with self.subTest(source_length=len(sid), rule_length=len(rule)):
                    result = self.accept(source_id=sid, rule_version=rule, contract_fingerprint=fingerprint)
                    self.assertEqual((result.source_id, result.rule_version, result.contract_fingerprint), (sid, rule, fingerprint))

    def test_one_entry_and_ten_thousand_entries_are_accepted(self):
        for entries in (reference_entries()[:1], self.large_entries):
            result = self.accept(entries=entries)
            self.assertEqual(len(result.entries), len(entries))
            self.assertIs(result.entries[0], entries[0])
            self.assertIs(result.entries[-1], entries[-1])
            self.assertEqual(result.fingerprint, result.fingerprint)

    def test_id_wrong_types_and_string_subclasses_are_rejected(self):
        bad = (None, True, 1, 1.5, b"source-a", [], {}, (), date(2026, 1, 1),
               HostileValue(), StringSubclass("source-a"))
        for field in ("source_id", "rule_version"):
            for value in bad:
                with self.subTest(field=field, type=type(value).__name__):
                    self.reject(**{field: value})

    def test_invalid_ids_lengths_characters_whitespace_and_unicode_are_rejected(self):
        bad = ("", "x" * 129, " source-a", "source-a ", "source\n", "source\t", "-source",
               ".source", "_source", ":source", "source/a", "source?x", "source=key", "åsource", "Ａ", PRIVATE + "\n")
        for field in ("source_id", "rule_version"):
            for value in bad:
                with self.subTest(field=field, length=len(value)):
                    self.reject(**{field: value})

    def test_contract_fingerprint_wrong_types_and_string_subclasses_are_rejected(self):
        for value in (None, True, 1, 1.5, CONTRACT.encode(), [], {}, (), HostileValue(), StringSubclass(CONTRACT)):
            with self.subTest(type=type(value).__name__):
                self.reject(contract_fingerprint=value)

    def test_contract_fingerprint_requires_exactly_64_lowercase_hex_characters(self):
        bad = ("", "a" * 63, "a" * 65, CONTRACT.upper(), "g" * 64, "A" * 64,
               "0x" + "a" * 62, " " + "a" * 63, "a" * 63 + "\n", "a" * 63 + "å", PRIVATE)
        for value in bad:
            with self.subTest(length=len(value)):
                self.reject(contract_fingerprint=value)

    def test_outer_entries_wrong_types_and_tuple_subclass_are_rejected_unread(self):
        entries = reference_entries()
        for value in (None, True, 1, 1.5, "entries", b"entries", list(entries), {}, {entries[0]},
                      TupleSubclass(entries), HostileValue()):
            with self.subTest(type=type(value).__name__):
                self.reject(entries=value)
        iterator = iter(entries)
        self.reject(entries=iterator)
        self.assertIs(next(iterator), entries[0])

    def test_empty_and_over_limit_entries_reject_before_reading_entries(self):
        self.reject(entries=())
        oversized = (reference_entries()[0],) * 10001
        original = SessionAvailability.__getattribute__
        def guard(entry, name):
            if name in ("session_date", "not_before"):
                raise AssertionError("oversized collection must reject before reading entries")
            return original(entry, name)
        with patch.object(SessionAvailability, "__getattribute__", guard):
            with self.assertRaises(ApiFailure) as caught:
                SourceAvailability("source-a", "fixture-rule-v1", CONTRACT, oversized)
            self.assertEqual(caught.exception.code, "invalid_source_availability")
        self.assertEqual(len(oversized), 10001)

    def test_wrong_entry_types_subclasses_and_mixed_input_are_rejected_unchanged(self):
        first, last = reference_entries()
        subclass = SessionSubclass(first.session_date, first.not_before)
        for value in (None, True, 1, "entry", {}, [], (first.session_date, first.not_before),
                      first.session_date, first.not_before, HostileValue(), subclass, source()):
            for entries in ((value,), (first, value, last), (first, value)):
                with self.subTest(type=type(value).__name__, position=len(entries)):
                    self.reject(entries=entries)

    def test_validation_order_identifiers_then_fingerprint_then_entries(self):
        entries = TupleSubclass(reference_entries())
        original = api_contracts.identifier
        cases = ((dict(source_id=None, rule_version=None, contract_fingerprint=None, entries=entries), []),
                 (dict(source_id="", rule_version=None, contract_fingerprint=None, entries=entries), [""]),
                 (dict(source_id="source-a", rule_version=None, contract_fingerprint=None, entries=entries), ["source-a"]),
                 (dict(contract_fingerprint=None, entries=entries), ["source-a", "fixture-rule-v1"]))
        for changes, expected in cases:
            with patch.object(api_contracts, "identifier", wraps=original) as identify:
                self.reject(**changes)
                self.assertEqual([call.args[0] for call in identify.call_args_list], expected)
        original_match = api_contracts.re.fullmatch
        def guard_pattern(pattern, *args, **kwargs):
            if pattern == r"[0-9a-f]{64}":
                raise AssertionError("later contract-fingerprint regex forbidden")
            return original_match(pattern, *args, **kwargs)
        with patch.object(api_contracts.re, "fullmatch", side_effect=guard_pattern):
            self.reject(source_id=None, contract_fingerprint=HostileValue(), entries=entries)

    def test_duplicate_session_dates_reject_even_with_different_times(self):
        first, second = reference_entries()
        duplicate = replace(first, not_before=first.not_before + timedelta(microseconds=1))
        for entries in ((first, first), (first, duplicate), (first, second, replace(second))):
            self.reject(entries=entries)

    def test_unsorted_session_dates_reject_without_sorting_or_mutation(self):
        first, second = reference_entries()
        third = replace(second, session_date=date(2026, 10, 1))
        for entries in ((second, first), (first, third, second), (third, second, first)):
            self.reject(entries=entries)

    def test_shared_and_decreasing_not_before_values_are_valid(self):
        first, second = reference_entries()
        shared = (first, replace(second, not_before=first.not_before))
        decreasing = (replace(first, not_before=second.not_before), replace(second, not_before=first.not_before))
        for entries in (shared, decreasing):
            result = self.accept(entries=entries)
            self.assertLess(result.entries[0].session_date, result.entries[1].session_date)
            self.assertGreaterEqual(result.entries[0].not_before, result.entries[1].not_before)

    def test_session_dates_remain_distinct_from_normalized_utc_dates(self):
        first = SessionAvailability(date(2026, 9, 29), datetime(2026, 9, 30, 1, tzinfo=timezone(timedelta(hours=2))))
        second = SessionAvailability(date(2026, 9, 30), datetime(2026, 10, 1, 1, tzinfo=timezone(timedelta(hours=-3))))
        value = self.accept(entries=(first, second))
        self.assertEqual([entry.session_date for entry in value.entries], [date(2026, 9, 29), date(2026, 9, 30)])
        self.assertEqual(value.entries[1].not_before, datetime(2026, 10, 1, 4, tzinfo=UTC))

    def test_frozen_slots_and_nested_collections_and_entries_are_read_only(self):
        value = source()
        before = (asdict(value), value.fingerprint)
        self.assertTrue(value.__dataclass_params__.frozen)
        names = ("source_id", "rule_version", "contract_fingerprint", "entries")
        self.assertEqual(value.__slots__, names)
        self.assertEqual(tuple(field.name for field in fields(value)), names)
        self.assertFalse(hasattr(value, "__dict__"))
        for name in names:
            with self.assertRaises(FrozenInstanceError):
                setattr(value, name, None)
            with self.assertRaises(FrozenInstanceError):
                delattr(value, name)
        with self.assertRaises((TypeError, AttributeError)):
            value.fingerprint = "bad"
        with self.assertRaises(TypeError):
            value.entries[0] = value.entries[1]
        with self.assertRaises(TypeError):
            del value.entries[0]
        with self.assertRaises(FrozenInstanceError):
            value.entries[0].not_before = value.entries[1].not_before
        with self.assertRaises(FrozenInstanceError):
            del value.entries[0].session_date
        self.assertEqual((asdict(value), value.fingerprint), before)

    def test_replace_revalidates_all_bindings_collection_and_date_order(self):
        original = source()
        cases = ({"source_id": ""}, {"rule_version": None}, {"contract_fingerprint": CONTRACT.upper()},
                 {"entries": []}, {"entries": ()}, {"entries": (None,)},
                 {"entries": TupleSubclass(original.entries)}, {"entries": original.entries[::-1]},
                 {"entries": (original.entries[0], original.entries[0])},
                 {"entries": original.entries * 5001})
        before = (asdict(original), original.fingerprint)
        for changes in cases:
            with self.assertRaises(ApiFailure) as caught:
                replace(original, **changes)
            self.assertEqual(caught.exception.code, "invalid_source_availability")
            self.assertEqual(str(caught.exception), "API-hämtningen stoppades: invalid_source_availability")
            self.assertEqual((asdict(original), original.fingerprint), before)
        self.assertIsNot(replace(original), original)
        self.assertEqual(replace(original), original)
        self.assertEqual(replace(original, rule_version="rule-2").rule_version, "rule-2")

    def test_inputs_and_detached_serializations_cannot_change_accepted_object(self):
        entries = reference_entries()
        caller_values = dict(source_id="source-a", rule_version="fixture-rule-v1", contract_fingerprint=CONTRACT, entries=entries)
        original = SourceAvailability(**caller_values)
        before = (asdict(original), original.fingerprint)
        caller_values.update(source_id="source-b", entries=())
        detached = asdict(original)
        detached["entries"][0]["session_date"] = date(2000, 1, 1)
        detached["source_id"] = "source-c"
        self.assertEqual((asdict(original), original.fingerprint), before)
        self.assertIs(original.entries, entries)

    def test_fixed_independent_json_sha256_reference_and_exact_microsecond_format(self):
        self.assertEqual(hashlib.sha256(REFERENCE_JSON).hexdigest(), REFERENCE_SHA256)
        self.assertEqual(source().fingerprint, REFERENCE_SHA256)
        decoded = json.loads(REFERENCE_JSON)
        self.assertEqual(decoded["version"], 1)
        self.assertEqual(set(decoded), {"version", "source_id", "rule_version", "contract_fingerprint", "entries"})
        self.assertEqual(decoded["entries"], [["2026-09-29", "2026-09-30T00:00:00.000000+00:00"],
                                            ["2026-09-30", "2026-10-01T00:00:00.123456+00:00"]])
        self.assertEqual(json.dumps(decoded, sort_keys=True, separators=(",", ":")).encode(), REFERENCE_JSON)

    def test_each_binding_changes_fingerprint(self):
        original = source()
        for changes in ({"source_id": "source-b"}, {"rule_version": "fixture-rule-v2"}, {"contract_fingerprint": "f" * 64}):
            changed = replace(original, **changes)
            self.assertNotEqual(changed.fingerprint, REFERENCE_SHA256)
            self.assertNotEqual(changed.fingerprint, original.fingerprint)
        self.assertEqual(original.fingerprint, REFERENCE_SHA256)

    def test_each_session_date_and_each_microsecond_changes_fingerprint(self):
        original = source()
        results = {original.fingerprint}
        for index in range(len(original.entries)):
            for change in ({"session_date": original.entries[index].session_date + timedelta(days=-1 if index == 0 else 1)},
                           {"not_before": original.entries[index].not_before + timedelta(microseconds=1)}):
                entries = list(original.entries)
                entries[index] = replace(entries[index], **change)
                changed = replace(original, entries=tuple(entries))
                self.assertNotIn(changed.fingerprint, results)
                results.add(changed.fingerprint)
        self.assertEqual(len(results), 5)

    def test_entry_count_changes_fingerprint(self):
        original = source()
        third = replace(original.entries[-1], session_date=date(2026, 10, 1))
        variants = (original, replace(original, entries=original.entries[:1]),
                    replace(original, entries=original.entries[1:]), replace(original, entries=original.entries + (third,)))
        self.assertEqual(len({value.fingerprint for value in variants}), 4)

    def test_equivalent_normalized_offsets_and_named_zones_give_same_fingerprint(self):
        expected = source()
        variants = ((datetime(2026, 9, 30, tzinfo=UTC), datetime(2026, 10, 1, microsecond=123456, tzinfo=UTC)),
                    (datetime(2026, 9, 30, 2, tzinfo=timezone(timedelta(hours=2))),
                     datetime(2026, 10, 1, 2, microsecond=123456, tzinfo=timezone(timedelta(hours=2)))),
                    (datetime(2026, 9, 29, 20, tzinfo=self.ny), datetime(2026, 9, 30, 20, microsecond=123456, tzinfo=self.ny)),
                    (datetime(2026, 9, 30, 2, tzinfo=self.stockholm), datetime(2026, 10, 1, 2, microsecond=123456, tzinfo=self.stockholm)))
        for times in variants:
            entries = tuple(SessionAvailability(entry.session_date, instant) for entry, instant in zip(expected.entries, times))
            value = source(entries=entries)
            self.assertEqual(value, expected)
            self.assertEqual(value.fingerprint, REFERENCE_SHA256)

    def test_fingerprint_is_stable_across_processes_hash_seeds_and_timezones(self):
        code = '''from datetime import date,datetime,timezone,timedelta
from zoneinfo import ZoneInfo
import sys
from api_contracts import SessionAvailability,SourceAvailability
mode=sys.argv[1]
if mode=="utc":
    a=datetime(2026,9,30,tzinfo=timezone.utc); b=datetime(2026,10,1,microsecond=123456,tzinfo=timezone.utc)
elif mode=="offset":
    z=timezone(timedelta(hours=2)); a=datetime(2026,9,30,2,tzinfo=z); b=datetime(2026,10,1,2,microsecond=123456,tzinfo=z)
else:
    z=ZoneInfo("America/New_York"); a=datetime(2026,9,29,20,tzinfo=z); b=datetime(2026,9,30,20,microsecond=123456,tzinfo=z)
entries=(SessionAvailability(date(2026,9,29),a),SessionAvailability(date(2026,9,30),b))
value=SourceAvailability("source-a","fixture-rule-v1","0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",entries)
print(value.fingerprint)
'''
        for seed, mode, local_tz in (("0", "utc", "UTC"), ("1", "offset", "Pacific/Honolulu"),
                                    ("12345", "named", "Europe/Stockholm"), ("random", "named", "America/New_York")):
            env = dict(os.environ, PYTHONHASHSEED=seed, TZ=local_tz)
            result = subprocess.run([sys.executable, "-B", "-c", code, mode], env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual((result.returncode, result.stdout, result.stderr), (0, REFERENCE_SHA256 + "\n", ""))

    def test_construction_and_fingerprint_have_no_clock_or_other_side_effects(self):
        small = reference_entries()
        before = tuple((entry.session_date, entry.not_before) for entry in small)
        expected = source()
        large_hash = source(entries=self.large_entries).fingerprint
        with no_availability_side_effects():
            value = SourceAvailability("source-a", "fixture-rule-v1", CONTRACT, small)
            self.assertEqual(value, expected)
            self.assertEqual(value.fingerprint, REFERENCE_SHA256)
            self.assertEqual(value.fingerprint, REFERENCE_SHA256)
            large = SourceAvailability("source-a", "fixture-rule-v1", CONTRACT, self.large_entries)
            self.assertEqual(large.fingerprint, large_hash)
            for changes in ({"source_id": None}, {"contract_fingerprint": None}, {"entries": ()}, {"entries": (None,)}, {"entries": small[::-1]}):
                values = dict(source_id="source-a", rule_version="fixture-rule-v1", contract_fingerprint=CONTRACT, entries=small)
                values.update(changes)
                with self.assertRaises(ApiFailure) as caught:
                    SourceAvailability(**values)
                self.assertEqual(caught.exception.code, "invalid_source_availability")
        self.assertEqual(tuple((entry.session_date, entry.not_before) for entry in small), before)
        self.assertIs(api_contracts.date, date)
        self.assertIs(api_contracts.datetime, datetime)


if __name__ == "__main__":
    unittest.main()
