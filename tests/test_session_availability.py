"""Standalone availability normalization; no calendar/source/fetch integration."""
from contextlib import contextmanager, ExitStack, redirect_stderr, redirect_stdout
from dataclasses import asdict, fields, FrozenInstanceError, replace
from datetime import date, datetime, timedelta, timezone, tzinfo
import http.client
import io
import logging
import os
from pathlib import Path
import socket
import sqlite3
import sys
import time
import traceback
import unittest
from unittest.mock import patch
import urllib.request
from zoneinfo import ZoneInfo

import api_contracts
from api_contracts import ApiFailure, SessionAvailability
from api_profiles import CredentialStore
from api_state import StateStore


UTC = timezone.utc
DAY = date(2026, 1, 15)
INSTANT = datetime(2026, 1, 15, 12, 34, 56, 123456, tzinfo=UTC)
PRIVATE = "FAKE-PRIVATE-CONVERSION-DATA"


class DateSubclass(date):
    pass


class DateTimeSubclass(datetime):
    pass


class HostileValue:
    def __repr__(self):
        raise AssertionError("invalid input repr must not be called")

    __str__ = __repr__


class NoEnvironment:
    def __getattribute__(self, name):
        raise AssertionError("environment access forbidden")

    def __getitem__(self, key):
        raise AssertionError("environment access forbidden")

    def __iter__(self):
        raise AssertionError("environment access forbidden")


class OffsetResult(tzinfo):
    def __init__(self, result):
        self.result = result

    def utcoffset(self, value):
        return self.result

    def dst(self, value):
        return timedelta(0)


class OffsetError(tzinfo):
    def __init__(self, error):
        self.error = error

    def utcoffset(self, value):
        raise self.error


class FromUtcError(tzinfo):
    def __init__(self, error):
        self.error = error

    def utcoffset(self, value):
        return timedelta(hours=1)

    def dst(self, value):
        return timedelta(0)

    def fromutc(self, value):
        raise self.error


class BadRoundTrip(tzinfo):
    def __init__(self, shift):
        self.shift = shift

    def utcoffset(self, value):
        return timedelta(0)

    def dst(self, value):
        return timedelta(0)

    def fromutc(self, value):
        return value + self.shift


class InvalidFromUtc(tzinfo):
    def __init__(self, result):
        self.result = result

    def utcoffset(self, value):
        return timedelta(0)

    def dst(self, value):
        return timedelta(0)

    def fromutc(self, value):
        return self.result


def components(value):
    """Snapshot even a datetime whose tzinfo refuses offset/string conversion."""
    if type(value) is datetime:
        return (value.year, value.month, value.day, value.hour, value.minute,
                value.second, value.microsecond, value.tzinfo, value.fold)
    return id(value)


@contextmanager
def block_calendar_clock():
    """Intercept actual C clock methods without replacing exact date types."""
    previous = sys.getprofile()
    def guard(frame, event, function):
        if event == "c_call" and getattr(function, "__self__", None) in (date, datetime):
            if getattr(function, "__name__", "") in ("now", "utcnow", "today"):
                raise AssertionError("calendar clock read forbidden")
    sys.setprofile(guard)
    try:
        yield
    finally:
        sys.setprofile(previous)


@contextmanager
def no_availability_side_effects():
    output, errors = io.StringIO(), io.StringIO()
    blocked = ((os, "getenv"), (os, "open"), (io, "open"), (Path, "open"),
        (socket, "create_connection"), (socket, "getaddrinfo"),
        (socket.socket, "connect"), (socket.socket, "connect_ex"),
        (http.client.HTTPConnection, "request"), (urllib.request, "urlopen"),
        (sqlite3, "connect"), (StateStore, "__init__"), (StateStore, "_transaction"),
        (StateStore, "register"), (StateStore, "acquire"),
        (CredentialStore, "__init__"), (CredentialStore, "install"),
        (CredentialStore, "snapshot"), (CredentialStore, "assert_active"),
        (time, "time"), (time, "time_ns"), (time, "monotonic"),
        (time, "monotonic_ns"), (time, "perf_counter"), (time, "perf_counter_ns"),
        (time, "process_time"), (time, "sleep"), (logging.Logger, "_log"))
    with ExitStack() as stack:
        stack.enter_context(redirect_stdout(output))
        stack.enter_context(redirect_stderr(errors))
        for target, name in blocked:
            stack.enter_context(patch.object(target, name, side_effect=AssertionError(name + " forbidden")))
        stack.enter_context(patch("builtins.open", side_effect=AssertionError("file I/O forbidden")))
        stack.enter_context(patch("builtins.print", side_effect=AssertionError("output forbidden")))
        stack.enter_context(patch.object(os, "environ", NoEnvironment()))
        stack.enter_context(block_calendar_clock())
        yield
    if output.getvalue() or errors.getvalue():
        raise AssertionError("availability wrote output")


class SessionAvailabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Prepare timezone data before side-effect guards, never replace datetime.
        cls.zones = {key: ZoneInfo(key) for key in ("UTC", "Europe/Stockholm", "America/New_York",
            "Asia/Kathmandu", "Australia/Sydney", "Australia/Lord_Howe", "Pacific/Apia")}

    def accept(self, day, local, expected):
        before = components(local)
        result = SessionAvailability(day, local)
        self.assertIs(type(result.session_date), date)
        self.assertIs(result.session_date, day)
        self.assertIs(type(result.not_before), datetime)
        self.assertIs(result.not_before.tzinfo, UTC)
        self.assertEqual(result.not_before.fold, 0)
        self.assertEqual(result.not_before, expected)
        self.assertEqual(components(local), before)
        return result

    def reject(self, day, local):
        before = (id(day), components(local))
        with self.assertRaises(ApiFailure) as caught:
            SessionAvailability(day, local)
        self.assertIs(type(caught.exception), ApiFailure)
        self.assertEqual(caught.exception.code, "invalid_availability")
        self.assertEqual(str(caught.exception), "API-hämtningen stoppades: invalid_availability")
        self.assertEqual(repr(caught.exception), "ApiFailure('API-hämtningen stoppades: invalid_availability')")
        self.assertIsNone(caught.exception.status)
        visible = "".join(traceback.format_exception(caught.exception))
        self.assertNotIn(PRIVATE, visible)
        self.assertEqual((id(day), components(local)), before)

    def test_utc_normalizes_without_losing_microseconds(self):
        for micros in (0, 1, 123456, 999999):
            with self.subTest(microseconds=micros):
                local = INSTANT.replace(microsecond=micros)
                result = self.accept(DAY, local, local)
                self.assertEqual(result.not_before.microsecond, micros)

    def test_fixed_offsets_match_explicit_utc_expectations(self):
        cases = ((timedelta(0), (12, 34)), (timedelta(hours=5, minutes=45), (18, 19)),
                 (timedelta(hours=-3, minutes=-30), (9, 4)), (timedelta(hours=1), (13, 34)),
                 (timedelta(hours=-12), (0, 34)))
        for offset, (hour, minute) in cases:
            with self.subTest(offset=offset):
                local = datetime(2026, 1, 15, hour, minute, 56, 123456, tzinfo=timezone(offset))
                self.accept(DAY, local, INSTANT)

    def test_fractional_second_offset_uses_exact_datetime_arithmetic(self):
        local = datetime(2026, 1, 15, 12, 0, 0, 123456,
                         tzinfo=timezone(timedelta(seconds=30, microseconds=9)))
        self.accept(DAY, local, datetime(2026, 1, 15, 11, 59, 30, 123447, tzinfo=UTC))

    def test_named_zones_cover_winter_summer_and_non_hour_offset(self):
        cases = ((1, "UTC", 12, 34), (1, "Europe/Stockholm", 13, 34),
            (1, "America/New_York", 7, 34), (1, "Asia/Kathmandu", 18, 19),
            (1, "Australia/Sydney", 23, 34), (7, "Europe/Stockholm", 14, 34),
            (7, "America/New_York", 8, 34), (7, "Australia/Sydney", 22, 34))
        for month, zone, hour, minute in cases:
            with self.subTest(month=month, zone=zone):
                day = date(2026, month, 15)
                self.accept(day, datetime(2026, month, 15, hour, minute, 56, 123456, tzinfo=self.zones[zone]),
                            datetime(2026, month, 15, 12, 34, 56, 123456, tzinfo=UTC))

    def test_session_date_survives_previous_next_utc_day_and_year(self):
        cases = ((date(2026, 1, 1), datetime(2026, 1, 1, 0, 15, 0, 123456, tzinfo=timezone(timedelta(hours=14))),
                  datetime(2025, 12, 31, 10, 15, 0, 123456, tzinfo=UTC)),
                 (date(2026, 12, 31), datetime(2026, 12, 31, 23, 15, 0, 999999, tzinfo=timezone(timedelta(hours=-12))),
                  datetime(2027, 1, 1, 11, 15, 0, 999999, tzinfo=UTC)),
                 (date(2026, 1, 1), datetime(2026, 1, 1, 0, 15, 0, 1, tzinfo=self.zones["Asia/Kathmandu"]),
                  datetime(2025, 12, 31, 18, 30, 0, 1, tzinfo=UTC)))
        for day, local, expected in cases:
            self.assertNotEqual(day, expected.date())
            self.accept(day, local, expected)

    def test_session_date_is_explicit_without_unimplemented_calendar_inference(self):
        # This value type has no source/calendar binding yet.
        for day in (date.min, date.max, date(2026, 1, 1), date(2026, 2, 15)):
            self.accept(day, INSTANT, INSTANT)

    def test_valid_datetime_extremes_and_normalized_fold_zero(self):
        for local in (datetime.min.replace(tzinfo=UTC), datetime.max.replace(tzinfo=UTC),
                      INSTANT.replace(fold=1), INSTANT.replace(tzinfo=self.zones["UTC"], fold=1)):
            expected = local.replace(tzinfo=UTC, fold=0)
            result = self.accept(DAY, local, expected)
            self.assertEqual(result.not_before.fold, 0)
        self.assertEqual(INSTANT.replace(fold=1).fold, 1)

    def test_bad_session_types_and_date_datetime_subclasses_are_rejected(self):
        bad = (None, True, 0, 1.5, PRIVATE, b"2026-01-15", [], {}, (), object(),
               INSTANT, INSTANT.replace(tzinfo=None), DateSubclass(2026, 1, 15),
               DateTimeSubclass(2026, 1, 15, tzinfo=UTC), timedelta(days=1))
        for value in bad:
            with self.subTest(type=type(value).__name__):
                self.reject(value, INSTANT)

    def test_bad_not_before_types_and_datetime_subclasses_are_rejected(self):
        bad = (None, True, 0, 1.5, PRIVATE, b"2026-01-15", [], {}, (), object(), DAY,
               DateSubclass(2026, 1, 15), DateTimeSubclass(2026, 1, 15, tzinfo=UTC), timedelta(seconds=1))
        for value in bad:
            with self.subTest(type=type(value).__name__):
                self.reject(DAY, value)

    def test_hostile_invalid_values_are_not_stringified_or_inspected(self):
        hostile = HostileValue()
        self.reject(hostile, INSTANT)
        self.reject(DAY, hostile)
        # Invalid session type must stop before any timezone conversion.
        forbidden = INSTANT.replace(tzinfo=OffsetError(AssertionError("timezone read too early")))
        self.reject(hostile, forbidden)

    def test_naive_and_none_utcoffset_are_rejected(self):
        for local in (INSTANT.replace(tzinfo=None), INSTANT.replace(tzinfo=OffsetResult(None))):
            self.reject(DAY, local)

    def test_invalid_offset_return_types_and_out_of_range_offsets_are_rejected(self):
        for offset in (0, True, 1.5, PRIVATE, [], {}, timedelta(hours=24), timedelta(hours=-24),
                       timedelta(days=999999999), timedelta(hours=24, microseconds=-1) + timedelta(microseconds=1)):
            with self.subTest(type=type(offset).__name__):
                self.reject(DAY, INSTANT.replace(tzinfo=OffsetResult(offset)))

    def test_offset_conversion_failures_are_safely_translated(self):
        errors = (TypeError(PRIVATE), ValueError(PRIVATE), OverflowError(PRIVATE), ApiFailure("private_conversion_error"))
        for error in errors:
            with self.subTest(error=type(error).__name__):
                self.reject(DAY, INSTANT.replace(tzinfo=OffsetError(error)))

    def test_return_conversion_failures_are_safely_translated(self):
        errors = (TypeError(PRIVATE), ValueError(PRIVATE), OverflowError(PRIVATE), ApiFailure("private_conversion_error"))
        for error in errors:
            with self.subTest(error=type(error).__name__):
                self.reject(DAY, INSTANT.replace(tzinfo=FromUtcError(error)))

    def test_wrong_round_trip_and_invalid_fromutc_results_are_rejected(self):
        for shift in (timedelta(microseconds=1), timedelta(minutes=1), timedelta(days=-1)):
            self.reject(DAY, INSTANT.replace(tzinfo=BadRoundTrip(shift)))
        for result in (None, 0, PRIVATE, INSTANT.replace(tzinfo=None)):
            self.reject(DAY, INSTANT.replace(tzinfo=InvalidFromUtc(result)))

    def test_utc_underflow_overflow_and_near_boundary_valid_values(self):
        self.reject(date.min, datetime.min.replace(tzinfo=timezone(timedelta(hours=1))))
        self.reject(date.max, datetime.max.replace(tzinfo=timezone(timedelta(hours=-1))))
        self.accept(date.min, datetime.min.replace(tzinfo=timezone(timedelta(hours=-1))),
                    datetime(1, 1, 1, 1, tzinfo=UTC))
        self.accept(date.max, datetime.max.replace(tzinfo=timezone(timedelta(hours=1))),
                    datetime(9999, 12, 31, 22, 59, 59, 999999, tzinfo=UTC))

    def test_nonexistent_dst_times_reject_both_folds_and_non_hour_gap(self):
        cases = (("America/New_York", (2026, 3, 8, 2, 30)),
                 ("Europe/Stockholm", (2026, 3, 29, 2, 30)),
                 ("Australia/Lord_Howe", (2026, 10, 4, 2, 15)),
                 ("Pacific/Apia", (2011, 12, 30, 12, 0)))
        for zone, args in cases:
            for fold in (0, 1):
                with self.subTest(zone=zone, fold=fold):
                    local = datetime(*args, microsecond=123456, tzinfo=self.zones[zone], fold=fold)
                    self.reject(local.date(), local)

    def test_repeated_dst_times_produce_two_exact_distinct_utc_instants(self):
        cases = (("America/New_York", (2026, 11, 1, 1, 30),
                  (datetime(2026, 11, 1, 5, 30, 0, 123456, tzinfo=UTC), datetime(2026, 11, 1, 6, 30, 0, 123456, tzinfo=UTC))),
                 ("Europe/Stockholm", (2026, 10, 25, 2, 30),
                  (datetime(2026, 10, 25, 0, 30, 0, 123456, tzinfo=UTC), datetime(2026, 10, 25, 1, 30, 0, 123456, tzinfo=UTC))),
                 ("Australia/Lord_Howe", (2026, 4, 5, 1, 45),
                  (datetime(2026, 4, 4, 14, 45, 0, 123456, tzinfo=UTC), datetime(2026, 4, 4, 15, 15, 0, 123456, tzinfo=UTC))))
        for zone, args, expected in cases:
            results = []
            for fold in (0, 1):
                with self.subTest(zone=zone, fold=fold):
                    local = datetime(*args, microsecond=123456, tzinfo=self.zones[zone], fold=fold)
                    results.append(self.accept(local.date(), local, expected[fold]))
                    self.assertEqual(local.fold, fold)
            self.assertNotEqual(results[0], results[1])
            self.assertEqual(len(set(results)), 2)
            self.assertEqual(results[1].not_before - results[0].not_before,
                             timedelta(minutes=30 if zone == "Australia/Lord_Howe" else 60))

    def test_times_at_dst_gap_edges_remain_valid_to_microsecond(self):
        zone = self.zones["America/New_York"]
        self.accept(date(2026, 3, 8), datetime(2026, 3, 8, 1, 59, 59, 999999, tzinfo=zone),
                    datetime(2026, 3, 8, 6, 59, 59, 999999, tzinfo=UTC))
        self.accept(date(2026, 3, 8), datetime(2026, 3, 8, 3, 0, 0, 0, tzinfo=zone),
                    datetime(2026, 3, 8, 7, tzinfo=UTC))

    def test_dataclass_is_frozen_with_slots_and_immutable_field_values(self):
        value = SessionAvailability(DAY, INSTANT)
        self.assertTrue(value.__dataclass_params__.frozen)
        self.assertEqual([item.name for item in fields(value)], ["session_date", "not_before"])
        self.assertEqual(value.__slots__, ("session_date", "not_before"))
        self.assertFalse(hasattr(value, "__dict__"))
        for name in ("session_date", "not_before"):
            with self.assertRaises(FrozenInstanceError):
                setattr(value, name, None)
            with self.assertRaises(FrozenInstanceError):
                delattr(value, name)
        with self.assertRaises(AttributeError):
            value.not_before.year = 2000
        copied = asdict(value)
        copied["session_date"] = date(2000, 1, 1)
        self.assertEqual(value, SessionAvailability(DAY, INSTANT))

    def test_replace_preserves_or_revalidates_and_normalizes_new_values(self):
        original = SessionAvailability(DAY, INSTANT)
        self.assertEqual(replace(original), original)
        self.assertIsNot(replace(original), original)
        changed = replace(original, not_before=datetime(2026, 1, 15, 13, 34, 56, 123456, tzinfo=self.zones["Europe/Stockholm"]))
        self.assertEqual(changed, original)
        self.assertIs(changed.not_before.tzinfo, UTC)
        self.assertEqual(replace(original, session_date=date(2026, 1, 14)).session_date, date(2026, 1, 14))
        self.assertEqual(original.session_date, DAY)

    def test_replace_rejects_bad_types_naive_gap_and_overflow(self):
        original = SessionAvailability(DAY, INSTANT)
        changes = ({"session_date": None}, {"session_date": INSTANT},
                   {"session_date": DateSubclass(2026, 1, 15)}, {"not_before": None},
                   {"not_before": DateTimeSubclass(2026, 1, 15, tzinfo=UTC)},
                   {"not_before": INSTANT.replace(tzinfo=None)},
                   {"not_before": datetime(2026, 3, 8, 2, 30, tzinfo=self.zones["America/New_York"])},
                   {"not_before": datetime.min.replace(tzinfo=timezone(timedelta(hours=1)))})
        for change in changes:
            with self.assertRaises(ApiFailure) as caught:
                replace(original, **change)
            self.assertEqual(caught.exception.code, "invalid_availability")
            self.assertEqual(original, SessionAvailability(DAY, INSTANT))

    def test_equivalent_zone_representations_have_equal_normalized_objects_and_hashes(self):
        values = (INSTANT, datetime(2026, 1, 15, 13, 34, 56, 123456, tzinfo=timezone(timedelta(hours=1))),
                  datetime(2026, 1, 15, 7, 34, 56, 123456, tzinfo=self.zones["America/New_York"]))
        normalized = [SessionAvailability(DAY, value) for value in values]
        self.assertEqual(len(set(normalized)), 1)
        self.assertTrue(all(hash(item) == hash(normalized[0]) for item in normalized))
        self.assertNotEqual(replace(normalized[0], session_date=date(2026, 1, 16)), normalized[0])

    def test_construction_has_no_clock_environment_state_network_file_or_log_side_effects(self):
        valid = (INSTANT, INSTANT.replace(fold=1),
                 datetime(2026, 1, 15, 13, 34, 56, 123456, tzinfo=self.zones["Europe/Stockholm"]),
                 datetime(2026, 11, 1, 1, 30, tzinfo=self.zones["America/New_York"], fold=1))
        invalid = (None, INSTANT.replace(tzinfo=None),
                   datetime(2026, 3, 8, 2, 30, tzinfo=self.zones["America/New_York"]),
                   INSTANT.replace(tzinfo=OffsetError(ValueError(PRIVATE))))
        expected = [SessionAvailability(DAY, value) for value in valid]
        with no_availability_side_effects():
            self.assertIs(api_contracts.date, date)
            self.assertIs(api_contracts.datetime, datetime)
            for local, result in zip(valid, expected):
                self.assertEqual(SessionAvailability(DAY, local), result)
            for local in invalid:
                with self.assertRaises(ApiFailure) as caught:
                    SessionAvailability(DAY, local)
                self.assertEqual(caught.exception.code, "invalid_availability")

    def test_clock_guard_detects_actual_methods_without_substituting_datetime_types(self):
        for clock in (datetime.now, datetime.utcnow, datetime.today, date.today):
            with self.subTest(clock=clock.__name__):
                with self.assertRaisesRegex(AssertionError, "calendar clock read forbidden"):
                    with block_calendar_clock():
                        clock()
        self.assertIs(api_contracts.datetime, datetime)
        self.assertIs(api_contracts.date, date)


if __name__ == "__main__":
    unittest.main()
