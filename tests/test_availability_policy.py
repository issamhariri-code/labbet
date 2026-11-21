"""Pure availability binding, request planning and post-adapter contract checks."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, timedelta, timezone
from types import MappingProxyType
import unittest

from api_availability import (
    DailyRequest, availability_from_json, availability_to_json, plan_daily_request,
    require_available, validate_available_result, validate_daily_request,
    validate_source_availability,
)
from api_contracts import ApiFailure, SessionAvailability, SourceAvailability
from test_api_client import NOW, ROW, contract
from test_session_availability import HostileValue, no_availability_side_effects


RULE = "reviewed-session-bounds-v1"
MICROSECOND = timedelta(microseconds=1)


class AvailabilityPolicyTests(unittest.TestCase):
    def setUp(self):
        self.contract = contract()
        self.availability = self.schedule()
        self.request = plan_daily_request(self.contract, NOW)
        self.observed = datetime(2026, 9, 30, 20, tzinfo=timezone.utc)
        self.prices = MappingProxyType({"2026-09-30": MappingProxyType(dict(ROW))})

    def schedule(self, *, contract_value=None, changes=None, **bindings):
        selected = self.contract if contract_value is None else contract_value
        changes = {} if changes is None else changes
        values = dict(source_id="source-a", rule_version=RULE,
                      contract_fingerprint=selected.fingerprint,
                      entries=tuple(SessionAvailability(day, changes.get(day, close))
                                    for day, close in selected.sessions))
        values.update(bindings)
        return SourceAvailability(**values)

    def assert_code(self, code, operation, *args, **kwargs):
        with self.assertRaises(ApiFailure) as caught:
            operation(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)
        self.assertIsNone(caught.exception.status)
        self.assertEqual(str(caught.exception), "API-hämtningen stoppades: " + code)

    def bind(self, availability=None, **changes):
        args = dict(source_id="source-a", rule_version=RULE, contract=self.contract)
        args.update(changes)
        return validate_source_availability(
            self.availability if availability is None else availability, **args)

    def result(self, **changes):
        args = dict(availability=self.availability, request=self.request,
                    contract=self.contract, prices=self.prices,
                    observed=self.observed, received=NOW)
        args.update(changes)
        return validate_available_result(**args)

    def test_valid_binding_accepts_exact_full_calendar_and_returns_same_object(self):
        self.assertIs(self.bind(), self.availability)
        self.assertEqual(self.availability.entries[-1].session_date, date(2026, 10, 2))

    def test_binding_rejects_wrong_source_rule_and_contract_in_order(self):
        wrong = replace(self.availability, source_id="other", rule_version="other",
                        contract_fingerprint="0" * 64)
        for code in ("availability_source_mismatch", "availability_rule_mismatch",
                     "availability_contract_mismatch"):
            with self.subTest(code=code):
                self.assert_code(code, self.bind, wrong)
            wrong = (replace(wrong, source_id="source-a")
                     if code == "availability_source_mismatch"
                     else replace(wrong, rule_version=RULE))

    def test_binding_requires_trusted_profile_identity_and_real_contract(self):
        for field in ("source_id", "rule_version"):
            for value in (None, True, "", "private\nvalue", HostileValue()):
                with self.subTest(field=field, kind=type(value).__name__):
                    self.assert_code("invalid_availability_binding", self.bind,
                                     **{field: value})
        self.assert_code("invalid_availability_binding", self.bind, contract=object())

    def test_binding_rejects_missing_or_non_exact_availability(self):
        class Derived(SourceAvailability):
            pass
        values = (None, {}, HostileValue(), Derived("source-a", RULE,
                  self.contract.fingerprint, self.availability.entries))
        for value in values:
            with self.subTest(kind=type(value).__name__):
                self.assert_code("availability_required", validate_source_availability,
                                 value, source_id="source-a", rule_version=RULE,
                                 contract=self.contract)

    def test_full_coverage_rejects_missing_past_current_or_future_session(self):
        for index in range(len(self.availability.entries)):
            candidate = replace(self.availability,
                entries=self.availability.entries[:index] + self.availability.entries[index + 1:])
            with self.subTest(index=index):
                self.assert_code("availability_coverage", self.bind, candidate)

    def test_full_coverage_rejects_extra_calendar_date_without_mutation(self):
        extra = SessionAvailability(date(2026, 10, 3), datetime(2026, 10, 3, 20,
                                                               tzinfo=timezone.utc))
        candidate = replace(self.availability, entries=self.availability.entries + (extra,))
        before = candidate.fingerprint
        self.assert_code("availability_coverage", self.bind, candidate)
        self.assertEqual(candidate.fingerprint, before)

    def test_any_bound_before_its_close_rejects_even_unused_future_row(self):
        for day, close in self.contract.sessions:
            with self.subTest(day=day):
                self.assert_code("availability_before_close", self.bind,
                                 self.schedule(changes={day: close - MICROSECOND}))

    def test_nonmonotonic_and_shared_bounds_are_valid_but_checked_individually(self):
        early, later = date(2026, 9, 29), date(2026, 9, 30)
        schedule = self.schedule(changes={early: NOW + MICROSECOND, later: NOW})
        self.assertIs(self.bind(schedule), schedule)
        require_available(schedule, (later,), NOW)
        self.assert_code("data_not_available", require_available, schedule, (early, later), NOW)
        shared = self.schedule(changes={early: NOW, later: NOW})
        self.bind(shared)
        require_available(shared, (early, later), NOW)

    def test_gate_microsecond_before_exact_and_after_boundary(self):
        day = date(2026, 9, 30)
        gate = NOW.replace(microsecond=123456)
        schedule = self.schedule(changes={day: gate})
        self.assert_code("data_not_available", require_available, schedule, (day,), gate - MICROSECOND)
        for instant in (gate, gate + MICROSECOND):
            require_available(schedule, (day,), instant)
        self.assert_code("availability_coverage", require_available, schedule,
                         (date(2026, 9, 28),), gate)

    def test_request_selects_latest_required_completed_sessions(self):
        selected = contract(minimum_sessions=2)
        request = plan_daily_request(selected, NOW)
        self.assertEqual(request.sessions, (date(2026, 9, 29), date(2026, 9, 30)))
        self.assertEqual(request.contract_fingerprint, selected.fingerprint)
        self.assertEqual(request.reference_time, NOW)

    def test_request_selection_changes_at_close_not_one_microsecond_before(self):
        before = plan_daily_request(self.contract, self.observed - MICROSECOND)
        exact = plan_daily_request(self.contract, self.observed)
        self.assertEqual(before.sessions, (date(2026, 9, 29),))
        self.assertEqual(exact.sessions, (date(2026, 9, 30),))
        # Subsequent wall time does not reselect targets in this immutable request.
        self.assertIs(validate_daily_request(before, self.contract, NOW), before)

    def test_request_normalizes_equivalent_offset_without_losing_microseconds(self):
        instant = NOW.replace(microsecond=456789)
        local = instant.astimezone(timezone(timedelta(hours=5, minutes=45)))
        first = plan_daily_request(self.contract, instant)
        second = plan_daily_request(self.contract, local)
        self.assertEqual(first, second)
        self.assertIs(second.reference_time.tzinfo, timezone.utc)
        self.assertEqual(second.reference_time.microsecond, 456789)

    def test_request_is_frozen_slots_and_replace_revalidates(self):
        self.assertFalse(hasattr(self.request, "__dict__"))
        with self.assertRaises(FrozenInstanceError):
            self.request.reference_time = NOW + timedelta(days=1)
        with self.assertRaises(TypeError):
            self.request.sessions[0] = date(2026, 9, 29)
        self.assert_code("invalid_daily_request", replace, self.request, sessions=())

    def test_request_rejects_invalid_instants_fingerprint_and_session_shapes(self):
        for instant in (None, "2026-09-30", NOW.replace(tzinfo=None), HostileValue()):
            with self.subTest(kind=type(instant).__name__):
                self.assert_code("invalid_daily_request", replace, self.request, reference_time=instant)
        for fingerprint in (None, "A" * 64, "0" * 63, HostileValue()):
            with self.subTest(kind=type(fingerprint).__name__):
                self.assert_code("invalid_daily_request", replace, self.request,
                                 contract_fingerprint=fingerprint)
        day = date(2026, 9, 30)
        for sessions in ([], (), (day,) * 10001, (day, day),
                         (day, day - timedelta(days=1)), (NOW,), (HostileValue(),)):
            with self.subTest(shape=type(sessions).__name__, count=len(sessions)):
                self.assert_code("invalid_daily_request", replace, self.request, sessions=sessions)

    def test_request_rejects_validly_shaped_wrong_contract_and_wrong_window(self):
        self.assert_code("request_contract_mismatch", validate_daily_request,
                         replace(self.request, contract_fingerprint="0" * 64), self.contract, NOW)
        self.assert_code("request_sessions_mismatch", validate_daily_request,
                         replace(self.request, sessions=(date(2026, 9, 29),)), self.contract, NOW)
        self.assert_code("invalid_daily_request", validate_daily_request, None, self.contract, NOW)

    def test_request_future_reference_rejects_exact_boundary_accepts(self):
        self.assert_code("future_request", validate_daily_request,
                         self.request, self.contract, NOW - MICROSECOND)
        self.assertIs(validate_daily_request(self.request, self.contract, NOW), self.request)

    def test_plan_retains_calendar_expiry_no_session_and_staleness_guards(self):
        self.assert_code("invalid_contract", plan_daily_request, None, NOW)
        self.assert_code("calendar_expired", plan_daily_request,
                         self.contract, datetime(2026, 10, 3, tzinfo=timezone.utc))
        self.assert_code("no_completed_session", plan_daily_request,
                         self.contract, datetime(2026, 9, 29, 19, tzinfo=timezone.utc))
        self.assert_code("stale_calendar", plan_daily_request,
                         contract(max_age_seconds=3599), NOW)

    def test_result_preserves_prices_and_separate_observation_reception(self):
        before = dict(self.prices)
        self.assertIsNone(self.result())
        self.assertEqual(dict(self.prices), before)
        self.assertNotEqual(self.observed, NOW)

    def test_result_checks_older_extra_rows_with_later_bound(self):
        day = date(2026, 9, 29)
        schedule = self.schedule(changes={day: NOW + MICROSECOND})
        self.bind(schedule)
        require_available(schedule, self.request.sessions, NOW)
        prices = {"2026-09-29": dict(ROW), "2026-09-30": dict(ROW)}
        before = deepcopy(prices)
        self.assert_code("data_not_available", self.result, availability=schedule, prices=prices)
        self.assertEqual(prices, before)

    def test_result_receipt_after_next_close_does_not_advance_request(self):
        later = datetime(2026, 10, 1, 21, tzinfo=timezone.utc)
        self.result(received=later)
        newer = {"2026-09-30": dict(ROW), "2026-10-01": dict(ROW)}
        self.assert_code("request_sessions_mismatch", self.result, prices=newer, received=later)
        self.assertEqual(self.request.sessions, (date(2026, 9, 30),))

    def test_result_missing_requested_session_does_not_accept_older_window(self):
        self.assert_code("incomplete_series", self.result,
                         prices={"2026-09-29": dict(ROW)})

    def test_result_rejects_wrong_observation_even_if_prices_match(self):
        for observed in (self.observed - MICROSECOND, self.observed + MICROSECOND, NOW):
            with self.subTest(observed=observed):
                self.assert_code("observation_mismatch", self.result, observed=observed)

    def test_result_actual_receipt_age_at_limit_and_one_microsecond_over(self):
        selected = contract(max_age_seconds=3600)
        schedule = self.schedule(contract_value=selected)
        request = plan_daily_request(selected, NOW)
        self.result(contract=selected, availability=schedule, request=request)
        self.assert_code("stale_data", self.result, contract=selected,
                         availability=schedule, request=request, received=NOW + MICROSECOND)

    def test_result_clock_rollback_rejects_before_other_result_checks(self):
        self.assert_code("clock_rollback", self.result, received=NOW - MICROSECOND, prices={})

    def test_result_rejects_noncanonical_dates_non_session_and_row_bounds(self):
        for text in ("20260930", "2026-W40-3", "private\nvalue", 42):
            with self.subTest(kind=type(text).__name__):
                self.assert_code("invalid_date", self.result, prices={text: dict(ROW)})
        self.assert_code("availability_coverage", self.result,
            prices={"2026-09-28": dict(ROW), "2026-09-30": dict(ROW)})
        self.assert_code("row_limit", self.result, prices={})
        self.assert_code("row_limit", self.result, contract=contract(max_rows=1),
            prices={"2026-09-29": dict(ROW), "2026-09-30": dict(ROW)})

    def test_json_roundtrip_is_lossless_canonical_and_detached(self):
        wire = availability_to_json(self.availability)
        before = deepcopy(wire)
        restored = availability_from_json(wire)
        self.assertEqual(restored, self.availability)
        self.assertEqual(restored.fingerprint, self.availability.fingerprint)
        self.assertEqual(availability_to_json(restored), wire)
        self.assertEqual(wire, before)
        wire["entries"][0]["not_before"] = "changed"
        self.assertEqual(restored, self.availability)
        self.assertIn(".000000+00:00", before["entries"][0]["not_before"])

    def test_json_equivalent_offset_microseconds_normalize_identically(self):
        schedule = self.schedule(changes={date(2026, 9, 30): NOW.replace(microsecond=123456)})
        wire = availability_to_json(schedule)
        wire["entries"][1]["not_before"] = "2026-09-30T23:00:00.123456+02:00"
        self.assertEqual(availability_from_json(wire), schedule)

    def test_json_rejects_outer_shape_unknown_keys_and_version_types(self):
        valid = availability_to_json(self.availability)
        invalid = [None, [], HostileValue(), dict(valid, unknown=True)]
        for field in valid:
            copy = dict(valid)
            del copy[field]
            invalid.append(copy)
        invalid.extend(dict(valid, version=value) for value in (True, False, "1", 1.0, 0, 2))
        for value in invalid:
            with self.subTest(kind=type(value).__name__):
                self.assert_code("availability_schema", availability_from_json, value)

    def test_json_rejects_entries_shape_empty_over_limit_and_unknown_fields(self):
        valid = availability_to_json(self.availability)
        item = valid["entries"][0]
        for entries in ((), {}, None, [], [item] * 10001, [None], [dict(item, extra="x")],
                        [{"session_date": "2026-09-29"}],
                        [dict(item, session_date=42)], [dict(item, not_before=None)]):
            with self.subTest(kind=type(entries).__name__):
                self.assert_code("availability_schema", availability_from_json,
                                 dict(valid, entries=entries))

    def test_json_rejects_invalid_dates_naive_times_and_bad_bindings(self):
        valid = availability_to_json(self.availability)
        for field, value, code in (
            ("session_date", "20260929", "availability_schema"),
            ("session_date", "2026-02-30", "availability_schema"),
            ("not_before", "private\nvalue", "availability_schema"),
            ("not_before", "2026-09-29T20:00:00", "invalid_availability"),
        ):
            wire = deepcopy(valid)
            wire["entries"][0][field] = value
            with self.subTest(field=field, code=code):
                self.assert_code(code, availability_from_json, wire)
        for field in ("source_id", "rule_version", "contract_fingerprint"):
            self.assert_code("invalid_source_availability", availability_from_json,
                             dict(valid, **{field: None}))

    def test_json_rejects_duplicate_or_reversed_dates_without_sorting(self):
        valid = availability_to_json(self.availability)
        for entries in ([valid["entries"][0]] * 2, list(reversed(valid["entries"]))):
            wire = dict(valid, entries=entries)
            before = deepcopy(wire)
            self.assert_code("invalid_source_availability", availability_from_json, wire)
            self.assertEqual(wire, before)
        self.assert_code("invalid_source_availability", availability_to_json, None)

    def test_pure_pipeline_and_json_have_no_clock_or_other_side_effects(self):
        wire = availability_to_json(self.availability)
        expected_fingerprint = self.availability.fingerprint
        with no_availability_side_effects():
            restored = availability_from_json(wire)
            bound = self.bind(restored)
            request = plan_daily_request(self.contract, NOW)
            validate_daily_request(request, self.contract, NOW)
            require_available(bound, request.sessions, request.reference_time)
            validate_available_result(bound, request, self.contract,
                                      self.prices, self.observed, NOW)
            roundtrip = availability_to_json(bound)
            actual_fingerprint = bound.fingerprint
        self.assertEqual(actual_fingerprint, expected_fingerprint)
        self.assertEqual(roundtrip, wire)


if __name__ == "__main__":
    unittest.main()
