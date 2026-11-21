"""Explicit session bounds and request planning; no clock, I/O or provider rules.

Bounds are operator-reviewed policy, not evidence of historical publication.
The complete schedule binds to an unchanged DailyContract fingerprint.
"""
from dataclasses import dataclass
from datetime import date, datetime
import re

from api_contracts import (ApiFailure, DailyContract, SessionAvailability,
                           SourceAvailability, identifier, utc)


def _instant(value):
    # Reuse the tested normalization, including rejection of nonexistent local times.
    return SessionAvailability(date(2000, 1, 1), value).not_before


@dataclass(frozen=True, slots=True)
class DailyRequest:
    reference_time: datetime
    contract_fingerprint: str
    sessions: tuple[date, ...]

    def __post_init__(self):
        try:
            normalized = _instant(self.reference_time)
        except ApiFailure:
            raise ApiFailure("invalid_daily_request") from None
        if (type(self.contract_fingerprint) is not str
                or re.fullmatch(r"[0-9a-f]{64}", self.contract_fingerprint) is None
                or type(self.sessions) is not tuple or not 1 <= len(self.sessions) <= 10000):
            raise ApiFailure("invalid_daily_request")
        previous = None
        for day in self.sessions:
            if type(day) is not date or (previous is not None and day <= previous):
                raise ApiFailure("invalid_daily_request")
            previous = day
        object.__setattr__(self, "reference_time", normalized)


def plan_daily_request(contract, reference_time):
    if type(contract) is not DailyContract:
        raise ApiFailure("invalid_contract")
    reference_time = _instant(reference_time)
    completed = contract.completed_sessions(reference_time)
    return DailyRequest(reference_time, contract.fingerprint,
                        tuple(day for day, _ in completed[-contract.minimum_sessions:]))


def validate_daily_request(request, contract, now):
    if type(request) is not DailyRequest:
        raise ApiFailure("invalid_daily_request")
    if request.contract_fingerprint != contract.fingerprint:
        raise ApiFailure("request_contract_mismatch")
    if request.reference_time > _instant(now):
        raise ApiFailure("future_request")
    if request != plan_daily_request(contract, request.reference_time):
        raise ApiFailure("request_sessions_mismatch")
    return request


def validate_source_availability(availability, *, source_id, rule_version, contract):
    """Compare against the selected profile's approved rule, never self-approve."""
    if (type(source_id) is not str or not identifier(source_id)
            or type(rule_version) is not str or not identifier(rule_version)
            or type(contract) is not DailyContract):
        raise ApiFailure("invalid_availability_binding")
    if type(availability) is not SourceAvailability:
        raise ApiFailure("availability_required")
    if availability.source_id != source_id:
        raise ApiFailure("availability_source_mismatch")
    if availability.rule_version != rule_version:
        raise ApiFailure("availability_rule_mismatch")
    if availability.contract_fingerprint != contract.fingerprint:
        raise ApiFailure("availability_contract_mismatch")
    if tuple(entry.session_date for entry in availability.entries) != tuple(
            day for day, _ in contract.sessions):
        raise ApiFailure("availability_coverage")
    if any(entry.not_before < utc(close) for entry, (_, close)
           in zip(availability.entries, contract.sessions)):
        raise ApiFailure("availability_before_close")
    return availability


def require_available(availability, sessions, reference_time):
    bounds = {entry.session_date: entry.not_before for entry in availability.entries}
    for day in sessions:
        if day not in bounds:
            raise ApiFailure("availability_coverage")
        if bounds[day] > reference_time:
            raise ApiFailure("data_not_available")


def validate_available_result(availability, request, contract, prices, observed, received):
    """Shared post-adapter guard; extra historical rows must pass their own bounds."""
    received, observed = _instant(received), _instant(observed)
    if received < request.reference_time:
        raise ApiFailure("clock_rollback")
    if (received - observed).total_seconds() > contract.max_age_seconds:
        raise ApiFailure("stale_data")
    if not prices or len(prices) > contract.max_rows:
        raise ApiFailure("row_limit")
    try:
        days = tuple(date.fromisoformat(text) for text in prices)
        if any(type(text) is not str or day.isoformat() != text
               for text, day in zip(prices, days)):
            raise ValueError
    except (ValueError, TypeError):
        raise ApiFailure("invalid_date") from None
    if not set(request.sessions).issubset(days):
        raise ApiFailure("incomplete_series")
    if max(days) != request.sessions[-1]:
        raise ApiFailure("request_sessions_mismatch")
    closes = dict(contract.sessions)
    if observed != utc(closes[request.sessions[-1]]):
        raise ApiFailure("observation_mismatch")
    require_available(availability, days, request.reference_time)


def availability_from_json(value):
    """Strict versioned wire format; a parsed rule still needs profile approval."""
    if (type(value) is not dict
            or set(value) != {"version", "source_id", "rule_version", "contract_fingerprint", "entries"}
            or type(value["version"]) is not int or value["version"] != 1
            or type(value["entries"]) is not list or not 1 <= len(value["entries"]) <= 10000):
        raise ApiFailure("availability_schema")
    entries = []
    for item in value["entries"]:
        if (type(item) is not dict or set(item) != {"session_date", "not_before"}
                or any(type(item[name]) is not str for name in item)):
            raise ApiFailure("availability_schema")
        try:
            day = date.fromisoformat(item["session_date"])
            if day.isoformat() != item["session_date"]:
                raise ValueError
            instant = datetime.fromisoformat(item["not_before"])
        except ValueError:
            raise ApiFailure("availability_schema") from None
        entries.append(SessionAvailability(day, instant))
    return SourceAvailability(value["source_id"], value["rule_version"],
                              value["contract_fingerprint"], tuple(entries))


def availability_to_json(availability):
    if type(availability) is not SourceAvailability:
        raise ApiFailure("invalid_source_availability")
    return {"version": 1, "source_id": availability.source_id,
            "rule_version": availability.rule_version,
            "contract_fingerprint": availability.contract_fingerprint,
            "entries": [{"session_date": entry.session_date.isoformat(),
                         "not_before": entry.not_before.isoformat(timespec="microseconds")}
                        for entry in availability.entries]}
