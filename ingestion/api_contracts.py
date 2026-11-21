"""Explicit daily-data contract. No network, clock reads or persistence here."""
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import hashlib
import json
import math
import re
from types import MappingProxyType
from zoneinfo import ZoneInfo


class ApiFailure(RuntimeError):
    """Only our own bounded code is public; never provider text or an exception repr."""
    def __init__(self, code, *, status=None):
        if not isinstance(code, str) or not re.fullmatch(r"[a-z_]{1,64}", code):
            code = "unknown"
        self.code = code
        self.status = status if type(status) is int and 100 <= status <= 599 else None
        super().__init__("API-hämtningen stoppades: " + code)


def identifier(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", value)


def utc(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ApiFailure("invalid_time")
    return value.astimezone(timezone.utc)


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ApiFailure("duplicate_json_field")
        result[key] = value
    return result


def _constant(value):
    raise ApiFailure("nonfinite_json_number")


def _float(value):
    result = float(value)
    if not math.isfinite(result):
        raise ApiFailure("nonfinite_json_number")
    return result


def decode_json(body, *, max_bytes=1048576, max_depth=16):
    if type(body) is not bytes or len(body) > max_bytes:
        raise ApiFailure("response_size")
    # Bound nesting before json allocates a deeply recursive object graph.
    depth = 0
    quoted = escaped = False
    for byte in body:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
        elif byte in (91, 123):
            depth += 1
            if depth > max_depth:
                raise ApiFailure("json_depth")
        elif byte in (93, 125):
            depth -= 1
    try:
        # Deliberately UTF-8 only: this is the adapter's documented wire contract.
        return json.loads(body.decode("utf-8"), object_pairs_hook=_pairs,
                          parse_constant=_constant, parse_float=_float)
    except UnicodeDecodeError:
        raise ApiFailure("invalid_encoding") from None
    except (ValueError, RecursionError):
        raise ApiFailure("invalid_json") from None


@dataclass(frozen=True, slots=True)
class SessionAvailability:
    session_date: date
    not_before: datetime

    def __post_init__(self):
        if type(self.session_date) is not date or type(self.not_before) is not datetime:
            raise ApiFailure("invalid_availability")
        try:
            normalized = utc(self.not_before).replace(fold=0)
            if normalized.astimezone(self.not_before.tzinfo) != self.not_before:
                raise ValueError
        except (ApiFailure, OverflowError, TypeError, ValueError):
            raise ApiFailure("invalid_availability") from None
        object.__setattr__(self, "not_before", normalized)


@dataclass(frozen=True, slots=True)
class SourceAvailability:
    source_id: str
    rule_version: str
    contract_fingerprint: str
    entries: tuple[SessionAvailability, ...]

    def __post_init__(self):
        if any(type(value) is not str or not identifier(value)
               for value in (self.source_id, self.rule_version)):
            raise ApiFailure("invalid_source_availability")
        if (type(self.contract_fingerprint) is not str
                or re.fullmatch(r"[0-9a-f]{64}", self.contract_fingerprint) is None):
            raise ApiFailure("invalid_source_availability")
        if type(self.entries) is not tuple or not 1 <= len(self.entries) <= 10000:
            raise ApiFailure("invalid_source_availability")
        previous = None
        for entry in self.entries:
            if type(entry) is not SessionAvailability:
                raise ApiFailure("invalid_source_availability")
            if previous is not None and entry.session_date <= previous:
                raise ApiFailure("invalid_source_availability")
            previous = entry.session_date

    @property
    def fingerprint(self):
        values = {
            "version": 1,
            "source_id": self.source_id,
            "rule_version": self.rule_version,
            "contract_fingerprint": self.contract_fingerprint,
            "entries": [
                [
                    entry.session_date.isoformat(),
                    entry.not_before.isoformat(timespec="microseconds"),
                ]
                for entry in self.entries
            ],
        }
        body = json.dumps(values, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(body.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class DailyContract:
    symbol: str
    instrument_id: str
    currency: str
    exchange: str
    timezone: str
    price_basis: str
    calendar_version: str
    calendar_valid_from: date
    calendar_valid_until: date
    # Reviewed exchange session CLOSE instants, including early closes/DST.
    sessions: tuple[tuple[date, datetime], ...]
    accepted_timezones: tuple[str, ...] = ()
    minimum_sessions: int = 1
    max_rows: int = 100
    max_age_seconds: float = 604800
    schema_version: str = "alphavantage-daily-v1"

    def __post_init__(self):
        for value in (self.symbol, self.instrument_id, self.exchange,
                      self.calendar_version, self.schema_version):
            if not identifier(value):
                raise ApiFailure("invalid_contract")
        if not re.fullmatch(r"[A-Z]{3}", self.currency) or self.price_basis not in ("raw", "adjusted"):
            raise ApiFailure("invalid_contract")
        try:
            ZoneInfo(self.timezone)
            for name in self.accepted_timezones:
                ZoneInfo(name)
        except (ValueError, KeyError, TypeError):
            raise ApiFailure("invalid_contract") from None
        if type(self.calendar_valid_from) is not date or type(self.calendar_valid_until) is not date or self.calendar_valid_until < self.calendar_valid_from:
            raise ApiFailure("invalid_calendar")
        if not isinstance(self.sessions, tuple) or not isinstance(self.accepted_timezones, tuple):
            raise ApiFailure("mutable_contract")
        if type(self.minimum_sessions) is not int or type(self.max_rows) is not int or not 1 <= self.minimum_sessions <= self.max_rows <= 10000:
            raise ApiFailure("invalid_contract")
        if type(self.max_age_seconds) not in (float, int) or not math.isfinite(self.max_age_seconds) or not 0 < self.max_age_seconds <= 31536000:
            raise ApiFailure("invalid_contract")
        seen = set()
        previous = None
        for item in self.sessions:
            if not isinstance(item, tuple) or len(item) != 2:
                raise ApiFailure("invalid_calendar")
            day, close = item
            close = utc(close)
            if type(day) is not date or day in seen or not self.calendar_valid_from <= day <= self.calendar_valid_until:
                raise ApiFailure("invalid_calendar")
            if close.astimezone(ZoneInfo(self.timezone)).date() != day or previous and close <= previous:
                raise ApiFailure("invalid_calendar")
            previous = close
            seen.add(day)

    def completed_sessions(self, now):
        now = utc(now)
        today = now.astimezone(ZoneInfo(self.timezone)).date()
        if not self.calendar_valid_from <= today <= self.calendar_valid_until:
            raise ApiFailure("calendar_expired")
        completed = tuple((day, utc(close)) for day, close in self.sessions if utc(close) <= now)
        if len(completed) < self.minimum_sessions:
            raise ApiFailure("no_completed_session")
        if (now - completed[-1][1]).total_seconds() > self.max_age_seconds:
            raise ApiFailure("stale_calendar")
        return completed

    @property
    def fingerprint(self):
        # Do not use repr: deterministic, unambiguous and secrets-free.
        values = {key: value for key, value in self.__dict__.items()}
        return hashlib.sha256(json.dumps(values, sort_keys=True, default=str,
                                        separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class PriceBatch:
    prices: object = field(repr=False)
    source_id: str
    instrument_id: str
    symbol: str
    request_id: str
    observed_at: datetime
    received_at: datetime
    config_version: str
    payload_hash: str
    metadata: object
    state_path: str | None = field(default=None, repr=False, compare=False)
    # Request selection is distinct from receipt/publication and stream metadata.
    request_context: object = field(default=None, repr=False, compare=False)
    provenance: object = field(default=None, repr=False, compare=False)

    @property
    def storage_context(self):
        context = {name: getattr(self, name) for name in (
            "source_id", "instrument_id", "symbol", "request_id", "observed_at",
            "received_at", "config_version", "payload_hash", "metadata")}
        context["metadata"] = dict(self.metadata)
        if self.request_context is not None or self.provenance is not None:
            from api_provenance import (RequestProvenance, provenance_from_documents,
                                        validate_provenance_binding)
            if type(self.provenance) is not RequestProvenance or self.request_context != self.provenance.request:
                raise ApiFailure("invalid_provenance")
            captured = provenance_from_documents(self.provenance.manifest_document,
                                                 self.provenance.request_document)
            try:
                days = tuple(date.fromisoformat(day) for day in self.prices)
            except (TypeError, ValueError):
                raise ApiFailure("invalid_provenance") from None
            validate_provenance_binding(captured, source_id=self.source_id,
                instrument_id=self.instrument_id, symbol=self.symbol, observed_at=self.observed_at,
                received_at=self.received_at, config_version=self.config_version,
                metadata=context["metadata"], session_dates=days)
            context["provenance"] = captured
        return context


def alpha_daily(data, contract, now):
    """No free-text error classification and no silent coercion of provider values."""
    if not isinstance(data, dict):
        raise ApiFailure("root_type")
    if any(key in data for key in ("next", "next_page", "cursor", "pagination")):
        raise ApiFailure("unsupported_pagination")
    for field, code in (("Error Message", "provider_error"),
                        ("Information", "provider_information"), ("Note", "provider_note")):
        if field in data:
            if not isinstance(data[field], str) or not data[field].strip():
                raise ApiFailure("provider_error_format")
            raise ApiFailure(code)
    prices = data.get("Time Series (Daily)")
    if prices is not None and not isinstance(prices, dict):
        raise ApiFailure("series_type")
    if not prices:
        raise ApiFailure("missing_series")
    if len(prices) > contract.max_rows:
        raise ApiFailure("row_limit")
    if any(not isinstance(row, dict) for row in prices.values()):
        raise ApiFailure("row_type")
    metadata = data.get("Meta Data")
    if not isinstance(metadata, dict):
        raise ApiFailure("missing_metadata")
    if metadata.get("2. Symbol") != contract.symbol:
        raise ApiFailure("instrument_mismatch")
    if metadata.get("5. Time Zone") not in (contract.timezone,) + contract.accepted_timezones:
        raise ApiFailure("timezone_mismatch")
    # Currency/exchange are verified in the source/instrument registry because this
    # endpoint does not promise those fields. Any explicit conflicting fields fail.
    for key, expected in (("currency", contract.currency), ("exchange", contract.exchange),
                          ("price_basis", contract.price_basis)):
        if key in metadata and metadata[key] != expected:
            raise ApiFailure("metadata_mismatch")
    if contract.price_basis != "raw" or contract.schema_version != "alphavantage-daily-v1":
        raise ApiFailure("adapter_contract_mismatch")
    completed = contract.completed_sessions(now)
    required = {day.isoformat() for day, _ in completed[-contract.minimum_sessions:]}
    if not required.issubset(prices):
        raise ApiFailure("incomplete_series")
    try:
        days = [date.fromisoformat(value) for value in prices]
        if any(day.isoformat() != text for day, text in zip(days, prices)):
            raise ValueError
        refreshed = metadata["3. Last Refreshed"]
        refreshed_day = date.fromisoformat(refreshed)
        if refreshed_day.isoformat() != refreshed:
            raise ValueError
    except (TypeError, ValueError, KeyError):
        raise ApiFailure("invalid_date") from None
    if max(days) != completed[-1][0] or refreshed_day != max(days):
        raise ApiFailure("stale_or_future_data")
    session_days = {day for day, _ in contract.sessions}
    if any(not contract.calendar_valid_from <= day <= contract.calendar_valid_until for day in days):
        raise ApiFailure("calendar_coverage")
    if any(day not in session_days for day in days):
        raise ApiFailure("non_session_data")
    # Immutable per-request result; no shared cache/global last-response state.
    frozen = MappingProxyType({day: MappingProxyType(dict(row)) for day, row in prices.items()})
    return frozen, completed[-1][1]
