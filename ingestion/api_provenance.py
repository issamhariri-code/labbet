"""Versioned, secret-free evidence of request selection, not publication proof.

The reusable manifest contains the complete contract and source schedule. Each
request separately binds its reference time, sessions and profile hash. Historical
decoding deliberately does not consult today's provider/rule registry or clock.
"""
from dataclasses import asdict, dataclass, fields, replace
from datetime import date, datetime
import hashlib
import json
import re

from api_availability import (DailyRequest, availability_from_json, availability_to_json,
    plan_daily_request, require_available, validate_available_result, validate_source_availability)
from api_contracts import ApiFailure, DailyContract, SessionAvailability, SourceAvailability, decode_json


MAX_MANIFEST_BYTES = 4 * 1048576
MAX_REQUEST_BYTES = 262144
_CONTRACT_FIELDS = frozenset(field.name for field in fields(DailyContract))


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _digest(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _hash(value):
    return type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _contract_json(contract):
    result = asdict(contract)
    for name in ("calendar_valid_from", "calendar_valid_until"):
        result[name] = result[name].isoformat()
    # Keep each original offset and numeric type: old fingerprint depends on it.
    result["sessions"] = [[day.isoformat(), close.isoformat()] for day, close in contract.sessions]
    result["accepted_timezones"] = list(contract.accepted_timezones)
    return result


def _date(text):
    if type(text) is not str:
        raise ApiFailure("invalid_provenance")
    value = date.fromisoformat(text)
    if value.isoformat() != text:
        raise ApiFailure("invalid_provenance")
    return value


def _contract_from_json(value):
    if (type(value) is not dict or set(value) != _CONTRACT_FIELDS
            or type(value["sessions"]) is not list or not 1 <= len(value["sessions"]) <= 10000
            or type(value["accepted_timezones"]) is not list):
        raise ApiFailure("invalid_provenance")
    result = dict(value)
    result["calendar_valid_from"] = _date(value["calendar_valid_from"])
    result["calendar_valid_until"] = _date(value["calendar_valid_until"])
    sessions = []
    for item in value["sessions"]:
        if type(item) is not list or len(item) != 2 or type(item[1]) is not str:
            raise ApiFailure("invalid_provenance")
        sessions.append((_date(item[0]), datetime.fromisoformat(item[1])))
    result["sessions"] = tuple(sessions)
    result["accepted_timezones"] = tuple(value["accepted_timezones"])
    return DailyContract(**result)


@dataclass(frozen=True, slots=True)
class RequestProvenance:
    contract: DailyContract
    availability: SourceAvailability
    request: DailyRequest
    profile_config_hash: str

    def __post_init__(self):
        if (type(self.contract) is not DailyContract or type(self.availability) is not SourceAvailability
                or type(self.request) is not DailyRequest or not _hash(self.profile_config_hash)
                or not 1 <= len(self.contract.sessions) <= 10000):
            raise ApiFailure("invalid_provenance")
        # DailyContract predates slots. Detach its __dict__ from the caller so a
        # later accidental edit of the live contract cannot rewrite this capture.
        object.__setattr__(self, "contract", replace(self.contract))
        validate_source_availability(self.availability, source_id=self.availability.source_id,
            rule_version=self.availability.rule_version, contract=self.contract)
        if self.request != plan_daily_request(self.contract, self.request.reference_time):
            raise ApiFailure("invalid_provenance")
        for day, close in self.contract.sessions:
            SessionAvailability(day, close)  # Reject nonexistent local closes; retain original offset.
        require_available(self.availability, self.request.sessions, self.request.reference_time)
        if (len(self.manifest_document.encode()) > MAX_MANIFEST_BYTES
                or len(self.request_document.encode()) > MAX_REQUEST_BYTES):
            raise ApiFailure("provenance_size")

    @property
    def manifest_document(self):
        return _canonical({"version": 1, "contract": _contract_json(self.contract),
                           "availability": availability_to_json(self.availability)})

    @property
    def manifest_hash(self):
        return _digest(self.manifest_document)

    @property
    def request_document(self):
        return _canonical({"version": 1, "manifest_hash": self.manifest_hash,
            "profile_config_hash": self.profile_config_hash,
            "request": {"reference_time": self.request.reference_time.isoformat(timespec="microseconds"),
                        "contract_fingerprint": self.request.contract_fingerprint,
                        "sessions": [day.isoformat() for day in self.request.sessions]}})

    @property
    def fingerprint(self):
        return _digest(self.request_document)

    @property
    def config_version(self):
        return _digest(self.profile_config_hash + self.contract.fingerprint + self.availability.fingerprint)

    def to_json(self):
        return {"version": 1, "manifest": json.loads(self.manifest_document),
                "request": json.loads(self.request_document)}


def provenance_from_json(value):
    try:
        if (type(value) is not dict or set(value) != {"version", "manifest", "request"}
                or type(value["version"]) is not int or value["version"] != 1):
            raise ApiFailure("invalid_provenance")
        manifest, record = value["manifest"], value["request"]
        if (type(manifest) is not dict or set(manifest) != {"version", "contract", "availability"}
                or type(manifest["version"]) is not int or manifest["version"] != 1
                or type(record) is not dict
                or set(record) != {"version", "manifest_hash", "profile_config_hash", "request"}
                or type(record["version"]) is not int or record["version"] != 1):
            raise ApiFailure("invalid_provenance")
        request = record["request"]
        if (type(request) is not dict
                or set(request) != {"reference_time", "contract_fingerprint", "sessions"}
                or type(request["reference_time"]) is not str or type(request["sessions"]) is not list
                or not 1 <= len(request["sessions"]) <= 10000):
            raise ApiFailure("invalid_provenance")
        context = DailyRequest(datetime.fromisoformat(request["reference_time"]),
            request["contract_fingerprint"], tuple(_date(day) for day in request["sessions"]))
        result = RequestProvenance(_contract_from_json(manifest["contract"]),
            availability_from_json(manifest["availability"]), context, record["profile_config_hash"])
        if record["manifest_hash"] != result.manifest_hash:
            raise ApiFailure("provenance_integrity")
        return result
    except ApiFailure:
        raise
    except (ValueError, TypeError, KeyError, OverflowError, RecursionError):
        raise ApiFailure("invalid_provenance") from None


def provenance_from_documents(manifest_document, request_document):
    if type(manifest_document) is not str or type(request_document) is not str:
        raise ApiFailure("invalid_provenance")
    if len(manifest_document) > MAX_MANIFEST_BYTES or len(request_document) > MAX_REQUEST_BYTES:
        raise ApiFailure("provenance_size")
    try:
        manifest = decode_json(manifest_document.encode(), max_bytes=MAX_MANIFEST_BYTES)
        record = decode_json(request_document.encode(), max_bytes=MAX_REQUEST_BYTES)
    except UnicodeError:
        raise ApiFailure("invalid_provenance") from None
    result = provenance_from_json({"version": 1, "manifest": manifest, "request": record})
    if manifest_document != result.manifest_document or request_document != result.request_document:
        raise ApiFailure("provenance_noncanonical")
    return result


def validate_provenance_binding(provenance, *, source_id, instrument_id, symbol,
        observed_at, received_at, config_version, metadata, session_dates):
    if type(provenance) is not RequestProvenance:
        raise ApiFailure("invalid_provenance")
    contract = provenance.contract
    if (source_id != provenance.availability.source_id or instrument_id != contract.instrument_id
            or symbol != contract.symbol or config_version != provenance.config_version):
        raise ApiFailure("provenance_binding")
    expected = {"currency": contract.currency, "price_basis": contract.price_basis,
                "schema_version": contract.schema_version, "market_timezone": contract.timezone,
                "market": contract.exchange}
    if metadata != expected:
        raise ApiFailure("provenance_binding")
    if (type(session_dates) is not tuple or any(type(day) is not date for day in session_dates)
            or len(set(session_dates)) != len(session_dates)):
        raise ApiFailure("provenance_binding")
    validate_available_result(provenance.availability, provenance.request, contract,
        {day.isoformat(): None for day in session_dates}, observed_at, received_at)
    return provenance
