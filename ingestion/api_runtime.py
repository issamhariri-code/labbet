"""Load a reviewed, secret-free registry. Real keys are read only at runtime."""
from datetime import date, datetime, timezone
import math
import os
from pathlib import Path

from api_adapters import daily_adapter_spec, resolve_daily_adapter
from api_availability import availability_from_json, validate_source_availability
from api_client import ApiClient, FetchPolicy
from api_contracts import ApiFailure, DailyContract, decode_json, identifier
from api_profiles import CredentialStore, ProfileError, SourceProfile, quota_limits_from_json
from api_state import StateStore


def load_runtime(symbol, environ=None, *, source_id=None):
    if source_id is not None and not identifier(source_id):
        raise ApiFailure("invalid_source_id")
    environ = os.environ if environ is None else environ
    location = environ.get("LABBET_API_CONFIG")
    if not location:
        raise ApiFailure("configuration_required")
    try:
        with open(location, "rb") as handle:
            data = decode_json(handle.read(65537), max_bytes=65536)
        if type(data) is not dict or set(data) != {"version", "environment", "state_path", "sources"} or type(data["version"]) is not int or data["version"] != 1:
            raise ApiFailure("configuration_schema")
        if data["environment"] != "production" or not isinstance(data["sources"], list) or not 1 <= len(data["sources"]) <= 16:
            raise ApiFailure("configuration_schema")
        matches = []
        names = set()
        for entry in data["sources"]:
            if type(entry) is not dict or set(entry) != {"adapter", "profile", "fetch_policy", "instruments"}:
                raise ApiFailure("configuration_schema")
            spec = daily_adapter_spec(entry["adapter"])
            if (type(entry["profile"]) is not dict or type(entry["fetch_policy"]) is not dict
                    or type(entry["instruments"]) is not list):
                raise ApiFailure("configuration_schema")
            profile_data = dict(entry["profile"])
            profile_data.setdefault("adapter_id", entry["adapter"])
            for field in ("allowed_runtime_ips", "allowed_account_regions", "allowed_endpoint_regions"):
                if field in profile_data:
                    if type(profile_data[field]) is not list:
                        raise ApiFailure("configuration_schema")
                    profile_data[field] = tuple(profile_data[field])
            if "extra_quota_limits" in profile_data:
                profile_data["extra_quota_limits"] = quota_limits_from_json(
                    profile_data["extra_quota_limits"]
                )

            profile = SourceProfile(**profile_data)
            if profile.adapter_id != entry["adapter"]:
                raise ApiFailure("adapter_identity_mismatch")
            if profile.source_id in names:
                raise ApiFailure("duplicate_source")
            names.add(profile.source_id)
            if profile.endpoint not in spec.production_endpoints:
                raise ApiFailure("adapter_endpoint_mismatch")
            if profile.environment != "production":
                raise ApiFailure("environment_mismatch")
            for raw in entry["instruments"]:
                if type(raw) is not dict or type(raw.get("symbol")) is not str:
                    raise ApiFailure("configuration_schema")
                selected = raw.get("symbol") == symbol and (
                    source_id is None or profile.source_id == source_id)
                if (not selected and profile.availability_rule_version is None
                        and "availability" not in raw):
                    continue
                values = dict(raw)
                raw_availability = values.pop("availability", None)
                for field in ("calendar_valid_from", "calendar_valid_until"):
                    values[field] = date.fromisoformat(values[field])
                values["sessions"] = tuple((date.fromisoformat(day), datetime.fromisoformat(close))
                                            for day, close in values["sessions"])
                values["accepted_timezones"] = tuple(values.get("accepted_timezones", ()))
                contract = DailyContract(**values)
                resolve_daily_adapter(profile, contract)
                availability = None
                if profile.availability_rule_version is not None:
                    if raw_availability is None:
                        raise ApiFailure("availability_required")
                    availability = availability_from_json(raw_availability)
                    validate_source_availability(availability, source_id=profile.source_id,
                        rule_version=profile.availability_rule_version, contract=contract)
                elif "availability" in raw:
                    raise ApiFailure("availability_not_enabled")
                if selected:
                    matches.append((profile, contract, FetchPolicy(**entry["fetch_policy"]), availability))
        if len(matches) != 1:
            raise ApiFailure("unknown_or_ambiguous_instrument")
        path = Path(data["state_path"])
        if not path.is_absolute() or not path.parent.is_dir():
            raise ApiFailure("state_path_required")
        profile, contract, policy, availability = matches[0]
        profile.assert_ready(expected_environment="production")
        state = StateStore(path)
        credentials = CredentialStore(state)
        try:
            credentials.install(profile, environ)
        except BaseException:
            credentials.close()
            state.close()
            raise
        if availability is None:
            return ApiClient(state, credentials, policy=policy), profile, contract
        return ApiClient(state, credentials, policy=policy, availabilities=(availability,)), profile, contract
    except ApiFailure:
        raise
    except ProfileError as error:
        raise ApiFailure(error.code) from None
    except (OSError, KeyError, TypeError, ValueError):
        raise ApiFailure("configuration_invalid") from None


def health_report(state, source_id, *, now, max_silence_seconds, max_data_age_seconds=None):
    """Evaluate an explicit SLO; caller decides how/when to deliver alerts."""
    if max_data_age_seconds is None:
        max_data_age_seconds = max_silence_seconds
    if any(type(value) not in (int, float) or not math.isfinite(value) or value <= 0
           for value in (max_silence_seconds, max_data_age_seconds)):
        raise ApiFailure("invalid_health_policy")
    result = state.health(source_id)
    last = result.get("last_success") if result else None
    data_age = result.get("data_age_seconds") if result else None
    stale = (last is None or now < last or now - last > max_silence_seconds or
             data_age is None or data_age + now - last > max_data_age_seconds)
    changed = state.observe(source_id, "stale", now, category="no_recent_success") if stale else False
    return {"source_id": source_id, "stale": stale, "changed": changed, "health": result}
