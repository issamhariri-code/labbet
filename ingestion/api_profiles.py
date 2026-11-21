"""Immutable source identity and versioned credentials; no secrets in metadata."""

from dataclasses import asdict, dataclass, field, replace
from datetime import date
import hashlib
import ipaddress
import json
import math
import re
import threading
import unicodedata
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from api_adapters import daily_adapter_spec
from api_contracts import ApiFailure


class ProfileError(ValueError):
    """Public messages deliberately contain neither supplied values nor secrets."""

    def __init__(self, code):
        self.code = code
        super().__init__("API configuration rejected: " + code)


def _identifier(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", value)


def _finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


@dataclass(frozen=True, slots=True)
class QuotaLimit:
    max_credits: int
    window_seconds: float

    def __post_init__(self):
        if type(self.max_credits) is not int or not 1 <= self.max_credits <= 100000:
            raise ProfileError("invalid_resource_limit")
        if (type(self.window_seconds) not in (int, float)
                or not 0 < self.window_seconds <= 31536000):
            raise ProfileError("invalid_resource_limit")
        object.__setattr__(self, "window_seconds", float(self.window_seconds))


def quota_limits_from_json(values):
    if type(values) is not list or any(
        type(value) is not dict
        or set(value) != {"max_credits", "window_seconds"}
        for value in values
    ):
        raise ProfileError("invalid_quota_limits")
    return tuple(
        QuotaLimit(value["max_credits"], value["window_seconds"])
        for value in values
    )


@dataclass(frozen=True, slots=True)
class SourceProfile:
    source_id: str
    account_ref: str
    environment: str
    endpoint: str
    credential_env: str
    credential_version: str
    quota_scope: str
    credential_expires_at: float | None = None
    config_version: str = "1"
    owner: str = "operator"
    purpose: str = "market-data-read"
    account_region: str | None = None
    endpoint_region: str | None = None
    required_regions: bool = False
    allowed_account_regions: tuple[str, ...] = ()
    allowed_endpoint_regions: tuple[str, ...] = ()
    allowed_runtime_ips: tuple[str, ...] = ()
    runtime_ip: str | None = None
    timezone: str = "UTC"
    currency: str | None = None
    exchange: str | None = None
    schema_version: str = "1"
    price_basis: str = "raw"
    rights_verified: bool = False
    rights_review_date: str | None = None
    review_due_date: str | None = None
    verification_reference: str | None = None
    max_attempts_per_window: int = 1
    quota_window_seconds: float = 60.0
    max_concurrency: int = 2
    circuit_failure_threshold: int = 3
    circuit_reset_seconds: float = 30.0
    extra_quota_limits: tuple[QuotaLimit, ...] = ()
    adapter_id: str = "alphavantage-daily-v1"
    availability_rule_version: str | None = None

    def __post_init__(self):
        try:
            spec = daily_adapter_spec(self.adapter_id)
        except ApiFailure as error:
            raise ProfileError(error.code) from None
        if self.availability_rule_version is not None and (
                type(self.availability_rule_version) is not str
                or self.availability_rule_version not in spec.availability_rules):
            raise ProfileError("unsupported_availability_rule")
        for name in ("source_id", "account_ref", "credential_version", "quota_scope", "config_version", "owner", "purpose", "schema_version"):
            if not _identifier(getattr(self, name)):
                raise ProfileError("invalid_identifier")
        if self.environment not in ("test", "production"):
            raise ProfileError("invalid_environment")
        if self.credential_expires_at is not None:
            if not _finite(self.credential_expires_at) or self.credential_expires_at < 0:
                raise ProfileError("invalid_credential_expiry")
            object.__setattr__(self, "credential_expires_at", float(self.credential_expires_at))
        if not isinstance(self.credential_env, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", self.credential_env):
            raise ProfileError("invalid_credential_reference")
        if not isinstance(self.endpoint, str) or not self.endpoint.isascii() or any(ord(c) <= 32 or ord(c) == 127 for c in self.endpoint):
            raise ProfileError("invalid_endpoint")
        try:
            parsed = urlsplit(self.endpoint)
            port = parsed.port
        except ValueError:
            raise ProfileError("invalid_endpoint") from None
        if parsed.scheme != "https" or not parsed.hostname or parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment or "\\" in self.endpoint:
            raise ProfileError("invalid_endpoint")
        if port is not None and not 1 <= port <= 65535:
            raise ProfileError("invalid_endpoint")
        if self.price_basis not in ("raw", "adjusted") or self.purpose != "market-data-read":
            raise ProfileError("invalid_data_permission")
        for name in ("account_region", "endpoint_region", "exchange"):
            value = getattr(self, name)
            if value is not None and not _identifier(value):
                raise ProfileError("invalid_market_metadata")
        for name in ("allowed_account_regions", "allowed_endpoint_regions"):
            values = getattr(self, name)
            if not isinstance(values, tuple) or any(not _identifier(value) for value in values):
                raise ProfileError("invalid_region_policy")
        if self.currency is not None and (not isinstance(self.currency, str) or not re.fullmatch(r"[A-Z]{3}", self.currency)):
            raise ProfileError("invalid_currency")
        if not isinstance(self.timezone, str):
            raise ProfileError("invalid_timezone")
        try:
            ZoneInfo(self.timezone)
        except (ValueError, ZoneInfoNotFoundError):
            raise ProfileError("invalid_timezone") from None
        if type(self.required_regions) is not bool or type(self.rights_verified) is not bool:
            raise ProfileError("invalid_verification_flag")
        if not isinstance(self.allowed_runtime_ips, tuple):
            raise ProfileError("mutable_ip_policy")
        try:
            for network in self.allowed_runtime_ips:
                ipaddress.ip_network(network, strict=True)
            if self.runtime_ip is not None:
                ipaddress.ip_address(self.runtime_ip)
        except (ValueError, TypeError):
            raise ProfileError("invalid_ip_policy") from None
        for name in ("rights_review_date", "review_due_date"):
            value = getattr(self, name)
            if value is not None:
                try:
                    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
                        raise ValueError
                except (ValueError, TypeError):
                    raise ProfileError("invalid_review_date") from None
        if self.rights_review_date and self.review_due_date and self.review_due_date < self.rights_review_date:
            raise ProfileError("invalid_review_period")
        if self.verification_reference is not None:
            # A document identifier or clean documentation URL, never a signed
            # URL, credential-bearing query, raw response or multiline note.
            value = self.verification_reference
            if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 _.:/#-]{0,255}", value):
                raise ProfileError("invalid_verification_reference")
            if "://" in value:
                reference = urlsplit(value)
                if reference.scheme != "https" or not reference.hostname or reference.username is not None or reference.query:
                    raise ProfileError("invalid_verification_reference")
        for name in ("max_attempts_per_window", "max_concurrency", "circuit_failure_threshold"):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= 100000:
                raise ProfileError("invalid_resource_limit")
        for name in ("quota_window_seconds", "circuit_reset_seconds"):
            value = getattr(self, name)
            if not _finite(value) or not 0 < value <= 31536000:
                raise ProfileError("invalid_resource_limit")
            # Equivalent JSON durations (60 and 60.0) must have one fingerprint
            # and one cross-process quota policy representation.
            object.__setattr__(self, name, float(value))

        if type(self.extra_quota_limits) is not tuple or any(
            type(limit) is not QuotaLimit
            for limit in self.extra_quota_limits
        ):
            raise ProfileError("invalid_quota_limits")

        object.__setattr__(
            self,
            "extra_quota_limits",
            tuple(sorted(
                self.extra_quota_limits,
                key=lambda limit: (limit.window_seconds, limit.max_credits),
            )),
        )

    def assert_ready(self, *, today=None, expected_environment=None):
        """Declared evidence gates use of a profile; no geolocation is inferred."""
        today = date.today() if today is None else today
        if expected_environment is not None and self.environment != expected_environment:
            raise ProfileError("environment_mismatch")
        if not self.rights_verified or not self.rights_review_date or not self.review_due_date:
            raise ProfileError("rights_not_verified")
        if date.fromisoformat(self.rights_review_date) > today or date.fromisoformat(self.review_due_date) < today:
            raise ProfileError("rights_review_expired")
        if self.required_regions and (not self.account_region or not self.endpoint_region):
            raise ProfileError("region_not_verified")
        if self.allowed_account_regions and self.account_region not in self.allowed_account_regions:
            raise ProfileError("account_region_denied")
        if self.allowed_endpoint_regions and self.endpoint_region not in self.allowed_endpoint_regions:
            raise ProfileError("endpoint_region_denied")
        if self.allowed_runtime_ips:
            if self.runtime_ip is None:
                raise ProfileError("runtime_ip_not_verified")
            address = ipaddress.ip_address(self.runtime_ip)
            if not any(address in ipaddress.ip_network(network) for network in self.allowed_runtime_ips):
                raise ProfileError("runtime_ip_denied")

    @property
    def quota_limits(self):
        return (
            QuotaLimit(
                self.max_attempts_per_window,
                self.quota_window_seconds,
            ),
        ) + self.extra_quota_limits

    def metadata(self):
        values = asdict(self)
        if values.get("extra_quota_limits") == ():
            values.pop("extra_quota_limits")
        if values.get("adapter_id") == "alphavantage-daily-v1":
            values.pop("adapter_id")
        if values.get("availability_rule_version") is None:
            values.pop("availability_rule_version")
        return values

    @property
    def config_hash(self):
        body = json.dumps(self.metadata(), sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        return hashlib.sha256(body).hexdigest()

    @property
    def credential_identity(self):
        return (self.source_id, self.account_ref, self.environment, self.credential_env)

    @property
    def cache_namespace(self):
        # Version includes account, rights, schema, endpoint and adjustment method.
        return self.source_id + ":" + self.config_hash


@dataclass(frozen=True, slots=True)
class CredentialSnapshot:
    identity: tuple[str, str, str, str]
    version: str
    secret: str = field(repr=False, compare=False)
    expires_at: float | None = None


class CredentialStore:
    """Memory-only secrets; optional persistent version/revocation metadata.

    Rotation keeps already-issued versions valid unless explicitly revoked.
    Revocation rejects their late results too. Reloading a revoked version never
    clears its persistent revocation. Existing secrets are never silently replaced.
    """

    def __init__(self, state_store=None):
        self._state = state_store
        self._lock = threading.RLock()
        self._values = {}
        self._active = {}
        self._revoked = set()
        self._revoked_versions = set()
        self._closed = False

    def install(self, profile, environ, *, expires_at=None):
        if expires_at is None:
            expires_at = profile.credential_expires_at
        elif profile.credential_expires_at is not None and expires_at != profile.credential_expires_at:
            raise ProfileError("credential_expiry_mismatch")
        try:
            secret = environ[profile.credential_env]
        except (KeyError, TypeError):
            raise ProfileError("missing_credential") from None
        if not isinstance(secret, str) or not secret.strip() or len(secret) > 8192 or any(unicodedata.category(c) in ("Cc", "Cf", "Cs") for c in secret):
            raise ProfileError("invalid_credential")
        if expires_at is not None and (not _finite(expires_at) or expires_at < 0):
            raise ProfileError("invalid_credential_expiry")
        key = (profile.credential_identity, profile.credential_version)
        snapshot = CredentialSnapshot(profile.credential_identity, profile.credential_version, secret, expires_at)
        with self._lock:
            if self._closed:
                raise ProfileError("credential_store_closed")
            if key in self._revoked or (profile.source_id, profile.credential_version) in self._revoked_versions:
                raise ProfileError("credential_revoked")
            old = self._values.get(key)
            if old is not None and (old.secret != secret or old.expires_at != expires_at):
                raise ProfileError("credential_version_reused")
            if self._state is not None:
                self._active[profile.credential_identity] = self._state.register_credential(profile, secret=secret, expires_at=expires_at)
            elif old is None:
                self._active[profile.credential_identity] = profile.credential_version
            # Reloading identical configuration must not invalidate in-flight
            # snapshots from earlier calls in the same process.
            self._values[key] = old if old is not None else snapshot
            return self._values[key]

    def snapshot(self, profile, now):
        key = (profile.credential_identity, profile.credential_version)
        with self._lock:
            snapshot = self._values.get(key)
            if snapshot is None:
                raise ProfileError("credential_not_installed")
            self.assert_active(snapshot, now)
            if self._state is not None:
                self._state.assert_credential_active(snapshot.identity, snapshot.version, now, require_current=True)
            elif self._active.get(snapshot.identity) != snapshot.version:
                raise ProfileError("credential_superseded")
            return snapshot

    def assert_active(self, snapshot, now):
        if not _finite(now):
            raise ProfileError("invalid_clock")
        key = (snapshot.identity, snapshot.version)
        with self._lock:
            installed = self._values.get(key)
            if installed is not snapshot:
                raise ProfileError("unknown_credential_snapshot")
            if key in self._revoked:
                raise ProfileError("credential_revoked")
            if snapshot.expires_at is not None and now >= snapshot.expires_at:
                raise ProfileError("credential_expired")
            if self._state is not None:
                self._state.assert_credential_active(snapshot.identity, snapshot.version, now)

    def rotate(self, profile, new_version, environ, *, now, expires_at=None, config_version=None):
        if new_version == profile.credential_version:
            raise ProfileError("credential_version_reused")
        if not _finite(now):
            raise ProfileError("invalid_clock")
        updated = replace(profile, credential_version=new_version, credential_expires_at=expires_at,
                          config_version=config_version or (profile.config_version + "." + new_version))
        with self._lock:
            # Validate replacement entirely before exposing it to new work.
            if expires_at is not None and (not _finite(expires_at) or expires_at <= now):
                raise ProfileError("credential_expired")
            self.install(updated, environ, expires_at=expires_at)
        return updated

    def revoke(self, source_id, version, *, now):
        if not _identifier(source_id) or not _identifier(version) or not _finite(now):
            raise ProfileError("invalid_revocation")
        with self._lock:
            keys = [key for key in self._values if key[0][0] == source_id and key[1] == version]
            if self._state is not None:
                self._state.revoke_credential(source_id, version, now)
            self._revoked.update(keys)
            self._revoked_versions.add((source_id, version))

    def close(self):
        with self._lock:
            self._closed = True
            self._values.clear()
            self._active.clear()
