"""One owner of retries; immutable credentials and source identity per job."""
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import math
import random
import time
from types import MappingProxyType
from urllib.parse import urlsplit
from uuid import uuid4

from api_adapters import resolve_daily_adapter
from api_availability import (plan_daily_request, require_available,
                              validate_available_result, validate_daily_request,
                              validate_source_availability)
from api_contracts import ApiFailure, PriceBatch, SourceAvailability, decode_json, utc
from api_profiles import ProfileError
from api_provenance import RequestProvenance
from api_state import StateRejected
from api_transport import TransportError, TransportPolicy, request


@dataclass(frozen=True)
class FetchPolicy:
    total_seconds: float = 30
    socket_seconds: float = 10
    max_attempts: int = 3
    max_bytes: int = 1048576  # > compact 100 daily rows; actual bytes, not header trust
    base_delay: float = 1
    max_delay: float = 10

    def __post_init__(self):
        for value in (self.total_seconds, self.socket_seconds, self.base_delay, self.max_delay):
            if type(value) not in (float, int) or not math.isfinite(value) or value <= 0:
                raise ApiFailure("invalid_fetch_policy")
        if self.total_seconds > 300 or self.socket_seconds > self.total_seconds or self.max_delay < self.base_delay:
            raise ApiFailure("invalid_fetch_policy")
        if type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 5:
            raise ApiFailure("invalid_fetch_policy")
        if type(self.max_bytes) is not int or not 1 <= self.max_bytes <= 4 * 1048576:
            raise ApiFailure("invalid_fetch_policy")


def retry_after(value, now):
    if value is None:
        return 0.0
    if not isinstance(value, str) or len(value) > 128:
        return 0.0
    try:
        if value.isascii() and value.isdecimal():
            seconds = int(value)
        else:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                return 0.0
            seconds = parsed.timestamp() - now
        # Very long server pause is retained (finite) rather than retried early.
        if seconds > 31536000:
            raise ApiFailure("retry_after_requires_review")
        return max(0.0, float(seconds))
    except (ValueError, TypeError, OverflowError):
        return 0.0


class ApiClient:
    def __init__(self, state, credentials, *, policy=None, transport=request,
                 monotonic=time.monotonic, wall=time.time, sleep=time.sleep,
                 jitter=random.random, transport_policy=None, availabilities=()):
        self.state, self.credentials = state, credentials
        self.policy = policy or FetchPolicy()
        self.transport = transport
        self.monotonic, self.wall, self.sleep, self.jitter = monotonic, wall, sleep, jitter
        self.transport_policy = transport_policy
        if type(availabilities) is not tuple or any(
                type(item) is not SourceAvailability for item in availabilities):
            raise ApiFailure("invalid_source_availability")
        schedules = {(item.source_id, item.contract_fingerprint): item for item in availabilities}
        if len(schedules) != len(availabilities):
            raise ApiFailure("duplicate_availability")
        self._availabilities = MappingProxyType(schedules)

    def fetch(self, profile, contract, *, total_seconds=None, request_context=None):
        # Suppress untrusted exception contexts at the public boundary.
        try:
            return self._fetch(profile, contract, total_seconds=total_seconds,
                               request_context=request_context)
        except (ProfileError, StateRejected) as error:
            raise ApiFailure(error.code) from None

    def _availability_request(self, profile, contract, request_context, now):
        availability = self._availabilities.get((profile.source_id, contract.fingerprint))
        if profile.availability_rule_version is None:
            if availability is not None or request_context is not None:
                raise ApiFailure("availability_not_enabled")
            return None, None
        validate_source_availability(availability, source_id=profile.source_id,
            rule_version=profile.availability_rule_version, contract=contract)
        if request_context is None:
            request_context = plan_daily_request(contract, now)
        validate_daily_request(request_context, contract, now)
        require_available(availability, request_context.sessions, request_context.reference_time)
        last_close = utc(dict(contract.sessions)[request_context.sessions[-1]])
        if (now - last_close).total_seconds() > contract.max_age_seconds:
            raise ApiFailure("stale_data")
        return availability, request_context

    def _fetch(self, profile, contract, *, total_seconds=None, request_context=None):
        if total_seconds is not None and (type(total_seconds) not in (float, int)
                                         or not math.isfinite(total_seconds) or total_seconds <= 0):
            raise ApiFailure("invalid_fetch_policy")
        budget = self.policy.total_seconds if total_seconds is None else min(total_seconds, self.policy.total_seconds)
        if not math.isfinite(budget) or budget <= 0:
            raise ApiFailure("deadline")
        started = self.monotonic()
        deadline = started + budget
        now = datetime.fromtimestamp(self.wall(), timezone.utc)
        profile.assert_ready(today=now.date())
        parsed = urlsplit(profile.endpoint)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        policy = self.transport_policy
        if policy is None:
            policy = TransportPolicy(
                origin=origin,
                test_mode=profile.environment == "test",
            )
        if not isinstance(policy, TransportPolicy) or type(policy.test_mode) is not bool:
            raise ApiFailure("configuration")
        if policy.test_mode != (profile.environment == "test"):
            raise ApiFailure("environment_mismatch")
        if (policy.origin not in (origin, origin + "/")
                or (not policy.test_mode and policy.ca_file is not None)):
            raise ApiFailure("configuration")
        adapter = resolve_daily_adapter(profile, contract)
        availability, request_context = self._availability_request(profile, contract, request_context, now)
        if availability is None:
            contract.completed_sessions(now)
        elif self.monotonic() >= deadline:
            raise ApiFailure("deadline")
        self.state.register(profile)
        credential = self.credentials.snapshot(profile, self.wall())
        request_id = str(uuid4())
        # A snapshot is captured once; concurrent rotation cannot change this URL.
        url = adapter.build_url(profile.endpoint, contract.symbol, credential.secret)
        for attempt in range(self.policy.max_attempts):
            remaining = deadline - self.monotonic()
            if remaining <= 0:
                raise ApiFailure("deadline")
            self.credentials.assert_active(credential, self.wall())
            lease = self.state.acquire(profile, request_id + ":" + str(attempt),
                                       self.wall(), lease_seconds=remaining + 5)
            try:
                remaining = deadline - self.monotonic()
                if remaining <= 0:
                    raise ApiFailure("deadline")
                response = self.transport(url, timeout=min(self.policy.socket_seconds, remaining),
                                          total_timeout=remaining, max_bytes=self.policy.max_bytes,
                                          policy=policy)
                if response.status != 200:
                    raise TransportError("http", status=response.status,
                                         retry_after=response.headers.get("retry-after"))
                data = decode_json(response.body, max_bytes=self.policy.max_bytes)
                received = datetime.fromtimestamp(self.wall(), timezone.utc)
                evaluation_time = received if request_context is None else request_context.reference_time
                prices, observed = adapter.parse(data, contract, evaluation_time)
                if availability is not None:
                    validate_available_result(availability, request_context, contract,
                                              prices, observed, received)
                # Keep original OHLCV validation as the one shared authority.
                from api_validation import prepare_rows
                prepare_rows(contract.symbol, prices)
                self.credentials.assert_active(credential, self.wall())
                if self.monotonic() >= deadline:
                    raise ApiFailure("deadline")
                metadata = MappingProxyType({"currency": contract.currency,
                    "price_basis": contract.price_basis, "schema_version": contract.schema_version,
                    "market_timezone": contract.timezone, "market": contract.exchange})
                # Bind storage version to BOTH connection profile and calendar/contract.
                binding = profile.config_hash + contract.fingerprint
                if availability is not None:
                    binding += availability.fingerprint
                version = hashlib.sha256(binding.encode()).hexdigest()
                provenance = None if availability is None else RequestProvenance(
                    contract, availability, request_context, profile.config_hash)
                batch = PriceBatch(prices, profile.source_id, contract.instrument_id,
                                  contract.symbol, request_id, observed, received, version,
                                  hashlib.sha256(response.body).hexdigest(), metadata,
                                  state_path=str(self.state.path), request_context=request_context,
                                  provenance=provenance)
                # Snapshot construction is part of the budget and precedes success.
                if provenance is not None:
                    self.credentials.assert_active(credential, self.wall())
                    if self.monotonic() >= deadline:
                        raise ApiFailure("deadline")
                self.state.success(lease, self.wall(), rows=len(prices),
                                   data_age_seconds=(received-observed).total_seconds(),
                                   request_id=request_id)
                return batch
            except TransportError as error:
                transient = error.kind in ("network", "dns", "timeout", "truncated") or (
                    error.kind == "http" and error.status in (408, 429, 500, 502, 503, 504))
                code = ("authentication" if error.status == 401 else
                        "permission_unknown" if error.status == 403 else
                        "quota" if error.status == 429 else error.kind)
                try:
                    server_wait = retry_after(error.retry_after, self.wall())
                except ApiFailure:
                    # Persist a conservative hold; manual review is required.
                    self.state.failure(lease, self.wall(), category="retry_after_requires_review",
                                       retry_after_seconds=31536000, transient=True)
                    raise ApiFailure("retry_after_requires_review") from None
                noise = min(1.0, max(0.0, self.jitter()))
                delay = max(server_wait, min(self.policy.max_delay,
                            self.policy.base_delay * 2 ** attempt) * (0.5 + 0.5 * noise))
                self.state.failure(lease, self.wall(), category=code,
                                   retry_after_seconds=delay if transient else 0, transient=transient)
                if not transient or attempt + 1 == self.policy.max_attempts:
                    raise ApiFailure(code, status=error.status) from None
                if self.monotonic() + delay >= deadline:
                    raise ApiFailure("retry_budget", status=error.status) from None
                self.sleep(delay)
            except (ApiFailure, ProfileError, ValueError) as error:
                code = error.code if isinstance(error, (ApiFailure, ProfileError)) else "invalid_prices"
                self.state.failure(lease, self.wall(), category=code)
                raise ApiFailure(code) from None
            except BaseException as error:
                # Cleanup is allowed for cancellation; errors are never converted to success.
                self.state.release(lease, self.wall())
                if isinstance(error, Exception):
                    raise ApiFailure("unexpected_failure") from None
                raise
        raise ApiFailure("attempt_budget")

    def fetch_with_fallback(self, primary, backup, contract, *, approved_sources=(), request_context=None):
        """Explicit operator approval plus equivalent contracts; never key roulette."""
        if (primary.source_id, backup.source_id) not in approved_sources:
            raise ApiFailure("fallback_not_approved")
        fields = ("currency", "exchange", "timezone", "price_basis", "schema_version")
        if any(getattr(primary, name) != getattr(backup, name) for name in fields):
            raise ApiFailure("fallback_incompatible")
        started = self.monotonic()
        enabled = primary.availability_rule_version is not None
        if enabled != (backup.availability_rule_version is not None):
            raise ApiFailure("fallback_availability_mismatch")
        if enabled or request_context is not None:
            now = datetime.fromtimestamp(self.wall(), timezone.utc)
            # Both sources must cover the SAME target before either starts I/O.
            resolve_daily_adapter(primary, contract)
            resolve_daily_adapter(backup, contract)
            _, request_context = self._availability_request(primary, contract, request_context, now)
            self._availability_request(backup, contract, request_context, now)
        try:
            if request_context is None:
                return self.fetch(primary, contract)
            remaining = self.policy.total_seconds - (self.monotonic() - started)
            if remaining <= 0:
                raise ApiFailure("deadline")
            return self.fetch(primary, contract, total_seconds=remaining, request_context=request_context)
        except ApiFailure as error:
            if error.code not in ("network", "dns", "timeout", "http", "truncated"):
                raise
            if error.code == "http" and error.status not in (408, 500, 502, 503, 504):
                raise
        remaining = self.policy.total_seconds - (self.monotonic() - started)
        if request_context is None:
            return self.fetch(backup, contract, total_seconds=remaining)
        if remaining <= 0:
            raise ApiFailure("deadline")
        return self.fetch(backup, contract, total_seconds=remaining, request_context=request_context)
