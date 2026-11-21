"""Provider-specific daily adapters; no I/O or retained request state."""
from dataclasses import dataclass
from types import MappingProxyType
from urllib.parse import urlencode

from api_contracts import ApiFailure, alpha_daily


class AlphaDailyAdapter:
    __slots__ = ()
    adapter_id = "alphavantage-daily-v1"

    @staticmethod
    def build_url(endpoint, symbol, secret):
        return endpoint + "?" + urlencode({
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "compact",
            "apikey": secret,
        })

    @staticmethod
    def parse(data, contract, received):
        return alpha_daily(data, contract, received)


@dataclass(frozen=True, slots=True)
class DailyAdapterSpec:
    adapter_type: type
    production_endpoints: tuple[str, ...]
    supported_contracts: frozenset[tuple[str, str]]
    availability_rules: frozenset[str] = frozenset()


_DAILY_ADAPTERS = MappingProxyType({
    AlphaDailyAdapter.adapter_id: DailyAdapterSpec(
        adapter_type=AlphaDailyAdapter,
        production_endpoints=("https://www.alphavantage.co/query",),
        supported_contracts=frozenset({
            ("raw", "alphavantage-daily-v1"),
        }),
        # Explicit reviewed bounds, not a claim about provider publication.
        availability_rules=frozenset({"reviewed-session-bounds-v1"}),
    ),
})


def daily_adapter_spec(adapter_id):
    if type(adapter_id) is not str:
        raise ApiFailure("unsupported_adapter")
    spec = _DAILY_ADAPTERS.get(adapter_id)
    if spec is None:
        raise ApiFailure("unsupported_adapter")
    return spec


def resolve_daily_adapter(profile, contract):
    spec = daily_adapter_spec(profile.adapter_id)
    rule = getattr(profile, "availability_rule_version", None)
    if rule is not None and (type(rule) is not str or rule not in spec.availability_rules):
        raise ApiFailure("unsupported_availability_rule")
    if (profile.environment == "production"
            and profile.endpoint not in spec.production_endpoints):
        raise ApiFailure("adapter_endpoint_mismatch")
    fields = ("currency", "exchange", "timezone", "price_basis", "schema_version")
    if any(getattr(profile, name) != getattr(contract, name) for name in fields):
        raise ApiFailure("source_contract_mismatch")
    if (contract.price_basis, contract.schema_version) not in spec.supported_contracts:
        raise ApiFailure("adapter_contract_mismatch")
    return spec.adapter_type()
