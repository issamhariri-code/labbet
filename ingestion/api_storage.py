"""Atomic, append-only API observations; never writes to legacy daily_prices.

observed_at follows the adapter's explicit observation-order contract, not the
time a worker happened to finish. The Alpha daily adapter uses the reviewed
market session's close; this is not a provider publication/revision timestamp.
Equal timestamps with conflicting values
are rejected, rather than resolved by arrival order. An outer transaction, if
present, remains the caller's responsibility; connection.transaction() then
creates a savepoint instead of committing the caller's unrelated work.
"""

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import re
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from api_contracts import ApiFailure
from api_provenance import RequestProvenance, provenance_from_documents, validate_provenance_binding


MAX_ROWS = 100_000
MAX_PRICE = Decimal("999999999999.999999")
MAX_VOLUME = 9_223_372_036_854_775_807
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}\Z")
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_REQUIRED_META = frozenset(("currency", "price_basis", "schema_version", "market_timezone"))
_OPTIONAL_META = frozenset(("market", "adjustment_version"))


@dataclass(frozen=True)
class PersistResult:
    run_id: str
    row_count: int
    inserted_revisions: int
    advanced_current: int
    replayed: bool


def _identifier(value, field):
    if type(value) is not str or not _ID.fullmatch(value):
        raise ValueError(f"API storage: invalid {field}")
    return value


def _timestamp(value, field):
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"API storage: {field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _digest(value):
    return hashlib.sha256(_json(value).encode("ascii")).hexdigest()


def _lock_key(value):
    return int.from_bytes(hashlib.sha256(_json(value).encode("ascii")).digest()[:8], "big", signed=True)


def _metadata(value):
    if type(value) is not dict or not _REQUIRED_META <= value.keys():
        raise ValueError("API storage: required contract metadata is missing")
    if value.keys() - (_REQUIRED_META | _OPTIONAL_META):
        raise ValueError("API storage: unsupported contract metadata")
    result = {}
    for name, item in value.items():
        if (type(item) is not str or not item or len(item) > 128
                or item != item.strip() or any(ord(char) < 32 or ord(char) == 127 for char in item)):
            raise ValueError("API storage: invalid contract metadata")
        result[name] = item
    if not re.fullmatch(r"[A-Z]{3}", result["currency"]):
        raise ValueError("API storage: invalid currency")
    if result["price_basis"] not in ("raw", "adjusted"):
        raise ValueError("API storage: unsupported price basis")
    try:
        if result["market_timezone"] != "UTC":
            ZoneInfo(result["market_timezone"])
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError("API storage: unknown market timezone") from None
    return result


def _rows(value, symbol):
    result = []
    seen = set()
    try:
        iterator = iter(value)
    except TypeError:
        raise ValueError("API storage: rows must be iterable") from None
    for item in iterator:
        if len(result) >= MAX_ROWS:
            raise ValueError("API storage: row limit exceeded")
        if type(item) not in (list, tuple) or len(item) != 7:
            raise ValueError("API storage: invalid row shape")
        row_symbol, trade_date, open_price, high_price, low_price, close_price, volume = item
        if type(row_symbol) is not str or row_symbol != symbol:
            raise ValueError("API storage: row symbol differs from batch")
        if type(trade_date) is not date or trade_date in seen:
            raise ValueError("API storage: invalid or duplicate trade date")
        prices = (open_price, high_price, low_price, close_price)
        for price in prices:
            if type(price) is not Decimal or not price.is_finite() or not 0 < price <= MAX_PRICE:
                raise ValueError("API storage: invalid price")
            try:
                with localcontext() as context:
                    context.prec = 32
                    if price != price.quantize(Decimal("0.000001")):
                        raise ValueError("API storage: price exceeds six decimal places")
            except InvalidOperation:
                raise ValueError("API storage: invalid price precision") from None
        if not low_price <= open_price <= high_price or not low_price <= close_price <= high_price:
            raise ValueError("API storage: inconsistent OHLC range")
        if type(volume) is not int or not 0 <= volume <= MAX_VOLUME:
            raise ValueError("API storage: invalid volume")
        seen.add(trade_date)
        canonical = [trade_date.isoformat(), *(format(price, ".6f") for price in prices), volume]
        result.append((tuple(item), _digest(canonical), canonical))
    if not result:
        raise ValueError("API storage: empty batch")
    return sorted(result, key=lambda entry: entry[0][1])


def _batch_fingerprint(*, source_id, instrument_id, symbol, request_id, observed_at,
                       received_at, config_version, payload_hash, metadata, prepared,
                       provenance_hash=None):
    values = {"source": source_id, "instrument": instrument_id, "symbol": symbol,
        "request": request_id, "observed": observed_at.isoformat(),
        "received": received_at.isoformat(), "config": config_version,
        "payload": payload_hash, "contract": metadata,
        "rows": [item[2] for item in prepared]}
    if provenance_hash is not None:
        values["request_provenance"] = provenance_hash
    return _digest(values)


def _read_provenance_record(cursor, run_id):
    cursor.execute("""
        SELECT p.manifest_hash, p.provenance_hash, m.canonical_document, p.canonical_document
        FROM labbet_api.request_provenance p
        JOIN labbet_api.availability_manifests m USING (manifest_hash)
        WHERE p.run_id = %s
    """, (run_id,))
    row = cursor.fetchone()
    if row is None:
        return None
    try:
        result = provenance_from_documents(row[2], row[3])
        if (result.manifest_hash, result.fingerprint) != row[:2]:
            raise ApiFailure("provenance_integrity")
        return result
    except ApiFailure:
        raise ApiFailure("provenance_integrity") from None


def persist_batch(connection, *, source_id, instrument_id, symbol, request_id,
                  observed_at, received_at, config_version, payload_hash, rows,
                  metadata, provenance=None):
    """Persist one validated observation atomically, or replay its original run.

    The stream identity is source + instrument + contract metadata. request_id
    is unique within a source, even across instruments. Reusing it with changed
    metadata, timestamps or data is an error. Immutable revisions retain older
    observations; only a strictly newer observed_at can advance a current row.
    No credentials, raw response bodies or arbitrary metadata are accepted.
    """
    source_id = _identifier(source_id, "source_id")
    instrument_id = _identifier(instrument_id, "instrument_id")
    symbol = _identifier(symbol, "symbol")
    request_id = _identifier(request_id, "request_id")
    config_version = _identifier(config_version, "config_version")
    if type(payload_hash) is not str or not _HASH.fullmatch(payload_hash):
        raise ValueError("API storage: payload_hash must be lowercase SHA-256")
    observed_at = _timestamp(observed_at, "observed_at")
    received_at = _timestamp(received_at, "received_at")
    if observed_at > received_at:
        raise ValueError("API storage: observation is later than reception")
    metadata = _metadata(metadata)
    prepared = _rows(rows, symbol)
    contract_hash = _digest(metadata)
    manifest_document = request_document = manifest_hash = provenance_hash = None
    if provenance is not None:
        if type(provenance) is not RequestProvenance:
            raise ApiFailure("invalid_provenance")
        # Capture bytes before entering SQL. Reconstruct once to reject mixed
        # documents and detach any caller-owned contract aliases.
        manifest_document, request_document = provenance.manifest_document, provenance.request_document
        captured = provenance_from_documents(manifest_document, request_document)
        manifest_hash, provenance_hash = captured.manifest_hash, captured.fingerprint
        validate_provenance_binding(captured, source_id=source_id, instrument_id=instrument_id,
            symbol=symbol, observed_at=observed_at, received_at=received_at,
            config_version=config_version, metadata=metadata,
            session_dates=tuple(item[0][1] for item in prepared))
    batch_hash = _batch_fingerprint(source_id=source_id, instrument_id=instrument_id,
        symbol=symbol, request_id=request_id, observed_at=observed_at, received_at=received_at,
        config_version=config_version, payload_hash=payload_hash, metadata=metadata, prepared=prepared,
        provenance_hash=provenance_hash)

    with connection.transaction():
        with connection.cursor() as cursor:
            # Every writer takes locks in the same order. Hash collisions only
            # serialize unrelated work; full database keys still define identity.
            cursor.execute("SELECT pg_catalog.pg_advisory_xact_lock(%s)",
                           (_lock_key(["request", source_id, request_id]),))
            cursor.execute("SELECT pg_catalog.pg_advisory_xact_lock(%s)",
                           (_lock_key(["stream", source_id, instrument_id, contract_hash]),))
            cursor.execute("""
                SELECT run_id, batch_hash, row_count FROM labbet_api.ingestion_runs
                WHERE source_id = %s AND request_id = %s
            """, (source_id, request_id))
            prior = cursor.fetchone()
            if prior:
                if prior[1] != batch_hash:
                    raise ValueError("API storage: request_id was reused with different content")
                if provenance is not None:
                    archived = _read_provenance_record(cursor, prior[0])
                    if (archived is None or archived.manifest_document != manifest_document
                            or archived.request_document != request_document):
                        raise ApiFailure("provenance_integrity")
                return PersistResult(str(prior[0]), prior[2], 0, 0, True)

            # Equal version timestamps cannot safely order contradictory facts.
            # Check all historical observations, not only the current pointer.
            for row, content_hash, _ in prepared:
                cursor.execute("""
                    SELECT 1 FROM labbet_api.price_revisions
                    WHERE source_id = %s AND instrument_id = %s AND contract_hash = %s
                      AND trade_date = %s AND observed_at = %s AND content_hash <> %s
                    LIMIT 1
                """, (source_id, instrument_id, contract_hash, row[1], observed_at, content_hash))
                if cursor.fetchone():
                    raise ValueError("API storage: conflicting values for the same observation timestamp")

            run_id = str(uuid4())
            cursor.execute("""
                INSERT INTO labbet_api.ingestion_runs
                (run_id, source_id, instrument_id, symbol, request_id, observed_at,
                 received_at, config_version, payload_hash, batch_hash, contract_hash,
                 contract_metadata, row_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            """, (run_id, source_id, instrument_id, symbol, request_id, observed_at,
                  received_at, config_version, payload_hash, batch_hash, contract_hash,
                  _json(metadata), len(prepared)))
            if provenance is not None:
                cursor.execute("""
                    INSERT INTO labbet_api.availability_manifests (manifest_hash, canonical_document)
                    VALUES (%s, %s) ON CONFLICT (manifest_hash) DO NOTHING
                """, (manifest_hash, manifest_document))
                cursor.execute("SELECT canonical_document FROM labbet_api.availability_manifests WHERE manifest_hash=%s",
                               (manifest_hash,))
                if cursor.fetchone() != (manifest_document,):
                    raise ApiFailure("provenance_integrity")
                cursor.execute("""
                    INSERT INTO labbet_api.request_provenance
                    (run_id, manifest_hash, provenance_hash, canonical_document) VALUES (%s, %s, %s, %s)
                """, (run_id, manifest_hash, provenance_hash, request_document))
            advanced = 0
            for row, content_hash, _ in prepared:
                revision_id = str(uuid4())
                cursor.execute("""
                    INSERT INTO labbet_api.price_revisions
                    (revision_id, run_id, source_id, instrument_id, contract_hash,
                     trade_date, observed_at, open_price, high_price, low_price,
                     close_price, volume, content_hash)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (revision_id, run_id, source_id, instrument_id, contract_hash,
                      row[1], observed_at, *row[2:], content_hash))
                cursor.execute("""
                    INSERT INTO labbet_api.current_prices
                    (source_id, instrument_id, contract_hash, trade_date, revision_id, observed_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_id, instrument_id, contract_hash, trade_date)
                    DO UPDATE SET revision_id = EXCLUDED.revision_id,
                                  observed_at = EXCLUDED.observed_at
                    WHERE EXCLUDED.observed_at > labbet_api.current_prices.observed_at
                """, (source_id, instrument_id, contract_hash, row[1], revision_id, observed_at))
                advanced += cursor.rowcount
            return PersistResult(run_id, len(prepared), len(prepared), advanced, False)


def read_request_provenance(connection, *, source_id, request_id):
    """Read and audit a committed request against its actual immutable price rows.

    None means an explicitly legacy run, not a fabricated reference time. The new
    schema must be installed to use this audit API; legacy writes remain usable
    without it. History is evaluated against archived rules, never today's clock.
    """
    source_id, request_id = _identifier(source_id, "source_id"), _identifier(request_id, "request_id")
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT run_id, instrument_id, symbol, observed_at, received_at, config_version,
                       payload_hash, batch_hash, contract_metadata, row_count, contract_hash
                FROM labbet_api.ingestion_runs WHERE source_id=%s AND request_id=%s
            """, (source_id, request_id))
            run = cursor.fetchone()
            if run is None:
                raise ApiFailure("unknown_request")
            provenance = _read_provenance_record(cursor, run[0])
            cursor.execute("""
                SELECT trade_date, open_price, high_price, low_price, close_price, volume,
                       content_hash, source_id, instrument_id, contract_hash, observed_at
                FROM labbet_api.price_revisions WHERE run_id=%s ORDER BY trade_date
            """, (run[0],))
            stored = cursor.fetchmany(MAX_ROWS + 1)
            try:
                observed_at = _timestamp(run[3], "observed_at")
                received_at = _timestamp(run[4], "received_at")
                if observed_at > received_at:
                    raise ValueError
                if not 1 <= len(stored) == run[9] <= MAX_ROWS:
                    raise ValueError
                metadata = _metadata(run[8])
                if _digest(metadata) != run[10]:
                    raise ValueError
                prepared = _rows([(run[2], *row[:6]) for row in stored], run[2])
                for archived, computed in zip(stored, prepared):
                    if (archived[6] != computed[1] or archived[7:] != (source_id, run[1], run[10], run[3])):
                        raise ValueError
                if provenance is not None:
                    validate_provenance_binding(provenance, source_id=source_id,
                        instrument_id=run[1], symbol=run[2], observed_at=observed_at, received_at=received_at,
                        config_version=run[5], metadata=metadata,
                        session_dates=tuple(item[0][1] for item in prepared))
                expected = _batch_fingerprint(source_id=source_id, instrument_id=run[1], symbol=run[2],
                    request_id=request_id, observed_at=observed_at, received_at=received_at, config_version=run[5],
                    payload_hash=run[6], metadata=metadata, prepared=prepared,
                    provenance_hash=None if provenance is None else provenance.fingerprint)
                if expected != run[7]:
                    raise ValueError
            except (ApiFailure, ValueError, TypeError, KeyError, IndexError):
                raise ApiFailure("provenance_integrity") from None
            return provenance
