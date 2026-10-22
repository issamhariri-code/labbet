"""Validate all rows before connecting; persist a complete, attributed batch."""
import json
import psycopg

from market_data import fetch_daily_prices
from api_storage import persist_batch
from api_validation import prepare_rows


symbol = "ONDS"     # fyll i symbol; måste också finnas i det granskade instrumentregistret
DB_OPTIONS = "-c statement_timeout=15000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=30000"


def run(symbol, *, source_id=None):
    if not symbol.strip():  # stoppar innan API-anrop om symbol är tom
        raise ValueError("Fyll i symbol innan körning")
    if source_id is None:
        batch = fetch_daily_prices(symbol)
    else:
        batch = fetch_daily_prices(symbol, source_id=source_id)
    rows = prepare_rows(symbol, batch.prices)
    try:
        with psycopg.connect(connect_timeout=10, options=DB_OPTIONS) as connection:
            result = persist_batch(connection, rows=rows, **batch.storage_context)
    except BaseException:
        # Failure reporting is best effort and must never replace the database
        # exception or cancellation. A completed fetch alone is not a commit.
        try:
            if getattr(batch, "state_path", None) is not None:
                from market_data import record_storage_outcome
                record_storage_outcome(batch, succeeded=False)
        except BaseException:
            pass
        raise
    # This must follow the connection context's successful commit. If the
    # separate health journal fails, CLI reports failure; committed SQL remains.
    if getattr(batch, "state_path", None) is not None:
        from market_data import record_storage_outcome
        record_storage_outcome(batch, succeeded=True)
    print(json.dumps({"source": batch.source_id, "request_id": batch.request_id,
                      "rows": len(rows), "status": "stored", "replayed": result.replayed}))


def cli():
    """Public process boundary: never render raw exceptions, URLs or DSNs."""
    import re
    import os
    import sys
    from api_contracts import ApiFailure

    try:
        if type(symbol) is str and not symbol.strip():
            print(json.dumps({"status": "error", "code": "fill_symbol"}), file=sys.stderr)
            return 1
        source_id = os.environ.get("LABBET_API_SOURCE")
        if source_id is None:
            run(symbol)
        else:
            run(symbol, source_id=source_id)
        return 0
    except ApiFailure as error:
        code = error.code
        if type(code) is not str or not re.fullmatch(r"[a-z_]{1,64}", code):
            code = "unknown"
        message = {"status": "error", "code": code}
        if type(error.status) is int and 100 <= error.status <= 599:
            message["http_status"] = error.status
        print(json.dumps(message), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print(json.dumps({"status": "cancelled", "code": "cancelled"}), file=sys.stderr)
        return 130
    except Exception:
        print(json.dumps({"status": "error", "code": "ingestion_failed"}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(cli())
