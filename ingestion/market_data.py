"""Public fetch entry point. One reviewed source/credential snapshot per request."""
from api_runtime import load_runtime
import time

from api_state import StateStore


def fetch_daily_prices(symbol, *, source_id=None):
    if source_id is None:
        client, profile, contract = load_runtime(symbol)
    else:
        client, profile, contract = load_runtime(symbol, source_id=source_id)
    try:
        return client.fetch(profile, contract)
    finally:
        client.credentials.close()
        client.state.close()


def record_storage_outcome(batch, *, succeeded, now=None):
    """Record database outcome separately from fetching, after its commit point.

    The optional clock value supports deterministic integration tests. Production
    uses the actual event time, never backdates a commit to the observation time.
    Legacy/in-memory test batches without a durable state path have no journal.
    """
    path = getattr(batch, "state_path", None)
    if path is None:
        return
    if type(succeeded) is not bool:
        raise ValueError("Invalid storage outcome")
    state = StateStore(path, existing_only=True)
    try:
        when = time.time() if now is None else now
        if succeeded:
            state.record_storage_success(batch.source_id, batch.request_id, when)
        else:
            state.record_storage_failure(batch.source_id, batch.request_id, when)
    finally:
        state.close()
