"""Local operator CLI: inspected state, deliberate revocation, no network calls."""

import argparse
import json
import math
from pathlib import Path
import re
import time

from api_contracts import ApiFailure
from api_runtime import health_report
from api_state import StateRejected, StateStore


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        # argparse's normal errors echo unknown arguments, which may be secrets.
        raise ApiFailure("invalid_arguments")


def _identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", value):
        raise ApiFailure("invalid_identifier")
    return value


def _positive(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ApiFailure("invalid_health_policy") from None
    if not math.isfinite(number) or number <= 0:
        raise ApiFailure("invalid_health_policy")
    return number


def main(argv=None, *, wall=time.time):
    state = None
    try:
        parser = _Parser(description="Inspect local API health or revoke a credential version.")
        parser.add_argument("--state", required=True)
        commands = parser.add_subparsers(dest="command", required=True)
        health = commands.add_parser("health")
        health.add_argument("--source", required=True)
        health.add_argument("--max-silence", required=True)
        health.add_argument("--max-data-age", required=True)
        revoke = commands.add_parser("revoke")
        revoke.add_argument("--source", required=True)
        revoke.add_argument("--version", required=True)
        commands.add_parser("inventory")
        args = parser.parse_args(argv)
        path = Path(args.state)
        if not path.is_absolute() or not path.is_file():
            raise ApiFailure("existing_state_required")
        # SQLite mode=rw closes the existence-check race without creating files.
        state = StateStore(path, existing_only=True)
        if args.command == "inventory":
            result, code = {"sources": state.inventory()}, 0
        elif args.command == "revoke":
            source, version = _identifier(args.source), _identifier(args.version)
            if not state.known_credential(source, version):
                raise ApiFailure("unknown_credential")
            state.revoke_credential(source, version, wall())
            result, code = {"source_id": source, "version": version, "status": "revoked"}, 0
        else:
            source = _identifier(args.source)
            if not any(item['source_id'] == source for item in state.inventory()):
                raise ApiFailure("unknown_source")
            now, silence = wall(), _positive(args.max_silence)
            result = health_report(state, source, now=now,
                                   max_silence_seconds=silence,
                                   max_data_age_seconds=_positive(args.max_data_age))
            health = result.get("health") or {}
            status = health.get("status")
            stored = health.get('last_storage_success')
            storage_stale = stored is None or now < stored or now - stored > silence
            storage_healthy = (not storage_stale and health.get('storage_status') == 'stored'
                               and health.get('storage_request_id') == health.get('last_success_request_id'))
            healthy = not result['stale'] and status == 'healthy' and storage_healthy
            result['healthy'] = healthy
            result['storage_healthy'] = storage_healthy
            result['storage_stale'] = storage_stale
            code = 0 if healthy else 1
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return code
    except (ApiFailure, StateRejected) as error:
        print(json.dumps({"error": ApiFailure(error.code).code}))
        return 1
    except Exception:
        print(json.dumps({"error": "administration_failed"}))
        return 1
    finally:
        if state is not None:
            state.close()


if __name__ == "__main__":
    raise SystemExit(main())
