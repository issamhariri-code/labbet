"""The process boundary exposes controlled JSON and exit codes, never raw errors."""

from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch


ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT / "ingestion" / "main.py"
sys.path.insert(0, str(MAIN.parent))
from api_contracts import ApiFailure


class CliBoundaryTests(unittest.TestCase):
    def setUp(self):
        # Legacy CLI cases must not inherit a host-selected source.
        environment = patch.dict(os.environ, {}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)
        # Loading the real entrypoint must not create a real client or database.
        modules = {}
        for name, attribute in (("market_data", "fetch_daily_prices"),
                                ("api_storage", "persist_batch"),
                                ("api_validation", "prepare_rows"),
                                ("psycopg", "connect")):
            module = ModuleType(name)
            setattr(module, attribute, Mock())
            modules[name] = module
        modules["market_data"].record_storage_outcome = Mock()
        self.main = ModuleType("labbet_cli_test")
        self.main.__file__ = str(MAIN)
        with patch.dict(sys.modules, modules):
            exec(compile(MAIN.read_text(encoding="utf-8"), str(MAIN), "exec"), self.main.__dict__)
        self.main.symbol = "TEST"

    def invoke(self, runner):
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.object(self.main, "run", runner), redirect_stdout(stdout), redirect_stderr(stderr):
            status = self.main.cli()
        return status, stdout.getvalue(), stderr.getvalue()

    def assert_secret_free(self, stdout, stderr):
        for secret in ("synthetic-secret", "postgresql://", "apikey=", "Traceback"):
            self.assertNotIn(secret, stdout + stderr)

    def test_success_has_exit_zero_and_preserves_success_output(self):
        runner = Mock(side_effect=lambda symbol: print(json.dumps({"status": "stored", "rows": 1})))
        status, stdout, stderr = self.invoke(runner)
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(stdout), {"status": "stored", "rows": 1})
        self.assertEqual(stderr, "")
        runner.assert_called_once_with("TEST")

    def test_api_failure_prints_only_controlled_code_and_status(self):
        def fail(symbol):
            try:
                raise RuntimeError("https://provider.invalid?apikey=synthetic-secret")
            except RuntimeError as original:
                raise ApiFailure("authentication", status=401) from original
        status, stdout, stderr = self.invoke(Mock(side_effect=fail))
        self.assertEqual(status, 1)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr), {"status": "error", "code": "authentication", "http_status": 401})
        self.assert_secret_free(stdout, stderr)

    def test_unexpected_exceptions_never_print_dsn_url_or_message(self):
        for error in (RuntimeError("postgresql://user:synthetic-secret@host/db"),
                      ValueError("apikey=synthetic-secret"), OSError("synthetic-secret")):
            with self.subTest(error_type=type(error).__name__):
                status, stdout, stderr = self.invoke(Mock(side_effect=error))
                self.assertEqual(status, 1)
                self.assertEqual(json.loads(stderr), {"status": "error", "code": "ingestion_failed"})
                self.assert_secret_free(stdout, stderr)

    def test_cancellation_returns_130_without_traceback(self):
        status, stdout, stderr = self.invoke(Mock(side_effect=KeyboardInterrupt("synthetic-secret")))
        self.assertEqual(status, 130)
        self.assertEqual(json.loads(stderr), {"status": "cancelled", "code": "cancelled"})
        self.assert_secret_free(stdout, stderr)

    def test_empty_symbol_stops_before_run_with_actionable_code(self):
        self.main.symbol = "  "
        runner = Mock()
        status, stdout, stderr = self.invoke(runner)
        self.assertEqual(status, 1)
        self.assertEqual(json.loads(stderr), {"status": "error", "code": "fill_symbol"})
        runner.assert_not_called()
        self.assert_secret_free(stdout, stderr)

    def test_malformed_error_fields_are_not_exposed(self):
        error = ApiFailure("authentication", status=401)
        error.code = "apikey=synthetic-secret"
        error.status = "synthetic-secret"
        status, stdout, stderr = self.invoke(Mock(side_effect=error))
        self.assertEqual(status, 1)
        self.assertEqual(json.loads(stderr), {"status": "error", "code": "unknown"})
        self.assert_secret_free(stdout, stderr)

    def test_health_write_failure_after_commit_reports_failure_without_false_success(self):
        batch = SimpleNamespace(prices={}, storage_context={}, source_id="source", request_id="request",
                                state_path="/synthetic/state.sqlite")
        connection = MagicMock()
        self.main.fetch_daily_prices = Mock(return_value=batch)
        self.main.prepare_rows = Mock(return_value=[])
        self.main.psycopg.connect = Mock(return_value=connection)
        self.main.persist_batch = Mock(return_value=SimpleNamespace(replayed=False))
        module = ModuleType("market_data")
        def failed_health(*args, **kwargs):
            connection.__exit__.assert_called_once()
            self.assertEqual(kwargs, {"succeeded": True})
            raise OSError("synthetic-secret")
        module.record_storage_outcome = Mock(side_effect=failed_health)
        with patch.dict(sys.modules, {"market_data": module}):
            status, stdout, stderr = self.invoke(Mock(wraps=self.main.run))
        self.assertEqual(status, 1)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr)["code"], "ingestion_failed")
        self.main.persist_batch.assert_called_once()
        self.assert_secret_free(stdout, stderr)

    def test_failure_reporting_cannot_replace_original_database_exception(self):
        batch = SimpleNamespace(prices={}, storage_context={}, source_id="source", request_id="request",
                                state_path="/synthetic/state.sqlite")
        original = OSError("postgresql://user:synthetic-secret@host/db")
        self.main.fetch_daily_prices = Mock(return_value=batch)
        self.main.prepare_rows = Mock(return_value=[])
        self.main.psycopg.connect = Mock(side_effect=original)
        module = ModuleType("market_data")
        module.record_storage_outcome = Mock(side_effect=RuntimeError("another synthetic-secret"))
        with patch.dict(sys.modules, {"market_data": module}):
            with self.assertRaises(OSError) as caught:
                self.main.run("TEST")
        self.assertIs(caught.exception, original)
        module.record_storage_outcome.assert_called_once_with(batch, succeeded=False)


if __name__ == "__main__":
    unittest.main()
