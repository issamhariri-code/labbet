"""Request evidence from fetch to committed archive; synthetic data only."""
from contextlib import closing, redirect_stdout
from dataclasses import FrozenInstanceError, replace
from datetime import date, timedelta
import importlib.util
import io
from pathlib import Path
import sqlite3
from types import MappingProxyType, SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch

import api_adapters
import api_storage
from api_adapters import daily_adapter_spec
from api_client import ApiClient, FetchPolicy
from api_contracts import ApiFailure, SourceAvailability
from api_provenance import RequestProvenance
from api_state import StateStore
from api_storage import read_request_provenance
from api_transport import TransportError
import test_api_pipeline as pipeline_fixtures
import test_availability_integration as availability_fixtures
from test_api_client import NOW, response


ROOT = Path(__file__).resolve().parents[1]
RULE = 'reviewed-session-bounds-v1'


def load_main(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'ingestion' / 'main.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RequestProvenanceBoundaryTests(unittest.TestCase):
    setUp = availability_fixtures.AvailabilityIntegrationTests.setUp
    client = availability_fixtures.AvailabilityIntegrationTests.client
    backup = availability_fixtures.AvailabilityIntegrationTests.backup
    reject = availability_fixtures.AvailabilityIntegrationTests.reject

    def test_enabled_batch_attaches_immutable_complete_secret_free_snapshot(self):
        batch = self.client(self.a).fetch(self.p, self.c)
        self.assertIs(type(batch.provenance), RequestProvenance)
        self.assertEqual(batch.provenance.contract, self.c)
        self.assertEqual(batch.provenance.availability, self.a)
        self.assertEqual(batch.provenance.request, batch.request_context)
        self.assertEqual(batch.provenance.profile_config_hash, self.p.config_hash)
        self.assertEqual(batch.provenance.config_version, batch.config_version)
        captured = batch.storage_context['provenance']
        self.assertEqual(captured, batch.provenance)
        self.assertIsNot(captured, batch.provenance)
        self.assertIsNot(captured.contract, batch.provenance.contract)
        for text in (repr(batch), batch.provenance.manifest_document, batch.provenance.request_document):
            self.assertNotIn('SYNTHETIC-AVAILABILITY', text)
            self.assertNotIn('apikey=', text)
        with self.assertRaises(FrozenInstanceError):
            batch.provenance.profile_config_hash = 'f' * 64

    def test_legacy_storage_context_keeps_exact_original_keys(self):
        legacy = replace(self.p, availability_rule_version=None)
        batch = self.client().fetch(legacy, self.c)
        self.assertIsNone(batch.provenance)
        self.assertEqual(set(batch.storage_context), {'source_id', 'instrument_id', 'symbol',
            'request_id', 'observed_at', 'received_at', 'config_version', 'payload_hash', 'metadata'})

    def test_missing_malformed_and_mismatched_provenance_stop_before_database_connect(self):
        batch = replace(self.client(self.a).fetch(self.p, self.c), state_path=None)
        variants = (replace(batch, provenance=None), replace(batch, provenance={}),
            replace(batch, request_context=None),
            replace(batch, request_context=replace(batch.request_context,
                reference_time=NOW - timedelta(microseconds=1))),
            replace(batch, config_version='f' * 64), replace(batch, source_id='wrong-source'),
            replace(batch, provenance=replace(batch.provenance, profile_config_hash='f' * 64)))
        main = load_main('labbet_provenance_boundary_main')
        for invalid in variants:
            with (self.subTest(source=invalid.source_id),
                  patch.object(main, 'fetch_daily_prices', return_value=invalid),
                  patch.object(main.psycopg, 'connect') as connect):
                with self.assertRaises(ApiFailure):
                    main.run('TEST')
                connect.assert_not_called()

    def test_valid_snapshot_reaches_persist_unchanged_after_full_prevalidation(self):
        batch = replace(self.client(self.a).fetch(self.p, self.c), state_path=None)
        main = load_main('labbet_provenance_handoff_main')
        context = MagicMock()
        connection = context.__enter__.return_value
        with (patch.object(main, 'fetch_daily_prices', return_value=batch),
              patch.object(main.psycopg, 'connect', return_value=context),
              patch.object(main, 'persist_batch', return_value=SimpleNamespace(replayed=False)) as persist,
              redirect_stdout(io.StringIO())):
            main.run('TEST')
        self.assertIs(persist.call_args.args[0], connection)
        captured = persist.call_args.kwargs['provenance']
        self.assertEqual(captured.manifest_document, batch.provenance.manifest_document)
        self.assertEqual(captured.request_document, batch.provenance.request_document)
        self.assertIsNot(captured, batch.provenance)
        self.assertEqual(persist.call_args.kwargs['rows'][0][1], date(2026, 9, 30))
        context.__exit__.assert_called_once_with(None, None, None)

    def test_snapshot_serialization_cost_is_inside_fetch_deadline_before_success(self):
        client = self.client(self.a)
        def slow_snapshot(*args, **kwargs):
            result = RequestProvenance(*args, **kwargs)
            self.clock.elapsed += 31
            return result
        with (patch('api_client.RequestProvenance', side_effect=slow_snapshot),
              patch.object(self.state, 'success', wraps=self.state.success) as success):
            self.reject('deadline', lambda: client.fetch(self.p, self.c))
            success.assert_not_called()
        self.assertIsNone(self.state.health(self.p.source_id)['last_success'])
        self.assertEqual(self.state.health(self.p.source_id)['active_jobs'], 0)

    def test_snapshot_rejection_is_not_recorded_as_fetch_success(self):
        client = self.client(self.a)
        with (patch('api_client.RequestProvenance', side_effect=ApiFailure('invalid_provenance')),
              patch.object(self.state, 'success', wraps=self.state.success) as success):
            self.reject('invalid_provenance', lambda: client.fetch(self.p, self.c))
            success.assert_not_called()
        self.assertIsNone(self.state.health(self.p.source_id)['last_success'])
        with closing(sqlite3.connect(self.state.path)) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0], 1)

    def test_cancelled_snapshot_construction_releases_job_without_success(self):
        client = self.client(self.a)
        with (patch('api_client.RequestProvenance', side_effect=KeyboardInterrupt),
              patch.object(self.state, 'success', wraps=self.state.success) as success):
            with self.assertRaises(KeyboardInterrupt):
                client.fetch(self.p, self.c)
            success.assert_not_called()
        with closing(sqlite3.connect(self.state.path)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM jobs WHERE status='running'").fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0], 1)

    def test_fallback_snapshot_uses_backup_rule_profile_and_original_request(self):
        backup, other = self.backup()
        custom_rule = 'backup-reviewed-v2'
        specs = dict(api_adapters._DAILY_ADAPTERS)
        specs[backup.adapter_id] = replace(daily_adapter_spec(backup.adapter_id),
            availability_rules=frozenset({RULE, custom_rule}))
        with patch.object(api_adapters, '_DAILY_ADAPTERS', MappingProxyType(specs)):
            backup = replace(backup, availability_rule_version=custom_rule)
            other = replace(other, rule_version=custom_rule)
            client = self.client(self.a, other, policy=FetchPolicy(max_attempts=1))
            def transport(*args, **kwargs):
                if self.transport.call_count == 1:
                    self.clock.elapsed += 2
                    raise TransportError('network')
                return response()
            self.transport.side_effect = transport
            batch = client.fetch_with_fallback(self.p, backup, self.c,
                approved_sources=((self.p.source_id, backup.source_id),))
            self.assertEqual(batch.provenance.availability, other)
            self.assertEqual(batch.provenance.availability.rule_version, custom_rule)
            self.assertEqual(batch.provenance.profile_config_hash, backup.config_hash)
            self.assertEqual(batch.provenance.request.reference_time, NOW)
            self.assertEqual(batch.provenance.request.sessions, (date(2026, 9, 30),))
            self.assertEqual(batch.received_at, NOW + timedelta(seconds=2))
            captured = batch.storage_context['provenance']
            self.assertEqual(captured, batch.provenance)
            self.assertIsNot(captured, batch.provenance)

    def test_mutated_archival_contract_cannot_pass_preconnect_with_recomputed_config(self):
        batch = replace(self.client(self.a).fetch(self.p, self.c), state_path=None)
        batch.provenance.contract.__dict__['calendar_version'] = 'mutated-capture-v2'
        batch = replace(batch, config_version=batch.provenance.config_version)
        before = batch.provenance.manifest_document, batch.provenance.request_document
        main = load_main('labbet_provenance_mutation_main')
        with (patch.object(main, 'fetch_daily_prices', return_value=batch),
              patch.object(main.psycopg, 'connect') as connect):
            with self.assertRaises(ApiFailure):
                main.run('TEST')
            connect.assert_not_called()
        self.assertEqual((batch.provenance.manifest_document, batch.provenance.request_document), before)
        self.assertEqual(self.c.calendar_version, 'fixture-calendar-v1')


class RequestProvenancePipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pipeline_fixtures.ComposedPipelineTests.setUpClass.__func__(cls)
        with cls.real_connect(cls.dsn, autocommit=True, connect_timeout=5) as connection:
            connection.execute((ROOT / 'sql' / '004_request_provenance.sql').read_text(encoding='utf-8'))

    setUp = pipeline_fixtures.ComposedPipelineTests.setUp
    tearDown = pipeline_fixtures.ComposedPipelineTests.tearDown
    legacy_snapshot = pipeline_fixtures.ComposedPipelineTests.legacy_snapshot
    server = pipeline_fixtures.ComposedPipelineTests.server
    route_connection = pipeline_fixtures.ComposedPipelineTests.route_connection
    run_bundle = pipeline_fixtures.ComposedPipelineTests.run_bundle
    invoke = pipeline_fixtures.ComposedPipelineTests.invoke
    stored = pipeline_fixtures.ComposedPipelineTests.stored
    assert_output_safe = pipeline_fixtures.ComposedPipelineTests.assert_output_safe

    def runtime(self, server, suffix, *, enabled=True):
        old, p, c = pipeline_fixtures.ComposedPipelineTests.runtime(self, server, suffix)
        if not enabled:
            return old, p, c
        p = replace(p, availability_rule_version=RULE)
        a = availability_fixtures.schedule(p, c)
        client = ApiClient(old.state, old.credentials, policy=old.policy, wall=old.wall,
                          transport_policy=old.transport_policy, availabilities=(a,))
        return client, p, c

    def capture_fetch(self, client, values):
        original = client.fetch
        def fetch(*args, **kwargs):
            batch = original(*args, **kwargs)
            values.append(batch)
            return batch
        return patch.object(client, 'fetch', side_effect=fetch)

    def audit(self, batch):
        with self.real_connect(self.dsn, connect_timeout=5) as connection:
            return read_request_provenance(connection, source_id=batch.source_id, request_id=batch.request_id)

    def health(self, source):
        with closing(StateStore(self.state_path, existing_only=True)) as state:
            return state.health(source)

    def assert_absent(self, batch):
        with self.real_connect(self.dsn, autocommit=True, connect_timeout=5) as connection:
            self.assertEqual(connection.execute(
                'SELECT count(*) FROM labbet_api.ingestion_runs WHERE source_id=%s',
                (batch.source_id,)).fetchone()[0], 0)
            self.assertEqual(connection.execute(
                'SELECT count(*) FROM labbet_api.availability_manifests WHERE manifest_hash=%s',
                (batch.provenance.manifest_hash,)).fetchone()[0], 0)
            self.assertEqual(connection.execute(
                'SELECT count(*) FROM labbet_api.price_revisions WHERE source_id=%s',
                (batch.source_id,)).fetchone()[0], 0)

    def test_real_tls_fetch_main_postgres_and_archive_reproduce_exact_snapshot(self):
        server = self.server()
        runtime = self.runtime(server, 'archive')
        batches = []
        with self.capture_fetch(runtime[0], batches):
            results, connect = self.invoke([runtime])
        self.assertEqual(results, [None])
        self.assertEqual(connect.call_count, 1)
        batch = batches[0]
        decoded = self.audit(batch)
        self.assertEqual(decoded.manifest_document, batch.provenance.manifest_document)
        self.assertEqual(decoded.request_document, batch.provenance.request_document)
        self.assertEqual(decoded.config_version, batch.config_version)
        self.assertEqual(decoded.request.sessions, (date(2026, 9, 29), date(2026, 9, 30)))
        self.assertEqual(len(self.stored()), 2)
        health = self.health(batch.source_id)
        self.assertEqual(health['storage_status'], 'stored')
        self.assertEqual(health['storage_request_id'], batch.request_id)
        self.assertEqual(health['last_success_request_id'], batch.request_id)
        self.assert_output_safe()

    def test_database_rejection_leaves_no_archive_and_marks_storage_failed(self):
        runtime = self.runtime(self.server(), 'readonly')
        batches = []
        self.read_only_database = True
        with self.capture_fetch(runtime[0], batches):
            results, connect = self.invoke([runtime])
        self.assertEqual(getattr(results[0], 'sqlstate', None), '25006')
        self.assertEqual(connect.call_count, 1)
        self.assert_absent(batches[0])
        self.assertEqual(self.stored(), [])
        health = self.health(batches[0].source_id)
        self.assertEqual(health['storage_status'], 'failed')
        self.assertIsNone(health['last_storage_success'])
        self.assert_output_safe()

    def test_two_concurrent_tls_sources_commit_separate_request_evidence(self):
        first = self.runtime(self.server(pipeline_fixtures.BODY_A), 'one')
        second = self.runtime(self.server(pipeline_fixtures.BODY_B), 'two')
        a, b = [], []
        with self.capture_fetch(first[0], a), self.capture_fetch(second[0], b):
            results, connect = self.invoke([first, second], parallel=True)
        self.assertEqual(results, [None, None])
        self.assertEqual(connect.call_count, 2)
        decoded_a, decoded_b = self.audit(a[0]), self.audit(b[0])
        self.assertEqual(decoded_a, a[0].provenance)
        self.assertEqual(decoded_b, b[0].provenance)
        self.assertNotEqual(decoded_a.manifest_hash, decoded_b.manifest_hash)
        self.assertNotEqual(decoded_a.profile_config_hash, decoded_b.profile_config_hash)
        self.assertEqual(len(self.stored()), 4)
        self.assert_output_safe()

    def test_committed_archive_is_readable_after_current_registry_removal(self):
        runtime = self.runtime(self.server(), 'historical')
        batches = []
        with self.capture_fetch(runtime[0], batches):
            results, _ = self.invoke([runtime])
        self.assertEqual(results, [None])
        with patch.object(api_adapters, '_DAILY_ADAPTERS', MappingProxyType({})):
            decoded = self.audit(batches[0])
        self.assertEqual(decoded.fingerprint, batches[0].provenance.fingerprint)
        self.assertEqual(decoded.request.reference_time, NOW)

    def test_cancellation_after_sql_write_rolls_back_prices_run_and_manifest(self):
        runtime = self.runtime(self.server(), 'cancelled')
        batches = []

        class CancelCursor:
            def __init__(self, cursor):
                self.actual = cursor

            def __enter__(self):
                self.actual.__enter__()
                return self

            def __exit__(self, *args):
                return self.actual.__exit__(*args)

            def __getattr__(self, name):
                return getattr(self.actual, name)

            def execute(self, query, parameters=None):
                result = self.actual.execute(query, parameters)
                if 'INSERT INTO labbet_api.price_revisions' in query:
                    # Run, manifest, request evidence and the first price INSERT
                    # really happened; cancel before their transaction commits.
                    raise KeyboardInterrupt
                return result

        class CancelConnection:
            def __init__(self, connection):
                self.actual = connection

            def transaction(self):
                return self.actual.transaction()

            def cursor(self):
                return CancelCursor(self.actual.cursor())

        def cancelled(connection, **values):
            return api_storage.persist_batch(CancelConnection(connection), **values)
        with (self.capture_fetch(runtime[0], batches),
              patch.object(self.main, 'persist_batch', side_effect=cancelled)):
            with self.assertRaises(KeyboardInterrupt):
                self.invoke([runtime])
        self.assert_absent(batches[0])
        self.assertEqual(self.stored(), [])
        self.assertEqual(self.health(batches[0].source_id)['storage_status'], 'failed')
        self.assertNotIn('"status": "stored"', self.output.getvalue())

    def test_legacy_pipeline_archive_read_returns_none_without_fabricating_evidence(self):
        runtime = self.runtime(self.server(), 'legacy', enabled=False)
        batches = []
        with self.capture_fetch(runtime[0], batches):
            results, _ = self.invoke([runtime])
        self.assertEqual(results, [None])
        self.assertIsNone(self.audit(batches[0]))
        self.assertIsNone(batches[0].provenance)
        self.assertEqual(len(self.stored()), 2)

    def test_cancel_after_persist_savepoint_release_rolls_back_outer_job_commit(self):
        runtime = self.runtime(self.server(), 'outer-cancelled')
        batches = []
        completed = []
        def cancel_before_outer_commit(connection, **values):
            # Real persist_batch has finished all inserts and released its
            # nested savepoint. main still owns the enclosing transaction.
            result = api_storage.persist_batch(connection, **values)
            completed.append(result)
            raise KeyboardInterrupt
        with (self.capture_fetch(runtime[0], batches),
              patch.object(self.main, 'persist_batch', side_effect=cancel_before_outer_commit)):
            with self.assertRaises(KeyboardInterrupt):
                self.invoke([runtime])
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0].row_count, 2)
        self.assertEqual(completed[0].inserted_revisions, 2)
        self.assert_absent(batches[0])
        self.assertEqual(self.stored(), [])
        health = self.health(batches[0].source_id)
        self.assertEqual(health['storage_status'], 'failed')
        self.assertIsNone(health['last_storage_success'])
        self.assertNotIn('"status": "stored"', self.output.getvalue())


if __name__ == '__main__':
    unittest.main()
