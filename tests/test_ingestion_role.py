"""Real labbet_ingestion sessions; only the dedicated role verifier may run this.

The database name labbet is required by migration 003. Isolation additionally
requires an explicit private socket and matching server run marker. The existing
storage suite retains its own labbet_api_test safeguard without modification.
"""
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
import os
from pathlib import Path
import sys
import unittest
from uuid import uuid4

import psycopg

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'ingestion'), str(ROOT / 'tests')]
from api_storage import persist_batch
import test_api_storage as storage_tests

OBSERVED = {'denials': [], 'migration_errors': [], 'identities': []}
TABLES = ('ingestion_runs', 'price_revisions', 'current_prices')


def connect(user='labbet_ingestion'):
    marker = os.environ.get('LABBET_ROLE_TEST_RUN')
    if not marker or len(marker) != 32 or os.environ.get('LABBET_ROLE_TEST_SOCKET') != '/pgsocket':
        raise RuntimeError('Explicit isolated role-test marker/socket required')
    connection = psycopg.connect(host='/pgsocket', dbname='labbet', user=user,
                                 autocommit=True, connect_timeout=5,
                                 options='-c statement_timeout=10000 -c lock_timeout=5000')
    try:
        identity = connection.execute("""SELECT current_database(), session_user, current_user,
          current_setting('labbet.test_run_id'), inet_server_addr() IS NULL,
          current_setting('server_version_num')""").fetchone()
        if identity != ('labbet', user, user, marker, True, '180006'):
            raise RuntimeError('Unexpected PostgreSQL identity/isolation marker')
        OBSERVED['identities'].append(list(identity))
        return connection
    except BaseException:
        connection.close()
        raise


def acl_snapshot(connection):
    return connection.execute("""SELECT jsonb_build_object(
      'role', (SELECT to_jsonb(r) FROM (
        SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin,
               rolreplication, rolbypassrls FROM pg_roles WHERE rolname='labbet_ingestion') r),
      'memberships', (SELECT jsonb_agg(to_jsonb(m)) FROM (
        SELECT roleid, member, grantor, admin_option, inherit_option, set_option
        FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname='labbet_ingestion')) m),
      'database_acl', (SELECT datacl::text FROM pg_database WHERE datname='labbet'),
      'schema_acl', (SELECT jsonb_agg(jsonb_build_array(nspname,nspacl::text) ORDER BY nspname)
        FROM pg_namespace WHERE nspname IN ('public','labbet_api')),
      'table_acl', (SELECT jsonb_agg(jsonb_build_array(n.nspname,c.relname,c.relacl::text) ORDER BY n.nspname,c.relname)
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname IN ('public','labbet_api') AND c.relkind='r'),
      'column_acl', (SELECT jsonb_agg(jsonb_build_array(c.relname,a.attname,a.attacl::text)
        ORDER BY c.relname,a.attnum) FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname IN ('public','labbet_api') AND a.attnum>0 AND NOT a.attisdropped))""").fetchone()[0]


class MigrationAtomicityTests(unittest.TestCase):
    def test_01_failure_after_role_creation_rolls_back_role_and_grants(self):
        with connect('postgres') as admin:
            before = acl_snapshot(admin)
            self.assertIsNone(before['role'])
            admin.execute('ALTER TABLE labbet_api.current_prices RENAME TO unavailable_current_prices')
            try:
                with self.assertRaises(psycopg.errors.UndefinedTable) as caught:
                    admin.execute((ROOT / 'sql/003_ingestion_role.sql').read_text(), prepare=False)
                self.assertEqual(caught.exception.sqlstate, '42P01')
                admin.rollback()  # BEGIN in the unchanged file is now aborted.
            finally:
                admin.rollback()
                admin.execute('ALTER TABLE labbet_api.unavailable_current_prices RENAME TO current_prices')
            self.assertEqual(acl_snapshot(admin), before)
            OBSERVED['migration_errors'].append({'case': 'failure-after-create-role', 'sqlstate': '42P01',
                                                 'role_and_acl_rolled_back': True})

    def test_02_install_and_reapplication_preserve_existing_permissions(self):
        with connect('postgres') as admin:
            admin.execute((ROOT / 'sql/003_ingestion_role.sql').read_text(), prepare=False)
            before = acl_snapshot(admin)
            self.assertTrue(before['role']['rolcanlogin'])
            with self.assertRaises(psycopg.errors.DuplicateObject) as caught:
                admin.execute((ROOT / 'sql/003_ingestion_role.sql').read_text(), prepare=False)
            self.assertEqual(caught.exception.sqlstate, '42710')
            admin.rollback()
            self.assertEqual(acl_snapshot(admin), before)
            OBSERVED['installed_acl'] = before
            OBSERVED['migration_errors'].append({'case': 'repeat-role-migration', 'sqlstate': '42710',
                                                 'existing_acl_unchanged': True})


class IngestionRoleTests(storage_tests.StoragePostgresTests):
    """Reuse all 15 real storage cases, with actual app login on every connection."""
    @classmethod
    def setUpClass(cls):
        cls.psycopg = psycopg
        # One inherited timeout test also opens this explicit connection string.
        cls.dsn = 'host=/pgsocket dbname=labbet user=labbet_ingestion'
        with cls.connect() as connection:
            assert connection.execute('SELECT session_user, current_user').fetchone() == (
                'labbet_ingestion', 'labbet_ingestion')

    @classmethod
    def connect(cls):
        return connect()

    def deny(self, label, query, params=None, connection=None):
        connection = connection or self.connection
        with self.assertRaises(psycopg.errors.InsufficientPrivilege) as caught:
            # Even an unexpectedly permitted mutation must never be committed.
            with connection.transaction(force_rollback=True):
                connection.execute(query, params)
        self.assertEqual(caught.exception.sqlstate, '42501')
        OBSERVED['denials'].append({'case': label, 'sqlstate': caught.exception.sqlstate})

    def test_history_is_immutable_in_database(self):
        # Override the admin regression's 55000 trigger expectation: this role
        # must be rejected earlier by PostgreSQL permission checks (42501).
        result = self.save()
        for table, assignment in (('ingestion_runs', "config_version='changed'"),
                                  ('price_revisions', 'close_price=10')):
            for operation in ('UPDATE', 'DELETE'):
                with self.subTest(table=table, operation=operation):
                    query = (f'UPDATE labbet_api.{table} SET {assignment}' if operation == 'UPDATE'
                             else f'DELETE FROM labbet_api.{table}') + ' WHERE run_id=%s'
                    self.deny(operation + '-' + table, query, (result.run_id,))
        self.assertEqual(self.counts(), (1, 1, 1))

    def test_effective_privileges_role_attributes_memberships_and_owners(self):
        snapshot = acl_snapshot(self.connection)
        self.assertEqual(snapshot['role'], {
            'rolname': 'labbet_ingestion', 'rolcanlogin': True,
            'rolsuper': False, 'rolinherit': False, 'rolcreaterole': False,
            'rolcreatedb': False, 'rolreplication': False, 'rolbypassrls': False})
        self.assertIsNone(snapshot['memberships'])
        owners = self.connection.execute("""SELECT c.relname, pg_get_userbyid(c.relowner)
          FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname='labbet_api' AND c.relkind='r' ORDER BY c.relname""").fetchall()
        self.assertEqual(owners, [(t, 'postgres') for t in sorted(TABLES)])
        self.assertEqual(self.connection.execute("""SELECT pg_get_userbyid(datdba)
          FROM pg_database WHERE datname=current_database()""").fetchone(), ('postgres',))
        self.assertEqual(self.connection.execute("""SELECT count(*) FROM pg_namespace
          WHERE nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)""").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("""SELECT count(*) FROM pg_proc
          WHERE proowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)""").fetchone()[0], 0)
        matrix = []
        for table in TABLES:
            for privilege in ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER', 'MAINTAIN'):
                permitted = self.connection.execute('SELECT has_table_privilege(current_user,%s,%s)',
                    ('labbet_api.' + table, privilege)).fetchone()[0]
                self.assertEqual(permitted, privilege in ('SELECT', 'INSERT'))
                matrix.append([table, privilege, permitted])
            columns = self.connection.execute("""SELECT column_name FROM information_schema.columns
              WHERE table_schema='labbet_api' AND table_name=%s ORDER BY ordinal_position""", (table,)).fetchall()
            for (column,) in columns:
                for privilege in ('SELECT', 'INSERT', 'UPDATE', 'REFERENCES'):
                    permitted = self.connection.execute('SELECT has_column_privilege(current_user,%s,%s,%s)',
                        ('labbet_api.' + table, column, privilege)).fetchone()[0]
                    wanted = privilege in ('SELECT', 'INSERT') or (privilege == 'UPDATE'
                        and table == 'current_prices' and column in ('revision_id', 'observed_at'))
                    self.assertEqual(permitted, wanted)
                    matrix.append([table, column, privilege, permitted])
        OBSERVED['effective_privileges'] = matrix
        OBSERVED['table_owners'] = owners

    def test_complete_stored_values_and_pointers_across_observation_order(self):
        row = self.args['rows'][0]
        other = (row[0], row[1] - timedelta(days=1), Decimal('20.123456'),
                 Decimal('21.654321'), Decimal('19.111111'), Decimal('20.555555'), 1234567)
        self.args['rows'] = [row, other]
        first = self.save()
        self.assertEqual((first.row_count, first.inserted_revisions, first.advanced_current, first.replayed),
                         (2, 2, 2, False))
        run = self.connection.execute("""SELECT source_id,instrument_id,symbol,request_id,
          observed_at,received_at,config_version,payload_hash,contract_metadata,row_count
          FROM labbet_api.ingestion_runs WHERE run_id=%s""", (first.run_id,)).fetchone()
        self.assertEqual(run, tuple(self.args[k] for k in ('source_id','instrument_id','symbol','request_id',
            'observed_at','received_at','config_version','payload_hash','metadata')) + (2,))
        values = self.connection.execute("""SELECT r.trade_date,r.open_price,r.high_price,r.low_price,
          r.close_price,r.volume,r.observed_at,c.observed_at,r.run_id::text,
          c.revision_id=r.revision_id FROM labbet_api.current_prices c
          JOIN labbet_api.price_revisions r USING(revision_id)
          WHERE c.source_id=%s ORDER BY r.trade_date""", (self.args['source_id'],)).fetchall()
        self.assertEqual(values, [tuple(r[1:]) + (self.args['observed_at'], self.args['observed_at'], first.run_id, True)
                                  for r in sorted((row, other), key=lambda r: r[1])])
        newer = deepcopy(self.args)
        newer.update(request_id='full-values-newer', observed_at=self.args['observed_at'] + timedelta(seconds=1))
        newer['rows'] = [(*row[:5], Decimal('10.750001'), 123)]
        newest = persist_batch(self.connection, **newer)
        older = deepcopy(self.args)
        older.update(request_id='full-values-older', observed_at=self.args['observed_at'] - timedelta(seconds=1))
        older['rows'] = [(*row[:5], Decimal('10.250001'), 456)]
        late = persist_batch(self.connection, **older)
        self.assertEqual((newest.advanced_current, late.advanced_current), (1, 0))
        self.assertEqual(self.counts(), (3, 4, 2))
        history = self.connection.execute("""SELECT observed_at,close_price,volume,run_id::text
          FROM labbet_api.price_revisions WHERE source_id=%s AND trade_date=%s ORDER BY observed_at""",
          (self.args['source_id'], row[1])).fetchall()
        self.assertEqual(history, [(older['observed_at'], Decimal('10.250001'),456,late.run_id),
                                  (self.args['observed_at'],row[5],row[6],first.run_id),
                                  (newer['observed_at'],Decimal('10.750001'),123,newest.run_id)])
        pointer = self.connection.execute("""SELECT c.observed_at,r.observed_at,r.run_id::text,r.close_price,r.volume
          FROM labbet_api.current_prices c JOIN labbet_api.price_revisions r USING(revision_id)
          WHERE c.source_id=%s AND c.trade_date=%s""", (self.args['source_id'], row[1])).fetchone()
        self.assertEqual(pointer, (newer['observed_at'],newer['observed_at'],newest.run_id,Decimal('10.750001'),123))
        replay = persist_batch(self.connection, **newer)
        self.assertEqual((replay.run_id,replay.replayed,replay.inserted_revisions,replay.advanced_current),
                         (newest.run_id,True,0,0))
        self.assertEqual(self.counts(), (3, 4, 2))

    def test_protected_current_keys_delete_and_truncate_denied(self):
        self.save()
        for column in ('source_id', 'instrument_id', 'contract_hash', 'trade_date'):
            with self.subTest(column=column):
                self.deny('UPDATE-current-key-' + column,
                          f'UPDATE labbet_api.current_prices SET {column}={column} WHERE source_id=%s',
                          (self.args['source_id'],))
        self.deny('mixed-UPDATE-current-key-and-pointer',
                  'UPDATE labbet_api.current_prices SET revision_id=revision_id, source_id=source_id WHERE source_id=%s',
                  (self.args['source_id'],))
        for table in TABLES:
            with self.subTest(table=table):
                self.deny('DELETE-' + table,
                          f'DELETE FROM labbet_api.{table} WHERE source_id=%s', (self.args['source_id'],))
                self.deny('TRUNCATE-' + table, f'TRUNCATE labbet_api.{table}')
        self.assertEqual(self.counts(), (1, 1, 1))

    def test_legacy_access_denied(self):
        for label, query in (
            ('SELECT', 'SELECT * FROM public.daily_prices LIMIT 1'),
            ('INSERT', "INSERT INTO public.daily_prices VALUES ('ROLE_TEST', DATE '2026-10-17',10,11,9,10,1)"),
            ('UPDATE', 'UPDATE public.daily_prices SET volume=volume'),
            ('DELETE', 'DELETE FROM public.daily_prices'),
            ('TRUNCATE', 'TRUNCATE public.daily_prices'),
        ):
            with self.subTest(operation=label):
                self.deny(label + '-legacy', query)

    def test_ddl_in_protected_schemas_denied(self):
        for schema in ('labbet_api', 'public'):
            for label, query in (
                ('CREATE-TABLE', f'CREATE TABLE {schema}.forbidden_table (x integer)'),
                ('CREATE-FUNCTION', f'CREATE FUNCTION {schema}.forbidden_function() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$'),
                ('ALTER-SCHEMA', f'ALTER SCHEMA {schema} OWNER TO labbet_ingestion'),
                ('DROP-SCHEMA', f'DROP SCHEMA {schema} CASCADE'),
            ):
                with self.subTest(schema=schema, operation=label):
                    self.deny(label + '-' + schema, query)
        for table in ('public.daily_prices',) + tuple('labbet_api.' + t for t in TABLES):
            for label, query in (
                ('ALTER-TABLE', f'ALTER TABLE {table} ADD COLUMN forbidden integer'),
                ('DROP-TABLE', f'DROP TABLE {table} CASCADE'),
            ):
                with self.subTest(table=table, operation=label):
                    self.deny(label + '-' + table, query)
        self.deny('ALTER-history-trigger', 'ALTER TABLE labbet_api.ingestion_runs DISABLE TRIGGER ingestion_runs_immutable')
        self.deny('REPLACE-history-function', """CREATE OR REPLACE FUNCTION labbet_api.reject_history_mutation()
          RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$""")

    def test_role_escalation_and_grant_denied(self):
        for label, query in (
            ('SET-ROLE-postgres', 'SET ROLE postgres'),
            ('ALTER-ROLE-superuser', 'ALTER ROLE labbet_ingestion SUPERUSER'),
            ('CREATE-ROLE', 'CREATE ROLE forbidden_role'),
            ('GRANT-postgres-membership', 'GRANT postgres TO labbet_ingestion'),
            ('GRANT-legacy-table', 'GRANT SELECT ON public.daily_prices TO PUBLIC'),
        ):
            with self.subTest(operation=label):
                self.deny(label, query)
        self.assertEqual(self.connection.execute('SELECT session_user,current_user').fetchone(),
                         ('labbet_ingestion', 'labbet_ingestion'))

    def test_public_connect_temp_and_public_schema_usage_are_explicitly_allowed(self):
        privileges = self.connection.execute("""SELECT
          has_database_privilege(current_user,'labbet','CONNECT'),
          has_database_privilege(current_user,'labbet','TEMP'),
          has_schema_privilege(current_user,'public','USAGE'),
          has_schema_privilege(current_user,'public','CREATE'),
          has_schema_privilege(current_user,'labbet_api','USAGE'),
          has_schema_privilege(current_user,'labbet_api','CREATE')""").fetchone()
        self.assertEqual(privileges, (True, True, True, False, True, False))
        public = self.connection.execute("""SELECT privilege_type FROM pg_database d,
          LATERAL aclexplode(COALESCE(d.datacl,acldefault('d',d.datdba))) a
          WHERE d.datname='labbet' AND a.grantee=0 ORDER BY privilege_type""").fetchall()
        self.assertEqual(public, [('CONNECT',), ('TEMPORARY',)])
        with self.connection.transaction(force_rollback=True):
            self.connection.execute('CREATE TEMP TABLE allowed_temp (value integer)')
            self.connection.execute('INSERT INTO allowed_temp VALUES (7)')
            self.assertEqual(self.connection.execute('SELECT value FROM allowed_temp').fetchone(), (7,))
        self.assertIsNone(self.connection.execute("SELECT to_regclass('pg_temp.allowed_temp')").fetchone()[0])
        OBSERVED['public_privileges'] = {'database': public, 'effective_connect_temp_usage_create': privileges}

    def test_future_tables_receive_no_automatic_permissions(self):
        name = 'future_' + uuid4().hex
        with connect('postgres') as admin:
            for schema in ('labbet_api', 'public'):
                admin.execute(f'CREATE TABLE {schema}.{name} (value integer)')
                for privilege in ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER', 'MAINTAIN'):
                    with self.subTest(schema=schema, privilege=privilege):
                        self.assertFalse(self.connection.execute('SELECT has_table_privilege(current_user,%s,%s)',
                            (schema + '.' + name, privilege)).fetchone()[0])
                self.deny('SELECT-future-' + schema, f'SELECT * FROM {schema}.{name}')
                self.deny('INSERT-future-' + schema, f'INSERT INTO {schema}.{name} VALUES (1)')

    def test_failure_after_current_pointer_update_rolls_back_every_table(self):
        original = self.save()
        before = self.current()
        actual = self.connection
        args = deepcopy(self.args)
        args['request_id'] = 'fault-after-pointer'
        args['observed_at'] += timedelta(seconds=1)
        seen = []
        class CursorProxy:
            def __enter__(self):
                self.cursor = actual.cursor().__enter__()
                return self
            def __exit__(self, *exc):
                return self.cursor.__exit__(*exc)
            def execute(self, query, params=None):
                result = self.cursor.execute(query, params)
                if 'INSERT INTO labbet_api.current_prices' in query:
                    seen.append(self.cursor.rowcount)
                    raise RuntimeError('injected after successful pointer update')
                return result
            def __getattr__(self, name):
                return getattr(self.cursor, name)
        class ConnectionProxy:
            def transaction(self):
                return actual.transaction()
            def cursor(self):
                return CursorProxy()
        with self.assertRaisesRegex(RuntimeError, 'after successful pointer update'):
            persist_batch(ConnectionProxy(), **args)
        self.assertEqual(seen, [1])
        self.assertEqual(self.counts(), (1, 1, 1))
        self.assertEqual(self.current(), before)
        replay = self.save()
        self.assertTrue(replay.replayed)
        self.assertEqual(replay.run_id, original.run_id)

    def test_column_rights_alone_do_not_enforce_newer_observation_order(self):
        self.save()
        newer = self.args['observed_at'] + timedelta(seconds=1)
        self.save(request_id='newer', observed_at=newer)
        old = self.connection.execute("""SELECT revision_id,observed_at FROM labbet_api.price_revisions
          WHERE source_id=%s ORDER BY observed_at LIMIT 1""", (self.args['source_id'],)).fetchone()
        with self.connection.transaction(force_rollback=True):
            self.connection.execute("""UPDATE labbet_api.current_prices SET revision_id=%s, observed_at=%s
              WHERE source_id=%s""", (*old, self.args['source_id']))
            self.assertEqual(self.current()[0][1], old[1])
        self.assertEqual(self.current()[0][1], newer)
        OBSERVED['manual_backward_pointer_update_possible'] = True
