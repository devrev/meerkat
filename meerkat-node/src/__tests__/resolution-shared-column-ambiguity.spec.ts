import { Query, ResolutionConfig, TableSchema } from '@devrev/meerkat-core';
import { cubeQueryToSQLWithResolution } from '../cube-to-sql-with-resolution/cube-to-sql-with-resolution';
import { duckdbExec } from '../duckdb-exec';

/**
 * Regression test for ISS-301213.
 *
 * CSV export from a dashboard widget fails with
 *   `Binder Error: Ambiguous reference to column name "<col>"`
 * when the widget's SQL resolves two (or more) scalar lookup columns whose
 * source lookup tables share a column name beyond the one being resolved.
 *
 * Real-world shape (TKT-65015, Razorpay Incident dashboard): a base query
 * left-joins lookup tables `dim_devu` (owner) and `dim_group`, both of which
 * carry an unrelated column (`deprecated_created_by_agent_id` in the HAR). Each
 * lookup is joined as `SELECT <resolved col>, * FROM (<lookup sql>)`. The bare
 * `*` re-exposes every RAW column of the lookup, so the shared name appears
 * twice in the intermediate join relation and DuckDB's binder rejects the
 * export.
 *
 * Fix (qualify approach): each lookup table's SQL is rewritten to project its
 * declared columns (plus the join column) under table-qualified aliases
 * (`<config>__<column>`). Those aliases are globally unique per resolution
 * config, so the bare `*` over the join can never surface two columns with the
 * same name — eliminating the ambiguity at its source.
 *
 * NOTE ON THE ORACLE: the node `duckdb` binding tolerates intermediate
 * relations that carry duplicate column names (production `duckdb-wasm` does
 * not), so a plain "COPY does not throw" assertion is false-green on this
 * engine. We therefore assert structurally on the generated lookup SQL — it
 * must project only qualified aliases and never a bare `*` that could leak a
 * colliding raw column — and additionally verify the export still produces
 * correct, resolved data.
 */

const CREATE_BASE_TABLE = `CREATE OR REPLACE TABLE iss301213_tickets (
  id INTEGER,
  owner_id VARCHAR,
  group_id VARCHAR
)`;

const BASE_DATA = `INSERT INTO iss301213_tickets VALUES
(1, 'owner1', 'group1'),
(2, 'owner2', 'group2'),
(3, 'owner1', 'group2')`;

// Owner lookup carries an extra `deprecated_created_by_agent_id` column that is
// never resolved — pure passthrough that the bare `*` would otherwise leak.
const CREATE_OWNER_LOOKUP = `CREATE OR REPLACE TABLE iss301213_owner_lookup (
  id VARCHAR,
  fullname VARCHAR,
  deprecated_created_by_agent_id VARCHAR
)`;

const OWNER_LOOKUP_DATA = `INSERT INTO iss301213_owner_lookup VALUES
('owner1', 'Alice', 'copy-o1'),
('owner2', 'Bob', 'copy-o2')`;

// Group lookup ALSO carries `deprecated_created_by_agent_id` — this is the
// shared column that becomes ambiguous when both lookups are re-exposed via `*`.
const CREATE_GROUP_LOOKUP = `CREATE OR REPLACE TABLE iss301213_group_lookup (
  id VARCHAR,
  name VARCHAR,
  deprecated_created_by_agent_id VARCHAR
)`;

const GROUP_LOOKUP_DATA = `INSERT INTO iss301213_group_lookup VALUES
('group1', 'Group One', 'copy-g1'),
('group2', 'Group Two', 'copy-g2')`;

const BASE_SCHEMA: TableSchema = {
  name: 'iss301213_tickets',
  sql: 'select * from iss301213_tickets',
  measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
  dimensions: [
    { alias: 'ID', name: 'id', sql: 'id', type: 'number' },
    { alias: 'Owner', name: 'owner_id', sql: 'owner_id', type: 'string' },
    { alias: 'Group', name: 'group_id', sql: 'group_id', type: 'string' },
  ],
};

const OWNER_LOOKUP_SCHEMA: TableSchema = {
  name: 'iss301213_owner_lookup',
  sql: 'select * from iss301213_owner_lookup',
  measures: [],
  dimensions: [
    { alias: 'ID', name: 'id', sql: 'id', type: 'string' },
    { alias: 'Full Name', name: 'fullname', sql: 'fullname', type: 'string' },
    {
      alias: 'Deprecated Agent',
      name: 'deprecated_created_by_agent_id',
      sql: 'deprecated_created_by_agent_id',
      type: 'string',
    },
  ],
};

const GROUP_LOOKUP_SCHEMA: TableSchema = {
  name: 'iss301213_group_lookup',
  sql: 'select * from iss301213_group_lookup',
  measures: [],
  dimensions: [
    { alias: 'ID', name: 'id', sql: 'id', type: 'string' },
    { alias: 'Name', name: 'name', sql: 'name', type: 'string' },
    {
      alias: 'Deprecated Agent',
      name: 'deprecated_created_by_agent_id',
      sql: 'deprecated_created_by_agent_id',
      type: 'string',
    },
  ],
};

const QUERY: Query = {
  measures: ['iss301213_tickets.count'],
  dimensions: [
    'iss301213_tickets.id',
    'iss301213_tickets.owner_id',
    'iss301213_tickets.group_id',
  ],
  order: { 'iss301213_tickets.id': 'asc' },
};

const COLUMN_PROJECTIONS = [
  'iss301213_tickets.id',
  'iss301213_tickets.owner_id',
  'iss301213_tickets.group_id',
  'iss301213_tickets.count',
];

// cubeQueryToSQLWithResolution mutates the resolutionConfig it receives
// (it rewrites columnConfig names to safe keys in place), so each test must
// start from a fresh copy.
const buildResolutionConfig = (): ResolutionConfig => ({
  columnConfigs: [
    {
      name: 'iss301213_tickets.owner_id',
      type: 'string' as const,
      source: 'iss301213_owner_lookup',
      joinColumn: 'id',
      resolutionColumns: ['fullname'],
    },
    {
      name: 'iss301213_tickets.group_id',
      type: 'string' as const,
      source: 'iss301213_group_lookup',
      joinColumn: 'id',
      resolutionColumns: ['name'],
    },
  ],
  tableSchemas: [OWNER_LOOKUP_SCHEMA, GROUP_LOOKUP_SCHEMA],
});

describe('ISS-301213 - shared lookup column ambiguity on CSV export', () => {
  beforeAll(async () => {
    await duckdbExec(CREATE_BASE_TABLE);
    await duckdbExec(BASE_DATA);
    await duckdbExec(CREATE_OWNER_LOOKUP);
    await duckdbExec(OWNER_LOOKUP_DATA);
    await duckdbExec(CREATE_GROUP_LOOKUP);
    await duckdbExec(GROUP_LOOKUP_DATA);
  });

  afterAll(async () => {
    await duckdbExec('DROP TABLE IF EXISTS iss301213_tickets');
    await duckdbExec('DROP TABLE IF EXISTS iss301213_owner_lookup');
    await duckdbExec('DROP TABLE IF EXISTS iss301213_group_lookup');
  });

  it('disambiguates the shared lookup column via table-qualified aliases', async () => {
    const sql = await cubeQueryToSQLWithResolution({
      query: QUERY,
      tableSchemas: [BASE_SCHEMA],
      resolutionConfig: buildResolutionConfig(),
      columnProjections: [...COLUMN_PROJECTIONS],
    });

    // The shared lookup column must be projected under a DISTINCT
    // table-qualified alias per lookup, so the bare `*` over the join can never
    // surface two identically-named columns (the ISS-301213 ambiguity). On the
    // buggy code path the column leaked anonymously through `SELECT * FROM
    // (select * from <lookup>)`, so these qualified aliases were absent.
    expect(sql).toContain(
      'iss301213_tickets__owner_id__deprecated_created_by_agent_id'
    );
    expect(sql).toContain(
      'iss301213_tickets__group_id__deprecated_created_by_agent_id'
    );

    // Each lookup must be joined through its qualified join-column alias rather
    // than a bare `.id`, confirming the lookup SQL was narrowed + qualified.
    expect(sql).toContain(
      '"iss301213_tickets__owner_id__id"'
    );
    expect(sql).toContain(
      '"iss301213_tickets__group_id__id"'
    );

    // The lookup subqueries no longer pass columns through by bare physical
    // name: the shared column only ever appears qualified, never as a standalone
    // unqualified output identifier.
    const bareLeak = /(?:SELECT|,)\s+deprecated_created_by_agent_id\b/;
    expect(bareLeak.test(sql)).toBe(false);
  });

  it('COPYs to CSV preserving resolved values + row count', async () => {
    const sql = await cubeQueryToSQLWithResolution({
      query: QUERY,
      tableSchemas: [BASE_SCHEMA],
      resolutionConfig: buildResolutionConfig(),
      columnProjections: [...COLUMN_PROJECTIONS],
    });

    const csvPath = '/tmp/test_iss301213_shared_column.csv';
    await expect(
      duckdbExec(`COPY (${sql}) TO '${csvPath}' (HEADER, DELIMITER ',')`)
    ).resolves.not.toThrow();

    const result = (await duckdbExec(
      `SELECT * FROM read_csv_auto('${csvPath}')`
    )) as any[];

    expect(result.length).toBe(3);
    expect(Number(result[0].ID)).toBe(1);
    // Resolved scalar values land under the original aliases.
    expect(result[0]['Owner']).toBe('Alice');
    expect(result[0]['Group']).toBe('Group One');
  });
});
