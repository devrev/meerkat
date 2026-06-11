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
 * Real-world shape (from the Razorpay HAR on TKT-65015): a `dim_ticket` base
 * query left-joins lookup tables `dim_devu` (owner) and `dim_group`, both of
 * which carry an unrelated column `devrev_copy`. The export wrapper produced a
 * trailing bare `*` that re-expanded BOTH lookup tables' columns into the same
 * scope, so `devrev_copy` became ambiguous and DuckDB's binder rejected the
 * COPY statement.
 *
 * The shared column is NOT projected and NOT resolved — it is pure passthrough
 * dragged up by the `*`. The fix narrows each lookup table's SQL to only SELECT
 * the columns needed for the join (joinColumn + resolutionColumns), so unrelated
 * columns like `devrev_copy` never enter the JOIN scope.
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

// Owner lookup carries an extra `devrev_copy` column that is never resolved.
const CREATE_OWNER_LOOKUP = `CREATE OR REPLACE TABLE iss301213_owner_lookup (
  id VARCHAR,
  fullname VARCHAR,
  devrev_copy VARCHAR
)`;

const OWNER_LOOKUP_DATA = `INSERT INTO iss301213_owner_lookup VALUES
('owner1', 'Alice', 'copy-o1'),
('owner2', 'Bob', 'copy-o2')`;

// Group lookup ALSO carries `devrev_copy` — this is the shared column that
// becomes ambiguous when both lookups are joined and re-exposed via `*`.
const CREATE_GROUP_LOOKUP = `CREATE OR REPLACE TABLE iss301213_group_lookup (
  id VARCHAR,
  name VARCHAR,
  devrev_copy VARCHAR
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
      alias: 'Devrev Copy',
      name: 'devrev_copy',
      sql: 'devrev_copy',
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
      alias: 'Devrev Copy',
      name: 'devrev_copy',
      sql: 'devrev_copy',
      type: 'string',
    },
  ],
};

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

  const query: Query = {
    measures: ['iss301213_tickets.count'],
    dimensions: [
      'iss301213_tickets.id',
      'iss301213_tickets.owner_id',
      'iss301213_tickets.group_id',
    ],
    order: { 'iss301213_tickets.id': 'asc' },
  };

  const resolutionConfig: ResolutionConfig = {
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
  };

  const columnProjections = [
    'iss301213_tickets.id',
    'iss301213_tickets.owner_id',
    'iss301213_tickets.group_id',
    'iss301213_tickets.count',
  ];

  it('generates SQL that COPYs to CSV without a Binder ambiguity error', async () => {
    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [BASE_SCHEMA],
      resolutionConfig,
      columnProjections,
    });

    // The narrowed lookup SQL ensures `devrev_copy` never enters the JOIN scope,
    // so bare `*` over joins is harmless — it only expands to needed columns.

    // The export must still execute and preserve resolved values + row count.
    const csvPath = '/tmp/test_iss301213_shared_column.csv';
    await expect(
      duckdbExec(`COPY (${sql}) TO '${csvPath}' (HEADER, DELIMITER ',')`)
    ).resolves.not.toThrow();

    const result = (await duckdbExec(
      `SELECT * FROM read_csv_auto('${csvPath}')`
    )) as any[];

    expect(result.length).toBe(3);
    expect(Number(result[0].ID)).toBe(1);
    expect(result[0]).toHaveProperty('Owner');
    expect(result[0]).toHaveProperty('Group');
    // Resolved scalar values land under the original aliases.
    expect(result[0]['Owner']).toBe('Alice');
    expect(result[0]['Group']).toBe('Group One');
  });
});
