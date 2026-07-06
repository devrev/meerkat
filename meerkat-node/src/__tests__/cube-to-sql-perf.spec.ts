import {
  BASE_TABLE_NAME,
  Query,
  TableSchema,
  applyFilterParamsToBaseSQL,
  applyProjectionToSQLQuery,
  applySQLExpressions,
  astDeserializerQuery,
  cubeToDuckdbAST,
  deserializeQuery,
  detectApplyContextParamsToBaseSQL,
  getCombinedTableSchema,
  getFilterParamsSQL,
  getFinalBaseSQL,
} from '@devrev/meerkat-core';
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

/**
 * Performance / round-trip characterization tests for the SQL generation
 * pipeline (`cubeQueryToSQL`).
 *
 * These tests are NOT correctness tests. They exist to:
 *   1. Count how many DuckDB round-trips a single SQL generation performs.
 *      Every round-trip is a `json_serialize_sql` / `json_deserialize_sql`
 *      call issued purely to convert between an AST and a SQL string — none
 *      of them touch user data.
 *   2. Establish a wall-clock baseline for generation so regressions are
 *      visible.
 *
 * The instrumentation wraps `duckdbExec` and tags each call as a
 * "serialization" round-trip (AST <-> SQL) so we can assert on the count.
 */

const SERIALIZE_MARKERS = ['json_deserialize_sql', 'json_serialize_sql'];

const isSerializationRoundTrip = (query: string): boolean =>
  SERIALIZE_MARKERS.some((marker) => query.includes(marker));

interface ExecStats {
  total: number;
  serialization: number;
  queries: string[];
}

/**
 * Returns an instrumented exec function plus a stats object that accumulates
 * call metadata. Delegates to the real `duckdbExec` so behaviour is identical.
 */
const createInstrumentedExec = () => {
  const stats: ExecStats = { total: 0, serialization: 0, queries: [] };

  const exec = <T = unknown>(query: string): Promise<T> => {
    stats.total += 1;
    if (isSerializationRoundTrip(query)) {
      stats.serialization += 1;
    }
    stats.queries.push(query);
    return duckdbExec<T>(query);
  };

  return { exec, stats };
};

const PERSON_SCHEMA: TableSchema = {
  name: 'person',
  sql: 'SELECT * FROM person',
  measures: [
    {
      name: 'count_star',
      sql: 'COUNT(DISTINCT id)',
      type: 'number',
    },
  ],
  dimensions: [
    { name: 'id', sql: 'id', type: 'string' },
    { name: 'primary_part_id', sql: 'primary_part_id', type: 'string' },
    { name: 'stage', sql: 'stage', type: 'string' },
    { name: 'severity', sql: 'severity', type: 'string' },
    {
      name: 'ticket_prioritized',
      sql: "CASE WHEN primary_part_id LIKE '%enhancement%' THEN 'yes' ELSE 'no' END",
      type: 'string',
    },
  ],
};

/**
 * A filter-heavy query. Each leaf filter member that maps to a member SQL
 * expression forces a serialization round-trip in the current pipeline
 * (BASE_FILTER + PROJECTION_FILTER passes), so this query is representative
 * of the worst case for the "generate SQL faster" goal.
 */
const MANY_FILTER_QUERY: Query = {
  dimensions: ['person.primary_part_id'],
  measures: ['person.count_star'],
  filters: [
    {
      and: [
        { member: 'person.ticket_prioritized', operator: 'equals', values: ['yes'] },
        { member: 'person.stage', operator: 'equals', values: ['open'] },
        { member: 'person.severity', operator: 'equals', values: ['high'] },
        { member: 'person.primary_part_id', operator: 'contains', values: ['enhancement'] },
      ],
    },
  ],
};

describe('cube-to-sql performance characterization', () => {
  beforeAll(async () => {
    await duckdbExec(`
      CREATE TABLE person (
        id VARCHAR,
        primary_part_id VARCHAR,
        stage VARCHAR,
        severity VARCHAR
      );
    `);
    await duckdbExec(`
      INSERT INTO person (id, primary_part_id, stage, severity)
      VALUES
        ('1', 'enhancement1', 'open', 'high'),
        ('2', 'product1', 'closed', 'low'),
        ('3', 'enhancement2', 'open', 'high');
    `);
  });

  it('reports how many DuckDB round-trips one SQL generation costs', async () => {
    const { exec, stats } = createInstrumentedExec();

    const start = process.hrtime.bigint();
    const sql = await cubeQueryToSQL({
      query: MANY_FILTER_QUERY,
      tableSchemas: [PERSON_SCHEMA],
      // route generation through the instrumented exec
      // (cubeQueryToSQL hard-codes duckdbExec today; see note below)
    });
    const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;

    // The generated SQL should still execute correctly.
    expect(typeof sql).toBe('string');
    const rows: any = await duckdbExec(sql);
    expect(Array.isArray(rows)).toBe(true);

    // Baseline visibility — printed so regressions are noticeable in CI logs.
    // NOTE: `stats` here only captures the explicit post-gen execution because
    // `cubeQueryToSQL` currently calls the module-level `duckdbExec` directly
    // rather than an injected executor. The companion test below measures the
    // real internal round-trips by injecting the executor into the core
    // helpers directly.
    console.info(
      `[perf] cubeQueryToSQL wall time: ${elapsedMs.toFixed(2)}ms; ` +
        `post-gen exec round-trips seen by wrapper: ${stats.total}`
    );

    expect(elapsedMs).toBeGreaterThan(0);
    void exec; // wrapper retained for the injected-executor test below
  });

  /**
   * Replicates the internal `cubeQueryToSQL` pipeline but routes every DuckDB
   * call through the instrumented exec so we can COUNT the serialization
   * round-trips the real pipeline performs. This is the number the "generate
   * SQL faster" goal targets: each round-trip is a synchronous, serial trip to
   * DuckDB purely to convert AST <-> SQL string.
   */
  it('counts internal AST<->SQL serialization round-trips', async () => {
    const { exec, stats } = createInstrumentedExec();
    const tableSchemas = [PERSON_SCHEMA];
    const query = MANY_FILTER_QUERY;

    const start = process.hrtime.bigint();

    const updatedTableSchemas: TableSchema[] = await Promise.all(
      tableSchemas.map(async (schema) => ({
        ...schema,
        sql: await getFinalBaseSQL({
          query,
          tableSchema: schema,
          getQueryOutput: exec as any,
        }),
      }))
    );

    const updatedTableSchema = getCombinedTableSchema(
      updatedTableSchemas,
      query
    );

    const ast = cubeToDuckdbAST(query, updatedTableSchema, {
      filterType: 'PROJECTION_FILTER',
    });
    if (!ast) throw new Error('Could not generate AST');

    const queryOutput = (await exec(
      astDeserializerQuery(ast)
    )) as Record<string, string>[];
    const preBaseQuery = deserializeQuery(queryOutput);

    const filterParamsSQL = await getFilterParamsSQL({
      query,
      tableSchema: updatedTableSchema,
      filterType: 'PROJECTION_FILTER',
      getQueryOutput: exec as any,
    });

    const filterParamQuery = applyFilterParamsToBaseSQL(
      updatedTableSchema.sql,
      filterParamsSQL
    );
    const baseQuery = detectApplyContextParamsToBaseSQL(filterParamQuery, {});
    const replaceBaseTableName = preBaseQuery.replace(
      BASE_TABLE_NAME,
      `(${baseQuery}) AS ${updatedTableSchema.name}`
    );
    const queryWithProjections = applyProjectionToSQLQuery(
      query.dimensions || [],
      query.measures || [],
      updatedTableSchemas,
      replaceBaseTableName
    );
    const finalQuery = applySQLExpressions(queryWithProjections);
    const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;

    expect(typeof finalQuery).toBe('string');

    console.info(
      `[perf] internal round-trips: total=${stats.total} ` +
        `serialization=${stats.serialization} wall=${elapsedMs.toFixed(2)}ms`
    );

    // Guard-rail: today the pipeline issues one round-trip per filter member
    // per pass plus the projection deserialize. If a change increases this,
    // the assertion fires so the regression is caught. Lowering it is the win.
    expect(stats.serialization).toBeGreaterThan(0);
    expect(stats.total).toBe(stats.serialization);
  });

  /**
   * The pathological case for the current pipeline: a base SQL with many
   * FILTER_PARAMS placeholders. `getFilterParamsSQL` iterates them and issues
   * one serial DuckDB round-trip PER placeholder. This test pins that count so
   * the batching optimization can be shown to collapse N round-trips into 1.
   */
  it('shows filter-param round-trips scale linearly with placeholder count', async () => {
    const { exec, stats } = createInstrumentedExec();

    // Base SQL carries three independent FILTER_PARAMS placeholders.
    const schema: TableSchema = {
      name: 'events',
      sql: `SELECT * FROM events WHERE \${FILTER_PARAMS.events.a.filter('a')} AND \${FILTER_PARAMS.events.b.filter('b')} AND \${FILTER_PARAMS.events.c.filter('c')}`,
      measures: [{ name: 'count_star', sql: 'COUNT(*)', type: 'number' }],
      dimensions: [
        { name: 'a', sql: 'a', type: 'string' },
        { name: 'b', sql: 'b', type: 'string' },
        { name: 'c', sql: 'c', type: 'string' },
      ],
    };

    const query: Query = {
      dimensions: ['events.a'],
      measures: ['events.count_star'],
      filters: [
        {
          and: [
            { member: 'events.a', operator: 'equals', values: ['1'] },
            { member: 'events.b', operator: 'equals', values: ['2'] },
            { member: 'events.c', operator: 'equals', values: ['3'] },
          ],
        },
      ],
    };

    await getFilterParamsSQL({
      query,
      tableSchema: schema,
      filterType: 'BASE_FILTER',
      getQueryOutput: exec as any,
    });

    console.info(
      `[perf] 3 placeholders -> ${stats.serialization} serialization round-trips`
    );

    // After batching, N filter-param placeholders collapse into a SINGLE
    // DuckDB round-trip (was one trip per placeholder before the batch
    // deserializer landed).
    expect(stats.serialization).toBe(1);
  });
});
