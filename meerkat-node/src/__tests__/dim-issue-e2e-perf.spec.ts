import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';
import { buildDimIssueFixture } from './helpers/dim-issue-fixture';

/**
 * End-to-end generation timing for a schema shaped like the real `dim_issue`
 * cube: ~360 dimensions (plain columns, json_extract_string on custom_fields,
 * string_array members with shouldUnnestGroupBy, computed CASE/epoch
 * expressions) plus ~72 measures. Projects the small fixed set of members from
 * the user's input, empty filters, ordered by a row-order column.
 *
 * Measured stage split for this shape (per generation, warm, in-memory DuckDB):
 *   getFinalBaseSQL          ~0.13ms  (CPU: wrapped projection build)
 *   deserialize round-trip   ~0.12ms  (DuckDB I/O: AST -> SQL text)
 *   applyProjectionToSQLQuery~0.05ms  (CPU: outer projection build)
 *   everything else          <0.01ms
 * The two DuckDB round-trips (getFinalBaseSQL also does one internally via
 * getWrappedBaseQueryWithProjections when the base has expressions) plus the
 * projection deserialize dominate. CPU work is real but secondary.
 */

describe('dim_issue end-to-end generation', () => {
  beforeAll(async () => {
    await duckdbExec(`
      CREATE TABLE dim_issue (
        id VARCHAR, space_id VARCHAR, subtype VARCHAR, title VARCHAR,
        display_id VARCHAR, created_date TIMESTAMP, target_close_date TIMESTAMP,
        actual_close_date TIMESTAMP, links_json VARCHAR, sla_summary VARCHAR,
        stage_json VARCHAR, priority_uenum_json VARCHAR, custom_fields VARCHAR,
        owned_by_ids VARCHAR[], custom_schema_fragment_ids VARCHAR[],
        __fdl_row_order__ INTEGER
      );
    `);
  });

  it('reports total cubeQueryToSQL wall time', async () => {
    const { schema, query } = buildDimIssueFixture();
    const contextParams = {
      current_dev_user_id: 'don:identity:dvrv-in-1:devo/2sRI6Hepzz:devu/9882',
    };

    // warmup
    await cubeQueryToSQL({ query, tableSchemas: [schema], contextParams });

    const ITER = 50;
    const start = process.hrtime.bigint();
    for (let i = 0; i < ITER; i += 1) {
      await cubeQueryToSQL({ query, tableSchemas: [schema], contextParams });
    }
    const avgMs = Number(process.hrtime.bigint() - start) / 1e6 / ITER;

    console.info(
      `[perf] dim_issue full cubeQueryToSQL avg over ${ITER}: ${avgMs.toFixed(3)}ms ` +
        `(dims=${schema.dimensions.length}, measures=${schema.measures.length}, ` +
        `projected=${query.dimensions?.length})`
    );

    expect(avgMs).toBeGreaterThan(0);
  });
});
