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
 * This shape has NO filter params, so the only DuckDB round-trip in generation
 * is the preBaseQuery deserialize (AST -> SQL text). The AST-free
 * `buildPreBaseQuerySync` fast-path removes it: measured in isolation the
 * preBaseQuery step drops from ~0.14ms (round-trip) to ~0.004ms (~31x), taking
 * full cubeQueryToSQL for this input from ~0.38ms down to ~0.25ms. The
 * remaining cost is pure CPU (getFinalBaseSQL wrapped-projection build +
 * applyProjectionToSQLQuery), no DuckDB.
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
