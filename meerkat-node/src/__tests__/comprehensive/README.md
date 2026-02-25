# Meerkat Comprehensive Test Suite

This directory contains high-coverage integration tests for `meerkat-node`, executed with Vitest through Nx.

## Directory Layout

```text
src/__tests__/comprehensive/
├── README.md
├── *.test.ts
├── synthetic/
│   ├── schema-setup.ts
│   └── table-schemas.ts
└── helpers/
    └── test-helpers.ts
```

## How To Run

Run from repository root (recommended):

```bash
npx nx test meerkat-node
```

Run one specific file:

```bash
npx vitest run --root "meerkat-node" --config "vitest.config.ts" "src/__tests__/comprehensive/filters-numeric.test.ts"
```

Run a name filter:

```bash
npx vitest run --root "meerkat-node" --config "vitest.config.ts" --testNamePattern "Numeric Filters"
```

Notes:
- `meerkat-node/package.json` does not define `test:vitest*` scripts.
- Test execution is configured in `meerkat-node/project.json` via `@nx/vite:test`.

## Current Test Runtime Behavior

Current Vitest settings from `meerkat-node/vitest.config.ts`:
- `include: ['src/**/*.{test,spec}.ts']`
- `pool: 'forks'` (safer for DuckDB native addon)
- `maxConcurrency: 1` (intentional; suites share DuckDB tables)
- `retry: 1`
- `testTimeout: 60000`

Do not increase concurrency unless table setup/teardown isolation is redesigned.

## Current Status

311 total tests with broad coverage across filters, joins, aggregates, grouping, ordering, functions, null handling, and edge cases.

Areas that still have known limitations:
- Multiple-filter composition (`AND`/`OR`/`BETWEEN`) remains a blocker in some scenarios.
- Array operations are partially limited in some query patterns.

## Known Issues

### 1) Multiple filters are not fully supported

Impact:
- Queries with combined predicates may fail or require expected-failure coverage.

Workaround:
- Prefer single-condition filter tests where possible, or mark known unsupported scenarios explicitly.

### 2) Array operations have partial support

Impact:
- Some array filtering/aggregation cases may not behave as expected.

Workaround:
- Use narrower assertions and reference SQL checks for supported paths.

## Synthetic Data Overview

### `fact_all_types` (~1M rows)

- Numeric fields span broad ranges for filter and aggregate validation.
- `resolved_by` intentionally includes NULL and non-NULL distributions.
- Boolean fields include skewed and balanced distributions.
- String dimensions include mixed cardinality values.
- Date/timestamp columns cycle across realistic ranges.
- Array fields include empty, single-value, and multi-value rows.

### `dim_user` (~10K rows)

- User identity and segmentation attributes for join and grouping tests.

### `dim_part` (~5K rows)

- Part metadata and category/tier dimensions for join-path coverage.

## Writing New Tests

Example file path:

```text
src/__tests__/comprehensive/my-feature.test.ts
```

Template:

```typescript
import { beforeAll, describe, expect, it } from 'vitest';
import { cubeQueryToSQL } from '../../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
} from './synthetic/schema-setup';
import { FACT_ALL_TYPES_SCHEMA } from './synthetic/table-schemas';

describe('My feature', () => {
  beforeAll(async () => {
    await dropSyntheticTables();
    await createAllSyntheticTables();
  });

  it('returns expected result', async () => {
    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.count'],
        dimensions: [],
        filters: [],
      },
      tableSchemas: [FACT_ALL_TYPES_SCHEMA],
    });

    const rows = await duckdbExec(sql);
    expect(Number(rows[0]?.fact_all_types__count)).toBeGreaterThan(0);
  });
});
```

## Data And Validation Approach

- Synthetic fixtures are created from `./synthetic/schema-setup.ts`.
- Schema definitions used by query generation are in `./synthetic/table-schemas.ts`.
- Most tests compare generated SQL results against a direct DuckDB reference query.
- Performance-sensitive assertions can use helpers from `./helpers/test-helpers.ts`.

## Known Caveats

- Some cases reflect current engine limitations and may intentionally use expected-failure patterns.
- DuckDB native dependency can fail to install on unsupported Node ABIs; prefer Node 22 LTS for local installs.

## Important Notes

1. DuckDB returns some aggregates (for example `COUNT(*)`) as `BigInt`; cast with `Number(...)` in assertions when needed.
2. Date values may appear as `Date` objects or strings depending on query path; normalize before strict comparisons.
3. Distinguish NULL from empty arrays in assertions (`NULL` vs `[]`).
4. Keep tests deterministic: shared DuckDB tables are the reason this suite runs serially.

## Related Files

- `meerkat-node/project.json` (Nx test target)
- `meerkat-node/vitest.config.ts` (Vitest behavior)
- `meerkat-node/vitest.setup.ts` (global test setup)

