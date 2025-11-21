# Meerkat Vitest Comprehensive Tests

This directory contains comprehensive tests for the Meerkat query engine using Vitest.

## 📦 What's Here

```
__vitest__/
├── README.md                           # This file
├── comprehensive/                      # Comprehensive test suites
│   ├── filters-numeric.test.ts         # Numeric filter tests (23 tests)
│   ├── filters-string.test.ts          # String filter tests (25+ tests)
│   ├── filters-date.test.ts            # Coming soon
│   ├── filters-array.test.ts           # Coming soon
│   ├── grouping-aggregates.test.ts     # Coming soon
│   ├── joins.test.ts                   # Coming soon
│   └── ...                             # More test suites
├── synthetic/                          # Synthetic test data
│   ├── schema-setup.ts                 # Creates 1M+ rows of test data
│   └── table-schemas.ts                # TableSchema definitions
└── helpers/                            # Test utilities
    └── test-helpers.ts                 # Batch error reporter, etc.
```

## 🚀 Quick Start

### Install Dependencies

```bash
cd meerkat-node
npm install
```

This will install Vitest and related dependencies.

### Run All Tests

```bash
npm run test:vitest
```

### Run with UI

```bash
npm run test:vitest:ui
```

### Run with Coverage

```bash
npm run test:vitest:coverage
```

### Run Specific Test File

```bash
npx vitest run src/__vitest__/comprehensive/filters-numeric.test.ts
```

### Watch Mode

```bash
npm run test:vitest:watch
```

## 📊 Current Test Coverage

### Completed Test Suites

- ✅ **Numeric Filters** (`filters-numeric.test.ts`) - 23 tests
  - BIGINT type with all operators
  - NUMERIC/DECIMAL type filters
  - DOUBLE/FLOAT type filters
  - Combined numeric filters (AND/OR)
  - NULL handling
  - Edge cases (0, empty results, large IN lists)

- ✅ **String Filters** (`filters-string.test.ts`) - 25+ tests
  - equals/notEquals operators
  - contains/notContains operators
  - IN/NOT IN operators
  - Set/NotSet (NULL handling)
  - Special characters (quotes, apostrophes, backslashes)
  - Empty strings
  - Combined string filters
  - Large IN lists with string_split optimization

### In Progress

- 🔄 **Date Filters** - Coming next
- 🔄 **Array Filters** - Coming soon
- 🔄 **Grouping & Aggregates** - Coming soon
- 🔄 **Joins** - Coming soon

### Total Progress

- **Completed**: 48+ tests
- **Target**: 739 tests
- **Progress**: ~6.5%

## 🎯 Test Philosophy

### Data-Driven Tests

We use parameterized tests to cover many permutations efficiently:

```typescript
const testCases = [
  { operator: 'equals', value: 100, expected: 1 },
  { operator: 'gt', value: 900000, expected: 100000 },
  // ... more cases
];

testCases.forEach(({ operator, value, expected }) => {
  it(`should filter with ${operator}`, async () => {
    // Test implementation
  });
});
```

### Reference Oracle Pattern

Every test validates Meerkat's output against direct SQL:

```typescript
// What Meerkat generates
const meerkatSQL = await cubeQueryToSQL({ query, tableSchemas });
const meerkatResult = await duckdbExec(meerkatSQL);

// Reference SQL (known correct)
const referenceSQL = `SELECT COUNT(*) FROM fact WHERE x > 100`;
const referenceResult = await duckdbExec(referenceSQL);

// Validate
expect(meerkatResult[0].count).toBe(referenceResult[0].count);
```

### Performance Budgets

Tests include performance assertions:

```typescript
const { result, duration } = await measureExecutionTime(() => duckdbExec(sql));
expect(duration).toBeLessThan(5000); // < 5 seconds
```

## 🗄️ Synthetic Test Data

### Scale

- **fact_all_types**: 1,000,000 rows with ~50 columns
- **dim_user**: 10,000 rows
- **dim_part**: 5,000 rows
- **Total**: ~1,015,000 rows

### Data Types Covered

- **BIGINT**: id_bigint, metric_bigint, small_bigint
- **NUMERIC**: metric_numeric, precise_numeric
- **DOUBLE**: metric_double, metric_float
- **BOOLEAN**: is_deleted, flag_boolean, is_active
- **VARCHAR**: priority, status, severity_label, environment, title, description
- **DATE**: record_date, created_date, mitigated_date
- **TIMESTAMP**: created_timestamp, identified_timestamp, deployment_time
- **VARCHAR[]**: tags, owned_by_ids, part_ids
- **JSON** (as VARCHAR): metadata_json, stage_json, impact_json

### Data Generation Time

- **Full setup**: ~30-60 seconds (1M+ rows)
- **One-time**: Data is created once per test suite

## 🧪 Writing New Tests

### 1. Create a new test file

```typescript
// src/__vitest__/comprehensive/my-test.test.ts
import { describe, it, expect, beforeAll } from 'vitest';
import { cubeQueryToSQL } from '../../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../../duckdb-exec';
import { createAllSyntheticTables, dropSyntheticTables } from '../synthetic/schema-setup';
import { FACT_ALL_TYPES_SCHEMA } from '../synthetic/table-schemas';

describe('My Comprehensive Tests', () => {
  beforeAll(async () => {
    await dropSyntheticTables();
    await createAllSyntheticTables();
  }, 120000); // 2 minute timeout
  
  it('should test something', async () => {
    const query = {
      measures: ['fact_all_types.count'],
      filters: [/* ... */],
      dimensions: [],
    };
    
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [FACT_ALL_TYPES_SCHEMA],
    });
    
    const result = await duckdbExec(sql);
    
    expect(result[0]?.fact_all_types__count).toBeGreaterThan(0);
  });
});
```

### 2. Use test helpers

```typescript
import { BatchErrorReporter, measureExecutionTime } from '../helpers/test-helpers';

const reporter = new BatchErrorReporter();

testCases.forEach(testCase => {
  try {
    // Run test
  } catch (error) {
    reporter.addError(testCase.name, error);
  }
});

reporter.throwIfErrors(); // Throw once with all failures
```

### 3. Performance testing

```typescript
const { result, duration } = await measureExecutionTime(() => duckdbExec(sql));

expect(result).toBeDefined();
expect(duration).toBeLessThan(1000); // < 1 second
```

## 📈 Test Execution

### Performance Benchmarks

- **Simple filter**: < 1 second
- **Complex filter**: < 5 seconds
- **Large IN list (1000+ values)**: < 2 seconds
- **Full test suite**: < 5 minutes (target)

### Current Performance

- **filters-numeric.test.ts**: ~15-20 seconds
- **filters-string.test.ts**: ~15-20 seconds

## 🐛 Troubleshooting

### Tests are slow

- Check if synthetic data is being recreated unnecessarily
- Use `beforeAll` instead of `beforeEach` for data setup
- Increase `maxConcurrency` in `vitest.config.ts`

### DuckDB errors

- Ensure you're using the singleton pattern
- Check that tables are created before tests run
- Verify SQL syntax matches DuckDB version

### Flaky tests

- Check for race conditions in data setup
- Use `retry: 1` in config for flakiness tolerance
- Validate test assumptions about data distribution

## 📝 Best Practices

1. **Always validate against reference SQL** when possible
2. **Use data-driven tests** to cover many permutations
3. **Include performance assertions** for critical paths
4. **Test edge cases**: NULL, empty strings, special characters
5. **Use meaningful test names** that describe what's being tested
6. **Keep tests independent**: No shared mutable state
7. **Clean up after yourself**: Drop tables when needed

## 🎯 Next Steps

1. Complete date/timestamp filter tests
2. Add array filter tests
3. Implement grouping & aggregation tests
4. Create join tests
5. Add combined operation tests
6. Achieve > 90% code coverage
7. Document all test patterns

## 📞 Getting Help

- Review existing tests for patterns
- Check `QUICK_START_GUIDE.md` in project root
- See `COMPREHENSIVE_TEST_MATRIX.md` for full test catalog
- Check `confidence_plan.md` for strategic overview

---

**Last Updated**: Implementation in progress  
**Status**: Phase 1 (Foundation) complete, Phase 2 (High-Priority Tests) in progress  
**Tests Complete**: 48+ / 739 (~6.5%)  
**Coverage Target**: > 90% line coverage

