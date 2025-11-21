# Meerkat Test Suite - Quick Reference Guide

## 🚀 Quick Start

### Run All Tests
```bash
cd meerkat-node
npm run test:vitest
```

### Run Specific Suite
```bash
npm run test:vitest -- filters-numeric    # Numeric filter tests
npm run test:vitest -- joins              # JOIN tests
npm run test:vitest -- aggregates         # Aggregate function tests
```

### Run in Watch Mode
```bash
npm run test:vitest -- --watch
```

---

## 📊 Current Status

**311 total tests | 246 passing (79%) | 2.2s execution time**

### ✅ Fully Working (100% pass rate)
- Numeric filters (BIGINT, NUMERIC, DOUBLE)
- String filters (VARCHAR/TEXT)
- Timestamp filters
- JOINs (simple & multi-table)
- Aggregates (COUNT, SUM, AVG, MIN, MAX, etc.)
- Ordering (single & multi-column)
- SQL functions (DATE_TRUNC, EXTRACT, string ops)
- NULL handling
- Edge cases

### ⚠️ Partially Working
- Boolean filters (50%) - blocked by multiple filter limitation
- Date filters (50%) - blocked by multiple filter limitation
- Grouping (94%) - nearly complete
- Pagination (86%) - nearly complete

### 🐛 Blocked
- Array operations (12.5%) - `UNNEST` not supported, type conversion issues
- Multiple filters - **CRITICAL BLOCKER**

---

## 🐛 Known Issues

### 1. Multiple Filters Not Supported
**Impact**: Cannot use `AND`, `OR`, or `BETWEEN` operators

**Workaround**: Use single filter conditions only

**Location**: `meerkat-core/src/cube-filter-transformer/factory.ts:142`

### 2. Array Operations Broken
**Impact**: Array filtering and aggregation fails

**Workaround**: Avoid array-based queries for now

---

## 📁 Test Structure

```
src/__vitest__/
├── comprehensive/           # Main test suites
│   ├── filters-*.test.ts   # Filter tests by type
│   ├── joins.test.ts       # JOIN operations
│   ├── aggregates.test.ts  # Aggregate functions
│   ├── grouping.test.ts    # GROUP BY operations
│   ├── ordering.test.ts    # ORDER BY operations
│   ├── pagination.test.ts  # LIMIT/OFFSET
│   ├── sql-functions.test.ts  # SQL functions
│   └── edge-cases.test.ts  # Edge cases & NULL handling
│
├── synthetic/              # Test data infrastructure
│   ├── schema-setup.ts     # Table creation & data generation
│   └── table-schemas.ts    # TableSchema definitions
│
└── helpers/                # Test utilities
    └── test-helpers.ts     # Helper functions
```

---

## 🔧 Adding New Tests

### 1. Choose the Right File
- **Filters** → `filters-[type].test.ts`
- **JOINs** → `joins.test.ts`
- **Aggregates** → `aggregates.test.ts`
- **Functions** → `sql-functions.test.ts`
- **Edge cases** → `edge-cases.test.ts`

### 2. Follow the Pattern
```typescript
import { describe, it, expect, beforeAll } from 'vitest';
import { cubeQueryToSQL } from '../../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../../duckdb-exec';
import { FACT_ALL_TYPES_SCHEMA } from '../synthetic/table-schemas';

describe('My New Test Suite', () => {
  beforeAll(async () => {
    // Setup runs once per suite
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  it('should test something', async () => {
    const query = {
      measures: ['fact_all_types.count'],
      filters: [
        {
          member: 'fact_all_types.priority',
          operator: 'equals',
          values: ['high'],
        },
      ],
      dimensions: [],
    };

    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [FACT_ALL_TYPES_SCHEMA],
    });

    const result = await duckdbExec(sql);
    
    expect(Number(result[0]?.fact_all_types__count)).toBeGreaterThan(0);
  });
});
```

### 3. Use Test Helpers
```typescript
import { measureExecutionTime } from '../helpers/test-helpers';

const { result, duration } = await measureExecutionTime(async () => {
  return duckdbExec(sql);
});

expect(duration).toBeLessThan(1000); // Performance assertion
```

---

## 📊 Synthetic Data Overview

### fact_all_types (1M rows)
```
Numeric:
  - id_bigint: 0 to 999,999
  - resolved_by: 50% NULL, 50% random values
  - metric_double: random doubles

Boolean:
  - is_active: 50% true
  - is_deleted: 10% true

Strings:
  - priority: low(20%), medium(20%), high(20%), critical(20%), urgent(20%)
  - status: open(33%), in_progress(33%), closed(33%)

Dates:
  - created_date: cycles every 366 days (includes Dec 31!)
  - record_date: cycles every 1460 days (~4 years)
  - mitigated_date: 30% NULL, rest random

Timestamps:
  - created_timestamp: date + time of day
  - identified_timestamp: offset timestamps

Arrays:
  - tags: 40% empty, 33% single value, 27% multiple values
  - part_ids: similar distribution
```

### dim_user (10K rows)
```
- user_id: user_0 to user_9999
- user_segment: enterprise(33%), pro(33%), free(33%)
- user_department: engineering(25%), product(25%), support(25%), sales(25%)
```

### dim_part (5K rows)
```
- part_id: part_0 to part_4999
- product_category: 5 categories (electronics, furniture, etc.)
- product_tier: premium(33%), standard(33%), budget(33%)
```

---

## 🔍 Debugging Tips

### 1. See Generated SQL
```typescript
const sql = await cubeQueryToSQL({
  query,
  tableSchemas: [FACT_ALL_TYPES_SCHEMA],
});
console.log('Generated SQL:', sql);
```

### 2. Run SQL Directly
```typescript
const result = await duckdbExec(`
  SELECT * FROM fact_all_types WHERE priority = 'high' LIMIT 10
`);
console.log('Direct result:', result);
```

### 3. Check Data Distribution
```typescript
const stats = await duckdbExec(`
  SELECT 
    priority,
    COUNT(*) as count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
  FROM fact_all_types
  GROUP BY priority
  ORDER BY priority
`);
console.table(stats);
```

### 4. Verbose Test Output
```bash
npm run test:vitest -- --reporter=verbose
```

---

## ⚡ Performance Expectations

| Query Type | Expected Time | Actual (1M rows) |
|:-----------|:-------------:|:----------------:|
| Simple filter | < 200ms | ~50-100ms |
| Aggregation | < 500ms | ~200-300ms |
| JOIN (2 tables) | < 1s | ~400-600ms |
| Multi-table JOIN | < 2s | ~800-1200ms |
| Large IN (1000+ values) | < 1s | ~400-600ms |

---

## 📚 Documentation

- **COMPREHENSIVE_TEST_SUMMARY.md** - Complete summary & status
- **CRITICAL_BUGS_FOUND.md** - Detailed bug reports
- **COMPREHENSIVE_TEST_MATRIX.md** - Full 739 test case matrix
- **QUICK_REFERENCE.md** (this file) - Quick reference

---

## 🎯 Common Tasks

### Add a New Filter Type Test
1. Copy `filters-numeric.test.ts` or similar
2. Rename to `filters-[your-type].test.ts`
3. Update test cases for your data type
4. Run: `npm run test:vitest -- filters-[your-type]`

### Test a Bug Fix
1. Find the relevant test file
2. Locate the failing test (marked with `×`)
3. Make your fix in `meerkat-core` or `meerkat-node`
4. Run: `npm run test:vitest -- [test-name]`
5. Verify test now passes

### Add Performance Test
```typescript
it('should execute quickly (< 500ms)', async () => {
  const start = Date.now();
  
  // Your query here
  await duckdbExec(sql);
  
  const duration = Date.now() - start;
  expect(duration).toBeLessThan(500);
});
```

---

## 🚨 Important Notes

1. **BigInt Handling**: DuckDB returns `COUNT(*)` as `BigInt` (e.g., `5n`). Always use `Number()` in assertions:
   ```typescript
   expect(Number(result[0]?.fact_all_types__count)).toBe(5);
   ```

2. **Date Formats**: Dates from DuckDB may be `Date` objects or ISO strings. Handle both:
   ```typescript
   const dateStr = row.created_date instanceof Date
     ? row.created_date.toISOString().split('T')[0]
     : row.created_date;
   ```

3. **NULL vs Empty**: Arrays are never NULL, but can be empty (`[]`). Booleans are never NULL.

4. **Multiple Filters**: Currently not supported. Avoid tests with `AND`/`OR`/`BETWEEN`.

---

**Last Updated**: November 20, 2025  
**Test Version**: 1.0  
**Maintainer**: Meerkat Team

