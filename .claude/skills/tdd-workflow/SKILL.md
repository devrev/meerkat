---
name: tdd-workflow
description: "TDD workflow for Meerkat SDK — Jest with @swc/jest, Nx monorepo, SQL AST testing patterns."
origin: ECC (optimized for Meerkat)
---

# Test-Driven Development Workflow

TDD workflow tailored for the Meerkat SDK (Cube-to-DuckDB query translation).

## When to Activate

- Writing new filter operators or transformers
- Fixing bugs in SQL generation
- Refactoring AST builder logic
- Adding new cube-to-DuckDB translation features

## Core Principles

### 1. Tests BEFORE Code
ALWAYS write tests first, then implement code to make tests pass.

### 2. Coverage Requirements
- Minimum 80% coverage
- All edge cases covered (null, empty arrays, special characters, nested `.` fields)
- Error scenarios tested

### 3. Test Types

#### Unit Tests
- Individual filter transformers
- AST builder functions
- Cube-to-DuckDB translation logic
- Utility functions

#### Integration Tests
- Full query generation pipeline
- DuckDB query execution (meerkat-node)

## TDD Workflow Steps

### Step 1: Write Test Cases
```typescript
describe('notEquals filter transformer', () => {
  it('generates correct AST for string value', () => {
    const result = transformNotEquals('status', ['active']);
    expect(generatedSQL).toBe('SELECT * FROM "table" WHERE "status" != \'active\'');
  });

  it('handles null value', () => {
    const result = transformNotEquals('status', [null]);
    expect(generatedSQL).toContain('IS NOT NULL');
  });

  it('handles nested field with . delimiter', () => {
    const result = transformNotEquals('table.column', ['value']);
    expect(generatedSQL).toContain('"table"."column"');
  });
});
```

### Step 2: Run Tests (They Should Fail)
```bash
npx nx test meerkat-core --testPathPattern="not-equals"
```

### Step 3: Implement Code
Write minimal code to make tests pass.

### Step 4: Run Tests Again
```bash
npx nx test meerkat-core
```

### Step 5: Refactor
Improve code quality while keeping tests green.

### Step 6: Verify Coverage
```bash
npx nx test meerkat-core --coverage
```

## Testing Patterns

### Filter Transformer Test
```typescript
import { transformFilter } from '../cube-filter-transformer';

describe('equals filter', () => {
  it('generates correct SQL for single value', () => {
    const sql = transformFilter({
      member: 'orders.status',
      operator: 'equals',
      values: ['active'],
    });
    expect(sql).toBe('SELECT "col" FROM "table" WHERE "status" = \'active\'');
  });

  it('generates correct SQL for number array', () => {
    const sql = transformFilter({
      member: 'orders.amount',
      operator: 'equals',
      values: [1, 2, 3],
    });
    expect(sql).toContain('IN (1, 2, 3)');
  });
});
```

### AST Structure Test
```typescript
describe('AST builder', () => {
  it('builds correct compare node', () => {
    const node = buildCompareNode('col', '=', 'value');
    expect(node).toMatchObject({
      type: 'COMPARE',
      operator: '=',
    });
  });
});
```

## Test File Organization

```
packages/meerkat-core/src/
├── cube-filter-transformer/
│   ├── equals/
│   │   ├── index.ts
│   │   └── __tests__/
│   │       └── equals.spec.ts
│   ├── not-equals/
│   │   ├── index.ts
│   │   └── __tests__/
│   │       └── not-equals.spec.ts
│   └── ...
├── ast-builder/
│   └── __tests__/
└── types/
```

## Running Tests

```bash
# Single project
npx nx test meerkat-core

# All projects
npx nx run-many --target=test --all

# Watch mode
npx nx test meerkat-core --watch

# Specific test file
npx nx test meerkat-core --testPathPattern="filter"

# With coverage
npx nx test meerkat-core --coverage
```

## Common Testing Mistakes to Avoid

### WRONG: Testing implementation details
```typescript
expect(internalASTNode._private).toBe('something');
```

### CORRECT: Test the generated SQL output
```typescript
expect(generatedSQL).toBe('SELECT "col" FROM "table" WHERE "col" = 1');
```

### WRONG: String concatenation in test expectations
```typescript
expect(sql).toBe('SELECT ' + col + ' FROM ' + table);
```

### CORRECT: Explicit full SQL assertions
```typescript
expect(sql).toBe('SELECT "col" FROM "table" WHERE "col" = 1');
```

## Best Practices

1. **Write Tests First** - Always TDD
2. **Test SQL output directly** - Assert on generated SQL strings
3. **Cover edge cases** - null, empty arrays, `.` delimiters, special characters
4. **One behavior per test** - Focus on single assertion
5. **Independent tests** - No shared mutable state between tests
6. **Keep tests fast** - Unit tests < 50ms each
7. **Add regression tests** - Reference DevRev issue IDs for bug fixes
