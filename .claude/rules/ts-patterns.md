---
paths:
  - "**/*.ts"
  - "**/*.tsx"
  - "**/*.js"
  - "**/*.jsx"
---
# TypeScript/JavaScript Patterns

> This file extends [common/patterns.md](../common/patterns.md) with TypeScript/JavaScript specific content.

## API Response Format

```typescript
interface ApiResponse<T> {
  success: boolean
  data?: T
  error?: string
  meta?: {
    total: number
    page: number
    limit: number
  }
}
```

## Transformer Pattern

Each cube-to-DuckDB transformer follows a consistent pattern:

```typescript
// Each filter operator gets its own module under cube-filter-transformer/<operator>/
export function transformOperator(
  member: string,
  values: FilterValue[],
): DuckDBNode {
  // Build AST node, never string concatenation
  return buildCompareNode(member, operator, values)
}
```

## AST Builder Pattern

All SQL generation goes through DuckDB JSON AST:

```typescript
// WRONG: String concatenation
const sql = `SELECT "${col}" FROM "${table}" WHERE "${col}" = ${value}`

// CORRECT: AST node construction
const node: SelectNode = {
  type: 'SELECT',
  select_list: [buildColumnRef(col)],
  from_table: buildTableRef(table),
  where_clause: buildCompare(col, '=', value),
}
```
