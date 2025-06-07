# @devrev/meerkat-core

`@devrev/meerkat-core` is the foundational library for the Meerkat ecosystem, a TypeScript SDK that seamlessly translates Cube-like queries into DuckDB Abstract Syntax Trees (AST). It provides the core logic for query transformation, designed to be environment-agnostic, running in both Node.js and browser environments.

This package focuses exclusively on generating a DuckDB-compatible AST from a JSON-based query object. It does not handle query execution, which is the responsibility of environment-specific packages like `@devrev/meerkat-node` and `@devrev/meerkat-browser`.

## Key Features

- **Cube-to-AST Transformation**: Converts Cube-style JSON queries into DuckDB-compatible SQL ASTs.
- **Environment Agnostic**: Runs in both Node.js and browser environments.
- **Type-Safe**: Provides strong TypeScript definitions for queries, schemas, and filters.
- **Advanced Filtering and Joins**: Supports complex filters, logical operators, and multi-table joins.
- **Extensible by Design**: Leverages DuckDB's native JSON serialization, avoiding the limitations of traditional query builders.

## Installation

```bash
npm install @devrev/meerkat-core
```

## Core Concepts

`meerkat-core` revolves around two main objects:

1.  **`Query`**: A JSON object that defines your analytics request. It specifies measures, dimensions, filters, and ordering.
2.  **`TableSchema`**: Defines the structure of your data tables, including columns, measures, dimensions, and joins.

The library uses these objects to generate a DuckDB AST. This AST can then be passed to an execution engine.

## Usage

Here's how to transform a Cube-style query into a DuckDB AST:

```typescript
import { cubeToDuckdbAST, Query, TableSchema } from '@devrev/meerkat-core';

// 1. Define the schema for your table
const schema: TableSchema = {
  name: 'users',
  sql: 'SELECT * FROM users',
  columns: [
    { name: 'id', type: 'INTEGER' },
    { name: 'name', type: 'VARCHAR' },
    { name: 'city', type: 'VARCHAR' },
    { name: 'signed_up_at', type: 'TIMESTAMP' },
  ],
};

// 2. Define your query
const query: Query = {
  measures: ['users.count'],
  dimensions: ['users.city'],
  filters: [
    {
      member: 'users.city',
      operator: 'equals',
      values: ['New York'],
    },
  ],
  limit: 100,
};

// 3. Generate the DuckDB AST
const ast = cubeToDuckdbAST(query, schema);

// The `ast` can now be deserialized into a SQL string for execution.
console.log(JSON.stringify(ast, null, 2));
```

## Ecosystem

`meerkat-core` is the foundation for:

- **`@devrev/meerkat-node`**: For server-side analytics in Node.js with `@duckdb/node-api`.
- **`@devrev/meerkat-browser`**: For client-side analytics in the browser with `@duckdb/duckdb-wasm`.
