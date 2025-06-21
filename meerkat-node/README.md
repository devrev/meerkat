# @devrev/meerkat-node

`@devrev/meerkat-node` is a library for converting cube queries into SQL and executing them in a Node.js environment using [DuckDB](https://duckdb.org/). It serves as a server-side query engine within the Meerkat ecosystem.

This package uses `@devrev/meerkat-core` to generate a DuckDB-compatible AST and `duckdb` to execute the resulting query against local data files like Parquet or CSV.

## Key Features

- **Cube to SQL Execution**: Translates cube queries into SQL and executes them.
- **Node.js Optimized**: Built to work seamlessly with `duckdb`.
- **Simplified API**: Provides a `duckdbExec` utility for easy query execution.

## Installation

```bash
npm install @devrev/meerkat-node @devrev/meerkat-core
```

## Basic Usage

Here's a basic example of how to convert a cube query into SQL and execute it.

```typescript
import { cubeQueryToSQL, duckdbExec } from '@devrev/meerkat-node';
import { Query, TableSchema } from '@devrev/meerkat-core';

async function main() {
  // 1. Define your table schema. In Node.js, the SQL typically points to a data file.
  const tableSchema: TableSchema = {
    name: 'users',
    sql: `SELECT * FROM 'users.parquet'`,
    columns: [
      { name: 'id', type: 'INTEGER' },
      { name: 'name', type: 'VARCHAR' },
      { name: 'city', type: 'VARCHAR' },
      { name: 'signed_up_at', type: 'TIMESTAMP' },
    ],
  };

  // 2. Define your Cube query.
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

  // 3. Convert the query to SQL.
  const sql = await cubeQueryToSQL({
    query,
    tableSchemas: [tableSchema],
  });

  // 4. Execute the query using DuckDB.
  const results = await duckdbExec(sql);

  console.log('Query Results:', results);
}

main();
```
