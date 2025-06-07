# @devrev/meerkat-browser

`@devrev/meerkat-browser` is a library for converting cube queries into SQL and executing them in a browser environment using [@duckdb/duckdb-wasm](https://github.com/duckdb/duckdb-wasm). It serves as a client-side query engine within the Meerkat ecosystem.

This package uses `@devrev/meerkat-core` to generate a DuckDB-compatible AST and `@duckdb/duckdb-wasm` to execute the resulting query against data sources available to the browser.

## Key Features

- **Cube to SQL Execution**: Translates cube queries into SQL and executes them in the browser.
- **Browser Optimized**: Built to work seamlessly with `@duckdb/duckdb-wasm`.
- **Client-Side Analytics**: Enables powerful, in-browser data analysis without a server round-trip.

## Installation

```bash
npm install @devrev/meerkat-browser @devrev/meerkat-core @duckdb/duckdb-wasm
```

`@duckdb/duckdb-wasm` is a peer dependency and should be configured according to its documentation.

## Usage

Here's a example of how to convert a cube query into SQL and execute the query in the client side with duckdb-wasm.

```typescript
import * as duckdb from '@duckdb/duckdb-wasm';
import { cubeQueryToSQL } from '@devrev/meerkat-browser';
import { Query, TableSchema } from '@devrev/meerkat-core';

async function main() {
  // 1. Initialize DuckDB-WASM
  const logger = new duckdb.ConsoleLogger();
  const worker = new Worker(duckdb.getJsDelivrWorker());
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.open(bundle);
  const connection = await db.connect();

  // 2. Define your table schemas
  const tableSchemas: TableSchema[] = [
    {
      name: 'users',
      // The SQL could point to a registered file or another data source
      sql: 'SELECT * FROM users',
      columns: [
        { name: 'id', type: 'INTEGER' },
        { name: 'name', type: 'VARCHAR' },
        { name: 'city', type: 'VARCHAR' },
        { name: 'signed_up_at', type: 'TIMESTAMP' },
      ],
    },
  ];

  // 3. Define your Cube query
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

  // 4. Convert the query to SQL
  const sqlQuery = await cubeQueryToSQL({
    connection,
    query,
    tableSchemas,
  });

  console.log('Generated SQL:', sqlQuery);

  // 5. You can now execute the generated SQL query with DuckDB
  const result = await connection.query(sqlQuery);
  console.log(
    'Query Results:',
    result.toArray().map((row) => row.toJSON())
  );
}

main();
```
