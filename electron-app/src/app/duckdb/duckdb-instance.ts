import { DuckDBInstance } from '@duckdb/node-api';

export async function getDBInstance() {
  const instance = await DuckDBInstance.create('table.db');

  const connection = await instance.connect();

  return connection;
}
