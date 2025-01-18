import { DuckDBSingleton } from './duckdb-singleton';
import { transformDuckDBQueryResult } from './utils/transform-duckdb-result';

export const duckdbExec = async (
  query: string
): Promise<Record<string, unknown>[]> => {
  const db = await DuckDBSingleton.getInstance();
  const connection = await db.connect();

  const result = await connection.run(query);

  const { data } = await transformDuckDBQueryResult(result);

  return data;
};
