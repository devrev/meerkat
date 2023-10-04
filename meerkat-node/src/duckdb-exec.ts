import { DuckDBSingleton } from './duckdb-singleton';

export const duckdbExec = <T = unknown>(query: string): Promise<T> => {
  const db = DuckDBSingleton.getInstance();
  return new Promise((resolve, reject) => {
    db.all(query, (err, res) => {
      if (err) {
        reject(err);
      }
      resolve(res as T);
    });
  });
};
