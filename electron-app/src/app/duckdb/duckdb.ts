import { getDBInstance } from './duckdb-instance';

export class NodeDuckDBFileManager {
  async executeQuery({ query }: { query: string }): Promise<any> {
    const db = await getDBInstance();

    try {
      console.log('query', query);
      const result = await db.run(query);

      const rows = await result.getRows();
      console.log('rows', rows);
      return rows;
    } catch (error) {
      console.log('error', error);
      throw error;
    }
  }
}

const duckDB = new NodeDuckDBFileManager();

export default duckDB;
