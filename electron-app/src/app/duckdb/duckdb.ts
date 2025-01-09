import { getDBInstance } from './duckdb-instance';

export class NodeDuckDBFileManager {
  async executeQuery({ query }: { query: string }): Promise<any> {
    const db = await getDBInstance();

    const result = await db.run(query);

    const rows = await result.getRows();
    return rows;
  }
}

const duckDB = new NodeDuckDBFileManager();

export default duckDB;
