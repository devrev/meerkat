import { DuckDBSingleton } from '@devrev/meerkat-node';
import { Database } from 'duckdb';
import { duckdbExec } from 'meerkat-node/src/duckdb-exec';

export class NodeDuckDB {
  private db: Database;

  constructor() {
    this.db = DuckDBSingleton.getInstance();
  }

  async executeQuery({ query }: { query: string }): Promise<any> {
    const result = await duckdbExec(query);

    return result;
  }
}

const duckDB = new NodeDuckDB();

export default duckDB;
