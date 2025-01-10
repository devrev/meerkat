import { DuckDBSingleton } from '@devrev/meerkat-node';
import { Connection, Database } from 'duckdb';
import { duckdbExec } from 'meerkat-node/src/duckdb-exec';

export class NodeDuckDB {
  private db: Database;
  private connection: Connection | null = null;

  constructor() {
    this.db = DuckDBSingleton.getInstance();

    this.init();
  }

  private async init(): Promise<void> {
    const connection = await this.db.connect();

    connection.exec('INSTALL arrow; LOAD arrow;');
    connection.exec('INSTALL json; LOAD json;');
  }

  async connect() {
    if (!this.connection) {
      this.connection = await this.db.connect();
    }

    return this.connection;
  }

  async executeQuery({ query }: { query: string }): Promise<any> {
    const result = await duckdbExec(query);

    const rows = await result;

    return rows;
  }

  async jsonToBuffer(jsonPath: string): Promise<Uint8Array> {
    const connection = await this.connect();
    try {
      await duckdbExec(`
          CREATE TABLE temp_data AS 
          SELECT * FROM read_json_auto(${JSON.stringify(
            jsonPath
          )}, auto_detect=true)
      `);

      const stream = await connection.arrowIPCStream('SELECT * FROM temp_data');
      const chunks: Uint8Array[] = [];

      for await (const chunk of stream) {
        chunks.push(chunk);
      }

      // Calculate total length and create final Uint8Array
      const totalLength = chunks.reduce((acc, chunk) => acc + chunk.length, 0);
      const uint8Array = new Uint8Array(totalLength);

      // Copy all chunks into the final array
      let offset = 0;
      for (const chunk of chunks) {
        uint8Array.set(chunk, offset);
        offset += chunk.length;
      }

      return uint8Array;
    } finally {
      // Ensure cleanup happens even if there's an error
      await duckdbExec('DROP TABLE IF EXISTS temp_data');
    }
  }
}

const duckDB = new NodeDuckDB();

export default duckDB;
