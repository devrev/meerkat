import { ColumnInfo, Connection, Database, TableData } from 'duckdb';

import { DuckDBSingleton } from '../duckdb-singleton';

export class DBMNode {
  private db: Database | null = null;
  private connection: Connection | null = null;
  private initPromise: Promise<void>;

  constructor({ onInitialize }: { onInitialize: (db: Database) => void }) {
    this.initPromise = this.initialize(onInitialize);
  }

  /**
   * Initialize the DuckDB instance
   */
  private async initialize(
    onInitialize: (db: Database) => void
  ): Promise<void> {
    this.db = await DuckDBSingleton.getInstance();

    onInitialize(this.db);
  }

  /**
   * Execute a query on the DuckDB connection.
   * @param query - The query to execute.
   * @returns The result of the query.
   */
  async query(
    query: string
  ): Promise<{ columns: ColumnInfo[]; data: TableData }> {
    await this.initPromise;

    return new Promise((resolve, reject) => {
      if (!this.db) {
        reject(new Error('Database not initialized'));
        return;
      }

      this.db.prepare(query, (err, statement) => {
        if (err) {
          reject(new Error(`Query preparation failed: ${err.message}`));
          return;
        }

        const columns = statement.columns();

        statement.all((err, data) => {
          if (err) {
            reject(new Error(`Query execution failed: ${err.message}`));
            return;
          }

          resolve({ columns, data });
        });
      });
    });
  }

  /**
   * Close the DuckDB connection and cleanup resources.
   */
  async close(): Promise<void> {
    await this.initPromise;

    if (this.connection) {
      this.connection.close();
      this.connection = null;
    }

    if (this.db) {
      this.db.close();
      this.db = null;
    }
  }
}
