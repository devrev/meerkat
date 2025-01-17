import { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api';
import { DuckDBSingleton } from '../duckdb-singleton';
import {
  QueryResult,
  transformDuckDBQueryResult,
} from '../utils/transform-duckdb-result';

export class DuckDBManager {
  private db: DuckDBInstance | null = null;
  private connection: DuckDBConnection | null = null;

  private initPromise: Promise<void>;

  constructor({
    initializeDatabase,
  }: {
    initializeDatabase?: (db: DuckDBInstance) => Promise<void>;
  }) {
    this.initPromise = this.initialize({ initializeDatabase });
  }

  /**
   * Initialize the DuckDB instance
   */
  private async initialize({
    initializeDatabase,
  }: {
    initializeDatabase?: (db: DuckDBInstance) => Promise<void>;
  }): Promise<void> {
    this.db = await DuckDBSingleton.getInstance();

    await initializeDatabase?.(this.db);
  }

  private async getConnection(): Promise<DuckDBConnection | null> {
    await this.initPromise;

    if (!this.connection) {
      this.connection = (await this.db?.connect()) ?? null;
    }

    return this.connection;
  }

  /**
   * Execute a query on the DuckDB connection.
   */
  async query(query: string): Promise<QueryResult> {
    const connection = await this.getConnection();

    if (!connection) throw new Error('DuckDB connection not initialized');

    const result = await connection.run(query);

    const data = await transformDuckDBQueryResult(result);

    return data;
  }

  /**
   * Close the DuckDB connection
   */
  async close(): Promise<void> {
    this.connection?.close();
  }
}
