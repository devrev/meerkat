import {
  DuckDBConnection,
  DuckDBInstance,
  DuckDBResult,
  DuckDBType,
  DuckDBValue,
} from '@duckdb/node-api';
import { DuckDBSingleton } from '../duckdb-singleton';
import { convertRowsToRecords } from '../utils/convert-rows-to-records';
import { convertRecordDuckDBValueToJSON } from '../utils/duckdb-type-convertor';

export class DBMNode {
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

  private async getTransformedData(result: DuckDBResult): Promise<{
    data: Record<string, DuckDBValue>[];
    columnTypes: { name: string; type: DuckDBType }[];
  }> {
    const columnNames = result.columnNames();
    const columnTypes = result.columnTypes();

    const columnDefinitions = columnNames.map((name, index) => ({
      name,
      type: columnTypes[index],
    }));

    const rows = await result.getRows();

    const data = convertRowsToRecords(rows, columnNames);

    return { data, columnTypes: columnDefinitions };
  }

  /**
   * Execute a query on the DuckDB connection.
   */
  async query(query: string): Promise<{
    data: Record<string, DuckDBValue>[];
    columnTypes: { name: string; type: DuckDBType }[];
  }> {
    const connection = await this.getConnection();

    if (!connection) throw new Error('DuckDB connection not initialized');

    const result = await connection.run(query);

    const data = await this.getTransformedData(result);

    const transformedData = convertRecordDuckDBValueToJSON(
      data.data,
      data.columnTypes
    );

    console.log(transformedData);
    return transformedData;
  }

  /**
   * Close the DuckDB connection
   */
  async close(): Promise<void> {
    this.connection?.close();
  }
}
