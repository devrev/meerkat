import { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { FileManagerType } from '../file-manager/file-manager-type';

export interface DBMConstructorOptions {
  fileManager: FileManagerType;
  db: AsyncDuckDB;
}
export class DBM {
  private fileManager: FileManagerType;
  private db: AsyncDuckDB;
  private connection: AsyncDuckDBConnection | null = null;
  private queriesQueue: {
    query: string;
    tableNames: string[];
    promise: {
      resolve: (value: any) => void;
      reject: (reason?: any) => void;
    };
  }[] = [];
  private queryQueueRunning = false;

  constructor(props: DBMConstructorOptions) {
    this.fileManager = props.fileManager;
    this.db = props.db;
  }

  private async _getConnection() {
    if (!this.connection) {
      this.connection = await this.db.connect();
    }
    return this.connection;
  }

  private async _queryWithTableNames(query: string, tableNames: string[]) {
    /**
     * Load all the files into the database
     */
    await this.fileManager.mountFileBufferByTableNames(tableNames);

    /**
     * Execute the query
     */
    const result = await this.query(query);

    /**
     * Unload all the files from the database, so that the files can be removed from memory
     */
    await this.fileManager.unmountFileBufferByTableNames(tableNames);

    return result;
  }

  /**
   * Execute the queries in the queue one by one
   * If there is no query in the queue, stop the queue
   * Recursively call itself to execute the next query
   */
  private async _startQueryExecution() {
    /**
     * Get the first query
     */
    const query = this.queriesQueue.shift();
    /**
     * If there is no query, stop the queue
     */
    if (!query) {
      this.queryQueueRunning = false;
      return;
    }

    try {
      /**
       * Execute the query
       */
      const result = await this._queryWithTableNames(
        query.query,
        query.tableNames
      );

      /**
       * Resolve the promise
       */
      query.promise.resolve(result);
    } catch (error) {
      /**
       * Reject the promise, so the caller can catch the error
       */
      query.promise.reject(error);
    }

    /**
     * Start the next query
     */
    this._startQueryExecution();
  }

  /**
   * Start the query queue execution if it is not running
   */
  private _startQueryQueue() {
    if (this.queryQueueRunning) {
      return;
    }
    this.queryQueueRunning = true;
    this._startQueryExecution();
  }

  public getQueueLength() {
    return this.queriesQueue.length;
  }

  public isQueryRunning() {
    return this.queryQueueRunning;
  }

  public async queryWithTableNames(query: string, tableNames: string[]) {
    const promise = new Promise((resolve, reject) => {
      this.queriesQueue.push({
        query,
        tableNames,
        promise: {
          resolve,
          reject,
        },
      });
    });
    this._startQueryQueue();
    return promise;
  }

  async query(query: string) {
    /**
     * Get the connection or create a new one
     */
    const connection = await this._getConnection();

    /**
     * Execute the query
     */
    return connection.query(query);
  }
}
