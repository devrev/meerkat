import { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { FileManagerType } from '../file-manager/file-manager-type';
import { DBMLogger } from '../logger/logger-types';

export interface DBMConstructorOptions {
  fileManager: FileManagerType;
  db: AsyncDuckDB;
  logger: DBMLogger;
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
    /**
     * Timestamp when the query was added to the queue
     */
    timestamp: number;
  }[] = [];
  private queryQueueRunning = false;
  private logger: DBMLogger;

  constructor({ fileManager, db, logger }: DBMConstructorOptions) {
    this.fileManager = fileManager;
    this.db = db;
    this.logger = logger;
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
    const startMountTime = Date.now();
    await this.fileManager.mountFileBufferByTableNames(tableNames);
    const endMountTime = Date.now();

    this.logger.debug(
      'Time spent in mounting files:',
      endMountTime - startMountTime,
      'ms',
      query
    );

    /**
     * Execute the query
     */
    const startQueryTime = Date.now();
    const result = await this.query(query);
    const endQueryTime = Date.now();

    this.logger.debug(
      'Time spent in executing query by duckdb:',
      endQueryTime - startQueryTime,
      'ms',
      query
    );

    /**
     * Unload all the files from the database, so that the files can be removed from memory
     */
    const startUnmountTime = Date.now();
    await this.fileManager.unmountFileBufferByTableNames(tableNames);
    const endUnmountTime = Date.now();

    this.logger.debug(
      'Time spent in unmounting files:',
      endUnmountTime - startUnmountTime,
      'ms',
      query
    );

    return result;
  }

  /**
   * Execute the queries in the queue one by one
   * If there is no query in the queue, stop the queue
   * Recursively call itself to execute the next query
   */
  private async _startQueryExecution() {
    this.logger.debug('Query queue length:', this.queriesQueue.length);
    /**
     * Get the first query
     */
    const query = this.queriesQueue.shift();
    /**
     * If there is no query, stop the queue
     */
    if (!query) {
      this.logger.debug('Query queue is empty, stopping the queue execution');
      this.queryQueueRunning = false;
      return;
    }

    try {
      const startTime = Date.now();
      this.logger.debug(
        'Time since query was added to the queue:',
        startTime - query.timestamp,
        'ms',
        query.query
      );
      /**
       * Execute the query
       */
      const result = await this._queryWithTableNames(
        query.query,
        query.tableNames
      );
      const endTime = Date.now();

      this.logger.debug(
        'Total time spent along with queue time',
        endTime - query.timestamp,
        'ms',
        query.query
      );
      /**
       * Resolve the promise
       */
      query.promise.resolve(result);
    } catch (error) {
      this.logger.warn('Error while executing query:', query.query);
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
      this.logger.debug('Query queue is already running');
      return;
    }
    this.logger.debug('Starting query queue');
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
        timestamp: Date.now(),
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
