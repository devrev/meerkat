import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { FileManagerType } from '../file-manager/file-manager-type';
import { DBMEvent } from '../logger/event-types';
import { DBMLogger } from '../logger/logger-types';
import { InstanceManagerType } from './instance-manager';

export interface DBMConstructorOptions {
  fileManager: FileManagerType;
  logger: DBMLogger;
  onEvent?: (event: DBMEvent) => void;
  instanceManager: InstanceManagerType;
  options?: {
    /**
     * shutdown the database after inactive of this time in milliseconds
     */
    shutdownInactiveTime?: number;
  };
}
export class DBM {
  private fileManager: FileManagerType;
  private instanceManager: InstanceManagerType;
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
  private onEvent?: (event: DBMEvent) => void;
  private options: DBMConstructorOptions['options'];
  private terminateDBTimeout: NodeJS.Timeout | null = null;

  constructor({
    fileManager,
    logger,
    onEvent,
    options,
    instanceManager,
  }: DBMConstructorOptions) {
    this.fileManager = fileManager;
    this.logger = logger;
    this.onEvent = onEvent;
    this.options = options;
    this.instanceManager = instanceManager;
  }

  private async _shutdown() {
    if (this.connection) {
      await this.connection.close();
      this.connection = null;
    }
    this.logger.debug('Shutting down the DB');
    await this.instanceManager.terminateDB();
  }

  private _startShutdownInactiveTimer() {
    if (!this.options?.shutdownInactiveTime) {
      return;
    }
    /**
     * Clear the previous timer if any, it can happen if we try to shutdown the DB before the timer is complete after the queue is empty
     */
    if (this.terminateDBTimeout) {
      clearTimeout(this.terminateDBTimeout);
    }
    this.terminateDBTimeout = setTimeout(async () => {
      /**
       * Check if there is any query in the queue
       */
      if (this.queriesQueue.length > 0) {
        this.logger.debug('Query queue is not empty, not shutting down the DB');
        return;
      }
      await this._shutdown();
    }, this.options.shutdownInactiveTime);
  }

  private _emitEvent(event: DBMEvent) {
    if (this.onEvent) {
      this.onEvent(event);
    }
  }

  private async _getConnection() {
    if (!this.connection) {
      const db = await this.instanceManager.getDB();
      this.connection = await db.connect();
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

    this._emitEvent({
      event_name: 'mount_file_buffer_duration',
      duration: endMountTime - startMountTime,
    });

    /**
     * Execute the query
     */
    const startQueryTime = Date.now();
    const result = await this.query(query);
    const endQueryTime = Date.now();

    const queryQueueDuration = endQueryTime - startQueryTime;

    this.logger.debug(
      'Time spent in executing query by duckdb:',
      queryQueueDuration,
      'ms',
      query
    );

    this._emitEvent({
      event_name: 'query_execution_duration',
      duration: queryQueueDuration,
    });

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

    this._emitEvent({
      event_name: 'unmount_file_buffer_duration',
      duration: endUnmountTime - startUnmountTime,
    });

    return result;
  }

  private async _stopQueryQueue() {
    this.logger.debug('Query queue is empty, stopping the queue execution');
    this.queryQueueRunning = false;
    /**
     * Clear the queue
     */
    this.queriesQueue = [];
    /**
     * Shutdown the DB
     */
    await this._startShutdownInactiveTimer();
  }

  /**
   * Execute the queries in the queue one by one
   * If there is no query in the queue, stop the queue
   * Recursively call itself to execute the next query
   */
  private async _startQueryExecution() {
    this.logger.debug('Query queue length:', this.queriesQueue.length);

    this._emitEvent({
      event_name: 'query_queue_length',
      value: this.queriesQueue.length,
    });

    /**
     * Get the first query
     */
    const query = this.queriesQueue.shift();
    /**
     * If there is no query, stop the queue
     */
    if (!query) {
      await this._stopQueryQueue();
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

      this._emitEvent({
        event_name: 'query_queue_duration',
        duration: startTime - query.timestamp,
      });

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
    this.logger.debug('FIND ME Adding query to the queue:', query);
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
