import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { FileManagerType } from '../file-manager/file-manager-type';
import { DBMEvent } from '../logger/event-types';
import { DBMLogger } from '../logger/logger-types';
import { InstanceManagerType } from './instance-manager';

import { DBMConstructorOptions, QueryOptions, QueryQueueItem } from './types';

export class DBM {
  private fileManager: FileManagerType;
  private instanceManager: InstanceManagerType;
  private connection: AsyncDuckDBConnection | null = null;
  private queriesQueue: QueryQueueItem[] = [];

  private queryQueueRunning = false;
  private logger: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;
  private options: DBMConstructorOptions['options'];
  private terminateDBTimeout: NodeJS.Timeout | null = null;
  private onDuckDBShutdown?: () => void;

  constructor({
    fileManager,
    logger,
    onEvent,
    options,
    instanceManager,
    onDuckDBShutdown,
  }: DBMConstructorOptions) {
    this.fileManager = fileManager;
    this.logger = logger;
    this.onEvent = onEvent;
    this.options = options;
    this.instanceManager = instanceManager;
    this.onDuckDBShutdown = onDuckDBShutdown;
  }

  private async _shutdown() {
    if (this.connection) {
      await this.connection.close();
      this.connection = null;
    }
    this.logger.debug('Shutting down the DB');
    if (this.onDuckDBShutdown) {
      this.onDuckDBShutdown();
    }
    await this.fileManager.onDBShutdownHandler();
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

  private async _queryWithTableNames(
    query: string,
    tableNames: string[],
    options?: QueryOptions
  ) {
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
      metadata: options?.metadata,
    });

    const tablesFileData = await this.fileManager.getFilesNameForTables(
      tableNames
    );

    /**
     * Execute the preQuery hook
     */
    if (options?.preQuery) {
      await options.preQuery(tablesFileData);
    }

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
      metadata: options?.metadata,
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
  private async _startQueryExecution(metadata?: object) {
    this.logger.debug('Query queue length:', this.queriesQueue.length);

    this._emitEvent({
      event_name: 'query_queue_length',
      value: this.queriesQueue.length,
      metadata,
    });

    /**
     * Get the first query
     */
    const queueElement = this.queriesQueue.shift();
    /**
     * If there is no query, stop the queue
     */
    if (!queueElement) {
      await this._stopQueryQueue();
      return;
    }

    try {
      const startTime = Date.now();
      this.logger.debug(
        'Time since query was added to the queue:',
        startTime - queueElement.timestamp,
        'ms',
        queueElement.query
      );

      this._emitEvent({
        event_name: 'query_queue_duration',
        duration: startTime - queueElement.timestamp,
        metadata,
      });

      /**
       * Execute the query
       */
      const result = await this._queryWithTableNames(
        queueElement.query,
        queueElement.tableNames,
        queueElement.options
      );
      const endTime = Date.now();

      this.logger.debug(
        'Total time spent along with queue time',
        endTime - queueElement.timestamp,
        'ms',
        queueElement.query
      );
      /**
       * Resolve the promise
       */
      queueElement.promise.resolve(result);
    } catch (error) {
      this.logger.warn('Error while executing query:', queueElement.query);
      /**
       * Reject the promise, so the caller can catch the error
       */
      queueElement.promise.reject(error);
    }

    /**
     * Start the next query
     */
    this._startQueryExecution(queueElement.options?.metadata);
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

  public async queryWithTableNames(
    query: string,
    tableNames: string[],
    options?: QueryOptions
  ) {
    const promise = new Promise((resolve, reject) => {
      this.queriesQueue.push({
        query,
        tableNames,
        promise: {
          resolve,
          reject,
        },
        timestamp: Date.now(),
        options,
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
