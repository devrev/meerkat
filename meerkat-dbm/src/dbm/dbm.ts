import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { v4 as uuidv4 } from 'uuid';

import { FileManagerType } from '../file-manager/file-manager-type';
import { DBMEvent, DBMLogger } from '../logger';
import { InstanceManagerType } from './instance-manager';
import {
  DBMConstructorOptions,
  QueryOptions,
  QueryQueueItem,
  TableConfig,
  TableLock,
} from './types';

export class DBM {
  private fileManager: FileManagerType;
  private instanceManager: InstanceManagerType;
  private connection: AsyncDuckDBConnection | null = null;
  private queriesQueue: QueryQueueItem[] = [];
  private tableLockRegistry: Record<string, TableLock> = {};

  private logger: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;
  private options: DBMConstructorOptions['options'];
  private terminateDBTimeout: NodeJS.Timeout | null = null;
  private onDuckDBShutdown?: () => void;

  private queryQueueRunning = false;
  private shutdownLock = false;
  private currentQueryItem?: QueryQueueItem;

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
    /**
     * If the shutdown lock is true, then don't shutdown the DB
     */
    if (this.shutdownLock) {
      return;
    }

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

  async lockTables(tableNames: string[]): Promise<void> {
    const promises = [];

    for (const tableName of tableNames) {
      const tableLock = this.tableLockRegistry[tableName];

      // If the table lock doesn't exist, create a new lock
      if (!tableLock) {
        this.tableLockRegistry[tableName] = {
          isLocked: true,
          promiseQueue: [],
        };
        continue;
      }

      // If the table is already locked, add the promise to the queue
      if (tableLock.isLocked) {
        const promise = new Promise<void>((resolve, reject) => {
          tableLock.promiseQueue.push({ reject, resolve });
        });
        promises.push(promise);
      }

      // Set the table as locked
      tableLock.isLocked = true;
    }

    // Wait for all promises to resolve (locks to be acquired)
    await Promise.all(promises);
  }

  async unlockTables(tableNames: string[]): Promise<void> {
    for (const tableName of tableNames) {
      const tableLock = this.tableLockRegistry[tableName];

      // If the table lock doesn't exist, create a new lock
      if (!tableLock) {
        this.tableLockRegistry[tableName] = {
          isLocked: false,
          promiseQueue: [],
        };
      }

      const nextPromiseInQueue = tableLock?.promiseQueue?.shift();

      // If there is a promise in the queue, resolve it and keep the table as locked
      if (nextPromiseInQueue) {
        tableLock.isLocked = true;
        nextPromiseInQueue.resolve();
      } else {
        // If there are no promises in the queue, set the table as unlocked
        tableLock.isLocked = false;
      }
    }
  }

  isTableLocked(tableName: string): boolean {
    return this.tableLockRegistry[tableName]?.isLocked ?? false;
  }

  private async _queryWithTables(
    query: string,
    tables: TableConfig[],
    options?: QueryOptions
  ) {
    /**
     * Load all the files into the database
     */
    const startMountTime = Date.now();
    await this.fileManager.mountFileBufferByTables(tables);
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

    const tablesFileData = await this.fileManager.getFilesNameForTables(tables);

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
  private async _startQueryExecution(index: number, metadata?: object) {
    this.logger.debug('Query queue length:', this.queriesQueue.length);

    this._emitEvent({
      event_name: 'query_queue_length',
      value: this.queriesQueue.length,
      metadata,
    });

    /**
     * Get the first query
     */
    const currentQueryItem = this.queriesQueue[index];

    /**
     * If there is no query, stop the queue
     */
    if (!currentQueryItem) {
      await this._stopQueryQueue();
      return;
    }

    try {
      /**
       * Lock the tables
       */
      await this.lockTables(currentQueryItem.tables.map((table) => table.name));

      const startTime = Date.now();
      this.logger.debug(
        'Time since query was added to the queue:',
        startTime - currentQueryItem.timestamp,
        'ms',
        currentQueryItem.query
      );

      this._emitEvent({
        event_name: 'query_queue_duration',
        duration: startTime - currentQueryItem.timestamp,
        metadata,
      });

      /**
       * Execute the query
       */
      const result = await this._queryWithTables(
        currentQueryItem.query,
        currentQueryItem.tables,
        currentQueryItem.options
      );
      const endTime = Date.now();

      this.logger.debug(
        'Total time spent along with queue time',
        endTime - currentQueryItem.timestamp,
        'ms',
        currentQueryItem.query
      );
      /**
       * Resolve the promise
       */
      currentQueryItem.promise.resolve(result);
    } catch (error) {
      this.logger.warn('Error while executing query:', currentQueryItem?.query);
      /**
       * Reject the promise, so the caller can catch the error
       */
      currentQueryItem?.promise.reject(error);
    } finally {
      /**
       * Unlock the tables
       */
      await this.unlockTables(
        currentQueryItem.tables.map((table) => table.name)
      );
    }

    /**
     * Clear the current query item
     */
    this.currentQueryItem = undefined;

    /**
     * Start the next query
     */
    // this._startQueryExecution();
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
    for (let i = 0; i < this.queriesQueue.length; i++) {
      this._startQueryExecution(i);
    }
  }

  public getQueueLength() {
    return this.queriesQueue.length;
  }

  public isQueryRunning() {
    return this.queryQueueRunning;
  }

  private _signalListener(
    connectionId: string,
    reject: (reason?: any) => void,
    signal?: AbortSignal
  ): void {
    const signalHandler = async (reason: Event) => {
      const indexToRemove = this.queriesQueue.findIndex(
        (queryItem) => queryItem.connectionId === connectionId
      );

      // Remove the item at the found index
      if (indexToRemove !== -1) {
        this.queriesQueue.splice(indexToRemove, 1);
      }

      /**
       * Check if the current executing query is the one that was aborted
       * If yes, then cancel the query
       */
      if (this.currentQueryItem?.connectionId === connectionId) {
        const connection = await this._getConnection();

        await connection.cancelSent();
      }

      signal?.removeEventListener('abort', signalHandler);

      reject(reason);
    };

    signal?.addEventListener('abort', signalHandler);
  }

  public async queryWithTables({
    query,
    tables,
    options,
  }: {
    query: string;
    tables: TableConfig[];
    options?: QueryOptions;
  }) {
    const connectionId = uuidv4();

    const promise = new Promise((resolve, reject) => {
      this._signalListener(connectionId, reject, options?.signal);

      this.queriesQueue.push({
        query,
        tables,
        promise: {
          resolve,
          reject,
        },
        connectionId,
        timestamp: Date.now(),
        options,
      });
    });

    this._startQueryQueue();
    return promise;
  }

  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  //@ts-ignore
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

  /**
   * Set the shutdown lock to prevent the DB from shutting down
   */
  public async setShutdownLock(state: boolean) {
    this.shutdownLock = state;
  }
}
