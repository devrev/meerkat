import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { Table } from 'apache-arrow/table';
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

export class DBMApp {
  private fileManager: FileManagerType;
  private instanceManager: InstanceManagerType;
  private connection: AsyncDuckDBConnection | null = null;
  private queriesQueue: QueryQueueItem[] = [];
  private tableLockRegistry: Record<string, TableLock> = {};
  private windowApi: any;
  private logger: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;
  private options: DBMConstructorOptions['options'];
  private terminateDBTimeout: NodeJS.Timeout | null = null;
  private onDuckDBShutdown?: () => void;
  private onCreateConnection?: (
    connection: AsyncDuckDBConnection
  ) => void | Promise<void>;

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
    onCreateConnection,
    windowApi,
  }: DBMConstructorOptions) {
    this.fileManager = fileManager;
    this.logger = logger;
    this.onEvent = onEvent;
    this.options = options;
    this.instanceManager = instanceManager;
    this.onDuckDBShutdown = onDuckDBShutdown;
    this.onCreateConnection = onCreateConnection;
    this.windowApi = windowApi;
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
      if (this.onCreateConnection) {
        await this.onCreateConnection(this.connection);
      }
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

    const response = await this.windowApi.ipcRenderer.invoke('execute-query', {
      query,
      tables,
      options,
    });
    console.log('response', response.message.data);

    return response;
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
    this.currentQueryItem = this.queriesQueue.shift();

    /**
     * If there is no query, stop the queue
     */
    if (!this.currentQueryItem) {
      await this._stopQueryQueue();
      return;
    }

    try {
      /**
       * Execute the query
       */
      const result = await this._queryWithTables(
        this.currentQueryItem.query,
        this.currentQueryItem.tables,
        this.currentQueryItem.options
      );
      const endTime = Date.now();

      this.logger.debug(
        'Total time spent along with queue time',
        endTime - this.currentQueryItem.timestamp,
        'ms',
        this.currentQueryItem.query
      );
      /**
       * Resolve the promise
       */
      this.currentQueryItem.promise.resolve(result);
    } catch (error) {
      this.logger.warn(
        'Error while executing query:',
        this.currentQueryItem?.query
      );
      /**
       * Reject the promise, so the caller can catch the error
       */
      this.currentQueryItem?.promise.reject(error);
    } finally {
      /**
       * Unlock the tables
       */
    }

    /**
     * Clear the current query item
     */
    this.currentQueryItem = undefined;

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

    const response = await this.windowApi.ipcRenderer.invoke('execute-query', {
      query,
      tables,
      options,
    });
    console.log('response', response.message.data);
    this._startQueryQueue();
    return response;
  }

  async query(query: string): Promise<Table<any>> {
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
