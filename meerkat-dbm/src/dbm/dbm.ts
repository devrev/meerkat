import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { Table } from 'apache-arrow/table';
import uniqBy from 'lodash/uniqBy';
import { v4 as uuidv4 } from 'uuid';
import { FileManagerType } from '../file-manager';
import { DBMEvent, DBMLogger } from '../logger';
import { InstanceManagerType } from './instance-manager';
import { TableLockManager } from './table-lock-manager';
import {
  DBMConstructorOptions,
  QueryOptions,
  QueryQueueItem,
  TableConfig,
} from './types';

export class DBM extends TableLockManager {
  private fileManager: FileManagerType;
  private instanceManager: InstanceManagerType;
  private connection: AsyncDuckDBConnection | null = null;
  private queriesQueue: QueryQueueItem[] = [];

  private logger: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;
  private options: DBMConstructorOptions['options'];
  private terminateDBTimeout: NodeJS.Timeout | null = null;
  private recycleDBTimeout: NodeJS.Timeout | null = null;
  // Ensures the intermediate recycle fires at most once per idle period.
  private recycledThisIdle = false;
  // Set while a recycle/shutdown teardown is mid-flight. Queries acquiring a
  // connection wait on this so they never bind to an instance that teardown is
  // about to terminate. Cleared when the teardown that set it completes.
  private teardownInProgress: Promise<void> | null = null;
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
  }: DBMConstructorOptions) {
    super();

    this.fileManager = fileManager;
    this.logger = logger;
    this.onEvent = onEvent;
    this.options = options;
    this.instanceManager = instanceManager;
    this.onDuckDBShutdown = onDuckDBShutdown;
    this.onCreateConnection = onCreateConnection;

    this._validateIdleOptions();
  }

  /**
   * Fail fast on a misconfigured idle policy. The recycle is the early action
   * and the shutdown is the late one, so when both are set the recycle must
   * fire strictly before the shutdown. Otherwise the shutdown's terminateDB
   * would run first and a still-pending recycle would re-instantiate the
   * engine we just tore down.
   */
  private _validateIdleOptions() {
    const recycle = this.options?.recycleInactiveTime;
    const shutdown = this.options?.shutdownInactiveTime;
    if (
      recycle !== undefined &&
      shutdown !== undefined &&
      recycle >= shutdown
    ) {
      throw new Error(
        `Invalid idle options: recycleInactiveTime (${recycle}ms) must be less than shutdownInactiveTime (${shutdown}ms).`
      );
    }
  }

  /**
   * Run a teardown (recycle/shutdown) body under a barrier so a query that
   * arrives mid-teardown waits in _getConnection instead of binding to an
   * instance that is about to be terminated. The body's synchronous prefix runs
   * before this records the promise, but it contains no await, so no query can
   * interleave before the barrier is in place.
   */
  private _runTeardown(body: () => Promise<void>): Promise<void> {
    // body() runs synchronously up to its first await before returning, but it
    // has no synchronous-prefix await that touches the connection, so recording
    // the barrier here still happens before any query can interleave.
    const promise = body();
    const barrier = promise.catch(() => undefined);
    this.teardownInProgress = barrier;
    barrier.then(() => {
      // Only clear if this teardown is still the active one — a later teardown
      // may have replaced the barrier in the meantime.
      if (this.teardownInProgress === barrier) {
        this.teardownInProgress = null;
      }
    });
    return promise;
  }

  private async _shutdown() {
    /**
     * If the shutdown lock is true, then don't shutdown the DB
     */
    if (this.shutdownLock) {
      return;
    }

    // A shutdown supersedes any pending recycle — clear it so it can't fire
    // afterwards and re-instantiate the engine we are about to terminate.
    if (this.recycleDBTimeout) {
      clearTimeout(this.recycleDBTimeout);
      this.recycleDBTimeout = null;
    }

    await this._runTeardown(async () => {
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
    });
  }

  /**
   * Tear down the engine and immediately re-instantiate it. Reclaims the
   * worker's high-water memory while leaving a warm engine for the next query.
   * Mirrors _shutdown()'s cleanup so the consumer's caches stay consistent,
   * then eagerly re-acquires the DB so the cold instantiate is paid now (during
   * idle) rather than on the next user-facing query.
   */
  /**
   * True while any query is pending or executing. The queue is drained by
   * _startQueryExecution shifting an item off before it runs, so a length of 0
   * does not by itself mean idle — check the running flag and current item too.
   */
  private _isBusy() {
    return (
      this.queriesQueue.length > 0 ||
      this.queryQueueRunning ||
      this.currentQueryItem !== undefined
    );
  }

  private async _recycle() {
    if (this.shutdownLock) {
      return;
    }
    await this._runTeardown(async () => {
      if (this.connection) {
        await this.connection.close();
        this.connection = null;
      }
      this.logger.debug('Recycling the DB (idle teardown + restart)');
      if (this.onDuckDBShutdown) {
        this.onDuckDBShutdown();
      }
      await this.fileManager.onDBShutdownHandler();
      await this.instanceManager.terminateDB();
      // Re-instantiate eagerly so the next query hits a warm engine.
      await this.instanceManager.getDB();
    });
  }

  /**
   * Arm the idle timers when the query queue drains. Two independent timers:
   *  - recycleInactiveTime (optional, earlier): teardown + restart ONCE, gated
   *    by shouldRecycle(). Reclaims memory without losing the warm engine.
   *  - shutdownInactiveTime (later): full shutdown, no restart.
   * Both re-arm on each queue-drain; recycle is throttled to once per idle
   * period via recycledThisIdle (reset whenever new activity re-arms timers).
   */
  private _startShutdownInactiveTimer() {
    this.recycledThisIdle = false;

    if (this.recycleDBTimeout) {
      clearTimeout(this.recycleDBTimeout);
      this.recycleDBTimeout = null;
    }
    if (this.terminateDBTimeout) {
      clearTimeout(this.terminateDBTimeout);
      this.terminateDBTimeout = null;
    }

    if (this.options?.recycleInactiveTime) {
      this.recycleDBTimeout = setTimeout(async () => {
        // Still idle? A running queue means a query was shifted off the queue
        // and is executing, so queriesQueue.length alone is not enough.
        if (this._isBusy() || this.recycledThisIdle) {
          return;
        }
        // Consult the consumer-supplied gate (defaults to always-recycle).
        const shouldRecycle = this.options?.shouldRecycle
          ? await this.options.shouldRecycle()
          : true;
        if (!shouldRecycle) {
          return;
        }
        // Re-check idle after the (possibly async) gate resolved — a query may
        // have arrived and started executing while shouldRecycle() awaited.
        if (this._isBusy() || this.recycledThisIdle) {
          return;
        }
        this.recycledThisIdle = true;
        await this._recycle();
      }, this.options.recycleInactiveTime);
    }

    if (this.options?.shutdownInactiveTime) {
      this.terminateDBTimeout = setTimeout(async () => {
        /**
         * Check if there is any query in the queue
         */
        if (this.queriesQueue.length > 0) {
          this.logger.debug(
            'Query queue is not empty, not shutting down the DB'
          );
          return;
        }
        await this._shutdown();
      }, this.options.shutdownInactiveTime);
    }
  }

  private _emitEvent(event: DBMEvent) {
    if (this.onEvent) {
      this.onEvent(event);
    }
  }

  private async _getConnection() {
    // Wait out any in-flight teardown so we never bind a connection to an
    // instance that recycle/shutdown is about to terminate. Loop because a new
    // teardown can begin while we awaited the previous one.
    while (this.teardownInProgress) {
      await this.teardownInProgress;
    }
    if (!this.connection) {
      const db = await this.instanceManager.getDB();
      this.connection = await db.connect();
      if (this.onCreateConnection) {
        await this.onCreateConnection(this.connection);
      }
    }
    return this.connection;
  }

  private async _queryWithTables(
    query: string,
    tables: TableConfig[],
    options?: QueryOptions
  ) {
    // Wait out any in-flight teardown before touching the DB. mountFileBuffer
    // registers buffers into the instance via getDB(), so it must not run
    // against an instance recycle/shutdown is about to terminate.
    while (this.teardownInProgress) {
      await this.teardownInProgress;
    }

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
       * Lock the tables
       */
      await this.lockTables(
        this.currentQueryItem.tables.map((table) => table.name)
      );

      const startTime = Date.now();
      this.logger.debug(
        'Time since query was added to the queue:',
        startTime - this.currentQueryItem.timestamp,
        'ms',
        this.currentQueryItem.query
      );

      this._emitEvent({
        event_name: 'query_queue_duration',
        duration: startTime - this.currentQueryItem.timestamp,
        metadata,
      });

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
      await this.unlockTables(
        this.currentQueryItem.tables.map((table) => table.name)
      );
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
    tables: _tables,
    options,
  }: {
    query: string;
    tables: TableConfig[];
    options?: QueryOptions;
  }) {
    const connectionId = uuidv4();

    // Deduplicate tables by name
    const tables = uniqBy(_tables, 'name');

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
