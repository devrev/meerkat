import uniqBy from 'lodash/uniqBy';
import { v4 as uuidv4 } from 'uuid';
import { FileManagerType } from '../../file-manager/file-manager-type';
import { DBMEvent, DBMLogger } from '../../logger';
import {
  BROWSER_RUNNER_TYPE,
  BrowserRunnerExecQueryMessageResponse,
} from '../../window-communication/runner-types';
import { TableLockManager } from '../table-lock-manager';
import { DBMConstructorOptions, QueryOptions, TableConfig } from '../types';
import { IFrameRunnerManager } from './runner-manager';

//Round Robin for multiple runners like 10
const roundRobin = (counter: number, maxValue: number): number => {
  if (counter === maxValue) {
    return 0;
  }

  return counter + 1;
};

export class DBMParallel extends TableLockManager {
  private fileManager: FileManagerType;
  private logger: DBMLogger;

  private onEvent?: (event: DBMEvent) => void;
  private options: DBMConstructorOptions['options'];
  private onDuckDBShutdown?: () => void;
  private iFrameRunnerManager: IFrameRunnerManager;
  private counter = 0;
  private terminateDBTimeout: NodeJS.Timeout | null = null;
  private recycleDBTimeout: NodeJS.Timeout | null = null;
  // Ensures the intermediate recycle fires at most once per idle period.
  private recycledThisIdle = false;
  // Set while a recycle/shutdown teardown is mid-flight. Queries wait on this
  // so they never start runners that teardown is about to stop. Cleared when
  // the teardown that set it completes.
  private teardownInProgress: Promise<void> | null = null;
  private activeNumberOfQueries = 0;
  private activeQueries: Map<
    string,
    {
      runnerId: string;
      signal?: AbortSignal;
      abortHandler?: () => void;
    }
  > = new Map();

  constructor({
    fileManager,
    logger,
    onEvent,
    options,
    instanceManager,
    onDuckDBShutdown,
    iFrameRunnerManager,
  }: DBMConstructorOptions & {
    iFrameRunnerManager: IFrameRunnerManager;
  }) {
    super();

    this.fileManager = fileManager;
    this.logger = logger;
    this.onEvent = onEvent;
    this.options = options;
    this.onDuckDBShutdown = onDuckDBShutdown;
    this.iFrameRunnerManager = iFrameRunnerManager;

    this._validateIdleOptions();
  }

  /**
   * Fail fast on a misconfigured idle policy. The recycle is the early action
   * and the shutdown is the late one, so when both are set the recycle must
   * fire strictly before the shutdown. Otherwise the shutdown's teardown would
   * run first and a still-pending recycle would restart the runners we just
   * stopped.
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
   * arrives mid-teardown waits in queryWithTables instead of starting runners
   * that teardown is about to stop. body()'s synchronous prefix runs before the
   * barrier is recorded, but it has no synchronous-prefix await, so no query can
   * interleave before the barrier is in place.
   */
  private _runTeardown(body: () => Promise<void>): Promise<void> {
    const promise = body();
    const barrier = promise.catch(() => undefined);
    this.teardownInProgress = barrier;
    barrier.then(() => {
      if (this.teardownInProgress === barrier) {
        this.teardownInProgress = null;
      }
    });
    return promise;
  }

  private async _shutdown() {
    // A shutdown supersedes any pending recycle — clear it so it can't fire
    // afterwards and restart the runners we are about to stop.
    if (this.recycleDBTimeout) {
      clearTimeout(this.recycleDBTimeout);
      this.recycleDBTimeout = null;
    }
    await this._runTeardown(async () => {
      this.logger.debug('Shutting down the DB');
      if (this.onDuckDBShutdown) {
        this.onDuckDBShutdown();
      }
      /**
       * This will remove all the iframes
       */
      this.iFrameRunnerManager.stopRunners();
      /**
       * This will remove all the file buffers from main thread
       */
      await this.fileManager.onDBShutdownHandler();
    });
  }

  /**
   * Tear down the runners and immediately restart them — reclaims iframe/worker
   * memory while keeping the engine warm for the next query. Mirrors _shutdown()
   * cleanup, then re-starts the runners eagerly.
   */
  private async _recycle() {
    await this._runTeardown(async () => {
      this.logger.debug('Recycling the DB (idle teardown + restart)');
      if (this.onDuckDBShutdown) {
        this.onDuckDBShutdown();
      }
      this.iFrameRunnerManager.stopRunners();
      await this.fileManager.onDBShutdownHandler();
      // Re-create the runners eagerly so the next query hits warm iframes.
      this.iFrameRunnerManager.startRunners();
    });
  }

  /**
   * Arm the idle timers when no queries are active. Two independent timers:
   *  - recycleInactiveTime (optional, earlier): teardown + restart ONCE, gated
   *    by shouldRecycle(). Reclaims memory without losing the warm engine.
   *  - shutdownInactiveTime (later): full shutdown, no restart.
   * Recycle is throttled to once per idle period via recycledThisIdle.
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
        if (this.activeNumberOfQueries > 0 || this.recycledThisIdle) {
          return;
        }
        const shouldRecycle = this.options?.shouldRecycle
          ? await this.options.shouldRecycle()
          : true;
        if (!shouldRecycle) {
          return;
        }
        if (this.activeNumberOfQueries > 0 || this.recycledThisIdle) {
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
        if (this.activeNumberOfQueries > 0) {
          this.logger.debug(
            'Query queue is not empty, not shutting down the DB'
          );
          return;
        }
        await this._shutdown();
      }, this.options.shutdownInactiveTime);
    }
  }

  private _signalListener(
    queryId: string,
    runnerId: string,
    reject: (reason?: Error) => void,
    signal?: AbortSignal
  ): void {
    if (!signal) return;

    const abortHandler = () => {
      const runner = this.iFrameRunnerManager.iFrameManagers.get(runnerId);

      if (runner) {
        // Send cancel message to iframe
        runner.communication.sendRequestWithoutResponse({
          payload: { queryId },
          type: BROWSER_RUNNER_TYPE.CANCEL_QUERY,
        });
      }

      // Clean up tracking
      this.activeQueries.delete(queryId);
      signal.removeEventListener('abort', abortHandler);

      reject(new DOMException('Query aborted by user', 'AbortError'));
    };

    signal.addEventListener('abort', abortHandler);

    // Store the handler for cleanup
    const queryInfo = this.activeQueries.get(queryId);
    if (queryInfo) {
      queryInfo.abortHandler = abortHandler;
    }
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
    const queryId = uuidv4();
    const signal = options?.signal;

    // Deduplicate tables by name
    const tables = uniqBy(_tables, 'name');

    try {
      const start = performance.now();
      /**
       * Tracking the number of active queries to shutdown the DB after inactivity
       */
      this.activeNumberOfQueries++;
      /**
       * Wait out any in-flight teardown so we don't start runners that recycle/
       * shutdown is about to stop. Loop because a new teardown can begin while
       * we awaited the previous one.
       */
      while (this.teardownInProgress) {
        await this.teardownInProgress;
      }
      /**
       * StartRunners will start the runners if they are not already running
       */
      this.iFrameRunnerManager.startRunners();

      /**
       * A simple round-robin to select the runner
       */
      const runners = this.iFrameRunnerManager.getRunnerIds();
      this.counter = roundRobin(this.counter, runners.length - 1);

      const runner = this.iFrameRunnerManager.iFrameManagers.get(
        runners[this.counter]
      );

      /**
       * StartRunners only initiates the runners, it does not guarantee that the runner is ready to accept the query
       * isFrameRunnerReady will wait until the runner is ready to accept the query
       */
      await this.iFrameRunnerManager.isFrameRunnerReady();

      if (!runner) {
        throw new Error('No runner found');
      }

      this.activeQueries.set(queryId, {
        runnerId: runners[this.counter],
        signal,
      });

      const abortPromise = new Promise<never>((_, reject) => {
        this._signalListener(queryId, runners[this.counter], reject, signal);
      });

      /**
       * Lock the tables
       */
      await this.lockTables(
        tables.map((table) => table.name),
        'read'
      );

      const responsePromise =
        runner.communication.sendRequest<BrowserRunnerExecQueryMessageResponse>(
          {
            type: BROWSER_RUNNER_TYPE.EXEC_QUERY,
            payload: {
              queryId,
              query,
              tables,
              options: {
                ...options,
                // Don't pass signal to iframe as it's not serializable
                signal: undefined,
              },
            },
          }
        );

      const response = await Promise.race([responsePromise, abortPromise]);

      const end = performance.now();
      this.logger.info(`Time to execute by DBM Parallel`, end - start);

      /**
       * Unlock the tables
       */
      this.unlockTables(
        tables.map((table) => table.name),
        'read'
      );

      /**
       * The implementation is based on postMessage API, so we don't have the ability to throw an error from the runner
       * We have to check the response and throw an error if isError is true
       */
      if (response.message.isError) {
        throw new Error(response.message.error);
      }

      return response.message.data;
    } catch (error) {
      // Only log the error if the query is not aborted
      if (!signal?.aborted) {
        this.logger.error('Error while executing query', error);
      }

      throw error;
    } finally {
      /**
       * Clean up abort signal listener
       */
      const queryInfo = this.activeQueries.get(queryId);
      if (queryInfo?.signal && queryInfo.abortHandler) {
        queryInfo.signal.removeEventListener('abort', queryInfo.abortHandler);
      }
      this.activeQueries.delete(queryId);

      /**
       * Stop the runner if there are no active queries
       */
      this.activeNumberOfQueries--;
      if (this.activeNumberOfQueries === 0) {
        this._startShutdownInactiveTimer();
      }
    }
  }

  public async query(query: string) {
    //no-ops
  }
}
