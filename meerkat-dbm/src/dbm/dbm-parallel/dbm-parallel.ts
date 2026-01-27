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
  }

  private async _shutdown() {
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
      if (this.activeNumberOfQueries > 0) {
        this.logger.debug('Query queue is not empty, not shutting down the DB');
        return;
      }
      await this._shutdown();
    }, this.options.shutdownInactiveTime);
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
          type: BROWSER_RUNNER_TYPE.CANCEL_QUERY,
          payload: { queryId },
        });
      }

      reject(new Error('Query aborted by user'));
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
      // If the query is aborted, don't log the error
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
