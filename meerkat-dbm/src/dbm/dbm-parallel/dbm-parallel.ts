import { FileManagerType } from '../../file-manager/file-manager-type';
import { DBMEvent, DBMLogger } from '../../logger';
import {
  BROWSER_RUNNER_TYPE,
  BrowserRunnerExecQueryMessageResponse,
} from '../../window-communication/runner-types';
import { DBMConstructorOptions, QueryOptions, TableConfig } from '../types';
import { IFrameRunnerManager } from './runner-manager';

//Round Robin for multiple runners like 10
const roundRobin = (
  counter: number,
  maxValue: number
): {
  counter: number;
} => {
  if (counter === maxValue) {
    return {
      counter: 0,
    };
  }

  return {
    counter: counter + 1,
  };
};

export class DBMParallel {
  private fileManager: FileManagerType;
  private logger: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;
  private options: DBMConstructorOptions['options'];
  private onDuckDBShutdown?: () => void;
  private iFrameRunnerManager: IFrameRunnerManager;
  private counter = 0;
  private terminateDBTimeout: NodeJS.Timeout | null = null;
  private activeNumberOfQueries = 0;

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

  public async queryWithTables({
    query,
    tables,
    options,
  }: {
    query: string;
    tables: TableConfig[];
    options?: QueryOptions;
  }) {
    try {
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
      this.counter = roundRobin(this.counter, runners.length - 1).counter;

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

      const response =
        await runner.communication.sendRequest<BrowserRunnerExecQueryMessageResponse>(
          {
            type: BROWSER_RUNNER_TYPE.EXEC_QUERY,
            payload: {
              query,
              tables,
              options,
            },
          }
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
      this.logger.error('Error while executing query', error);
      throw error;
    } finally {
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
