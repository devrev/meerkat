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
    this.iFrameRunnerManager.stopRunners();
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
      this.activeNumberOfQueries++;
      this.iFrameRunnerManager.startRunners();
      const runners = this.iFrameRunnerManager.getRunnerIds();
      this.counter = roundRobin(this.counter, runners.length - 1).counter;

      const runner = this.iFrameRunnerManager.iFrameManagers.get(
        runners[this.counter]
      );

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
      if (response.message.isError) {
        throw response.message.error;
      }
      return response.message.data;
    } catch (error) {
      this.logger.error('Error while executing query', error);
      throw error;
    } finally {
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
