import { FileManagerType } from '../../file-manager/file-manager-type';
import { DBMEvent, DBMLogger } from '../../logger';
import {
  BROWSER_RUNNER_TYPE,
  BrowserRunnerExecQueryMessage,
} from '../../window-communication/runner-types';
import { DBMConstructorOptions, QueryOptions, TableConfig } from '../types';
import { IFrameRunnerManager } from './runner-manager';

export class DBMParallel {
  private fileManager: FileManagerType;
  private logger: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;
  private options: DBMConstructorOptions['options'];
  private onDuckDBShutdown?: () => void;
  iFrameRunnerManager: IFrameRunnerManager;
  counter = 0;

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

  public async queryWithTables({
    query,
    tables,
    options,
  }: {
    query: string;
    tables: TableConfig[];
    options?: QueryOptions;
  }) {
    this.counter++;
    console.info(
      'Sending query to runner',
      this.counter,
      this.counter % 2,
      this.counter % 2 === 0 ? '1' : '2',
      query
    );
    const runner = this.iFrameRunnerManager.iFrameManagers.get(
      this.counter % 2 === 0 ? '1' : '2'
    );
    await this.iFrameRunnerManager.isFrameRunnerReady();

    if (!runner) {
      throw new Error('No runner found');
    }

    return runner.communication.sendRequest<BrowserRunnerExecQueryMessage>({
      type: BROWSER_RUNNER_TYPE.EXEC_QUERY,
      payload: {
        query,
        tables,
        options,
      },
    });
  }

  public async query(query: string) {
    //no-ops
  }
}
