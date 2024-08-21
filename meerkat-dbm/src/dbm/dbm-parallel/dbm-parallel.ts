import { FileManagerType } from '../../file-manager/file-manager-type';
import { DBMEvent, DBMLogger } from '../../logger';
import {
  BROWSER_RUNNER_TYPE,
  BrowserRunnerExecQueryMessage,
} from '../../window-communication/runner-types';
import { DBMConstructorOptions, QueryOptions, TableConfig } from '../types';
import { IFrameManager } from './iframe-manager';

export class DBMParallel {
  private fileManager: FileManagerType;
  private logger: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;
  private options: DBMConstructorOptions['options'];
  private onDuckDBShutdown?: () => void;
  iFrameManagers: IFrameManager[];

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
    this.onDuckDBShutdown = onDuckDBShutdown;
    this.iFrameManagers = [];
    this.iFrameManagers.push(new IFrameManager('1'));

    this.iFrameManagers[0].communication.onMessage((message) => {
      switch (message.message.type) {
        case BROWSER_RUNNER_TYPE.RUNNER_GET_FILE_BUFFERS:
          if (this.fileManager.getTableBufferData) {
            this.fileManager
              .getTableBufferData(message.message.payload.tables)
              .then((result) => {
                this.iFrameManagers[0].communication.sendResponse(
                  message.uuid,
                  result
                );
              });
          }

          break;
        default:
          break;
      }
    });
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
    return this.iFrameManagers[0].communication.sendRequest<BrowserRunnerExecQueryMessage>(
      {
        type: BROWSER_RUNNER_TYPE.EXEC_QUERY,
        payload: {
          query,
          tables,
          options,
        },
      }
    );
  }

  public async query(query: string) {
    //no-ops
  }
}
