import { FileBufferStore } from '../../file-manager/file-manager-type';
import { DBMEvent, DBMLogger } from '../../logger';
import { TableWiseFiles } from '../../types';
import { BROWSER_RUNNER_TYPE, BrowserRunnerMessage } from '../../window-communication/runner-types';
import { WindowMessage } from '../../window-communication/window-communication';
import { TableConfig } from '../types';
import { IFrameManager } from './iframe-manager';

export interface IFrameRunnerManagerConstructor {
  runnerURL: string;
  origin: string;
  fetchTableFileBuffers: (tables: TableConfig[]) => Promise<FileBufferStore<SharedArrayBuffer>[]>;
  fetchPreQuery: (runnerId: string, tableWiseFiles: TableWiseFiles[]) => string[];
  totalRunners: number;
  logger: DBMLogger;
  onEvent?: (event: DBMEvent) => void;
}

/**
 * This class is responsible for managing the iframe runners.
 * Some of the responsibilities include:
 * - Creating iframe runners
 * - Sending messages to iframe runners
 * - Listening to messages from iframe runners
 * - Destroying iframe runners
 * - Managing the state of iframe runners
 * - Managing the communication between the main thread and iframe runners
 */

export class IFrameRunnerManager {
  iFrameManagers: Map<string, IFrameManager> = new Map();
  private totalRunners: number;
  private iFrameReadyMap: Map<string, boolean> = new Map();
  private resolvePromises: ((value: unknown) => void)[] = [];
  private origin: string;
  private runnerURL: string;
  private logger: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;

  private fetchTableFileBuffers: (tables: TableConfig[]) => Promise<FileBufferStore<SharedArrayBuffer>[]>;
  private fetchPreQuery: (runnerId: string, tableWiseFiles: TableWiseFiles[]) => string[];

  constructor({
    runnerURL,
    origin,
    fetchTableFileBuffers,
    totalRunners = 2,
    logger,
    onEvent,
    fetchPreQuery,
  }: IFrameRunnerManagerConstructor) {
    this.logger = logger;
    this.onEvent = onEvent;
    this.runnerURL = runnerURL;
    this.origin = origin;
    this.totalRunners = totalRunners;
    this.fetchTableFileBuffers = fetchTableFileBuffers;
    this.fetchPreQuery = fetchPreQuery;
  }

  private addIFrameManager(uuid: string) {
    this.iFrameReadyMap.set(uuid, false);
    this.iFrameManagers.set(
      uuid,
      new IFrameManager({
        onMessage: this.messageListener.bind(this),
        origin: this.origin,
        runnerURL: this.runnerURL,
        uuid,
      }),
    );
  }

  private areRunnersRunning() {
    /**
     * Check if totalRunners are already created
     */
    return this.iFrameManagers.size === this.totalRunners;
  }

  public stopRunners() {
    for (const [key, value] of this.iFrameManagers) {
      value.destroy();
      this.iFrameManagers.delete(key);
    }
    this.iFrameReadyMap.clear();
  }

  public startRunners() {
    if (this.areRunnersRunning()) {
      return;
    }
    for (let i = 0; i < this.totalRunners; i++) {
      this.addIFrameManager(i.toString());
    }
  }

  public getRunnerIds() {
    return Array.from(this.iFrameManagers.keys());
  }

  /**
   * A promise that resolves when all the iframes are ready
   */
  public async isFrameRunnerReady() {
    if (Array.from(this.iFrameReadyMap.values()).every((value) => value)) {
      this.logger.info('All iframes are ready');

      return true;
    }
    const promiseObj = new Promise((resolve) => {
      this.resolvePromises.push(resolve);
    });

    return promiseObj;
  }

  private async messageListener(runnerId: string, message: WindowMessage<BrowserRunnerMessage>) {
    switch (message.message.type) {
      case BROWSER_RUNNER_TYPE.RUNNER_GET_FILE_BUFFERS:
        if (this.fetchTableFileBuffers) {
          this.fetchTableFileBuffers(message.message.payload.tables).then((result) => {
            const manager = this.iFrameManagers.get(runnerId);
            if (!manager) {
              return;
            }
            manager.communication.sendResponse(message.uuid, result);
          });
        }
        break;

      case BROWSER_RUNNER_TYPE.RUNNER_ON_READY: {
        this.iFrameReadyMap.set(runnerId, true);

        /**
         * If all the iframes are ready, resolve all the promises
         */
        if (Array.from(this.iFrameReadyMap.values()).every((value) => value)) {
          if (this.resolvePromises.length > 0) {
            this.resolvePromises.forEach((resolve) => resolve(true));
            this.resolvePromises = [];
          }
        }
        break;
      }

      case BROWSER_RUNNER_TYPE.RUNNER_ON_EVENT:
        if (this.onEvent) {
          this.onEvent(message.message.payload);
        }
        break;

      case BROWSER_RUNNER_TYPE.RUNNER_PRE_QUERY: {
        if (this.fetchPreQuery) {
          const manager = this.iFrameManagers.get(runnerId);
          if (!manager) {
            return;
          }

          const { tableWiseFiles } = message.message.payload;

          const preQueries = await this.fetchPreQuery(runnerId, tableWiseFiles);
          console.log('preQueries', preQueries);
          manager.communication.sendResponse(message.uuid, preQueries);
        }

        break;
      }

      default:
        break;
    }
  }
}
