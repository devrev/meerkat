import { FileBufferStore } from '../../file-manager/file-manager-type';
import {
  BROWSER_RUNNER_TYPE,
  BrowserRunnerMessage,
} from '../../window-communication/runner-types';
import { WindowMessage } from '../../window-communication/window-communication';
import { TableConfig } from '../types';
import { IFrameManager } from './iframe-manager';

export class IFrameRunnerManager {
  iFrameManagers: Map<string, IFrameManager> = new Map();
  private fetchTableFileBuffers: (
    tables: TableConfig[]
  ) => Promise<FileBufferStore[]>;
  private totalRunners: number;
  private iFrameReadyMap: Map<string, boolean> = new Map();
  private resolvePromises: ((value: unknown) => void)[] = [];
  private origin: string;
  private runnerURL: string;

  constructor({
    runnerURL,
    origin,
    fetchTableFileBuffers,
    totalRunners = 2,
  }: {
    runnerURL: string;
    origin: string;
    fetchTableFileBuffers: (
      tables: TableConfig[]
    ) => Promise<FileBufferStore[]>;
    totalRunners: number;
  }) {
    this.runnerURL = runnerURL;
    this.origin = origin;
    this.totalRunners = totalRunners;
    this.fetchTableFileBuffers = fetchTableFileBuffers;
  }

  private addIFrameManager(uuid: string) {
    this.iFrameReadyMap.set(uuid, false);
    this.iFrameManagers.set(
      uuid,
      new IFrameManager({
        runnerURL: this.runnerURL,
        origin: this.origin,
        uuid,
        onMessage: this.messageListener.bind(this),
      })
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

  public async isFrameRunnerReady() {
    if (Array.from(this.iFrameReadyMap.values()).every((value) => value)) {
      console.info('All iframes are ready');

      return true;
    }
    const promiseObj = new Promise((resolve) => {
      this.resolvePromises.push(resolve);
    });

    return promiseObj;
  }

  private messageListener(
    runnerId: string,
    message: WindowMessage<BrowserRunnerMessage>
  ) {
    switch (message.message.type) {
      case BROWSER_RUNNER_TYPE.RUNNER_GET_FILE_BUFFERS:
        if (this.fetchTableFileBuffers) {
          console.info(
            'Fetching table file buffers',
            runnerId,
            message.message.payload.tables
          );
          this.fetchTableFileBuffers(message.message.payload.tables).then(
            (result) => {
              const manager = this.iFrameManagers.get(runnerId);
              if (!manager) {
                return;
              }
              console.info('Sending response', runnerId, result);
              manager.communication.sendResponse(message.uuid, result);
            }
          );
        }
        break;

      case BROWSER_RUNNER_TYPE.RUNNER_ON_READY: {
        console.info('Runner is ready', runnerId);
        this.iFrameReadyMap.set(runnerId, true);
        console.info('IFrameReadyMap', this.iFrameReadyMap);
        //Check if all iframes are ready
        if (Array.from(this.iFrameReadyMap.values()).every((value) => value)) {
          if (this.resolvePromises.length > 0) {
            console.info('All iframes are ready');
            this.resolvePromises.forEach((resolve) => resolve(true));
            this.resolvePromises = [];
          }
        }
        break;
      }

      default:
        break;
    }
  }
}
