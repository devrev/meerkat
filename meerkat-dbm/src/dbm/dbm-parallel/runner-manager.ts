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
  fetchTableFileBuffers: (tables: TableConfig[]) => Promise<FileBufferStore[]>;

  iFrameReadyMap: Map<string, boolean> = new Map();
  resolvePromise: ((value: unknown) => void) | null = null;

  addIFrameManager(uuid: string) {
    this.iFrameReadyMap.set(uuid, false);
    this.iFrameManagers.set(
      uuid,
      new IFrameManager(uuid, this.messageListener.bind(this))
    );
  }

  constructor({
    fetchTableFileBuffers,
  }: {
    fetchTableFileBuffers: (
      tables: TableConfig[]
    ) => Promise<FileBufferStore[]>;
  }) {
    this.fetchTableFileBuffers = fetchTableFileBuffers;
    this.addIFrameManager('1');
    this.addIFrameManager('2');
  }

  public async isFrameRunnerReady() {
    if (Array.from(this.iFrameReadyMap.values()).every((value) => value)) {
      console.info('All iframes are ready');

      return true;
    }
    const promiseObj = new Promise((resolve) => {
      this.resolvePromise = resolve;
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
          if (this.resolvePromise) {
            console.info('All iframes are ready');
            this.resolvePromise(true);
          }
        }
        break;
      }

      default:
        break;
    }
  }
}
