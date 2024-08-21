import { FileBufferStore } from '../../file-manager/file-manager-type';
import { BrowserRunnerMessage } from '../../window-communication/runner-types';
import { WindowMessage } from '../../window-communication/window-communication';
import { TableConfig } from '../types';
import { IFrameManager } from './iframe-manager';

export class IFrameRunnerManager {
  iFrameManagers: Map<string, IFrameManager> = new Map();
  fetchTableFileBuffers: (tables: TableConfig[]) => Promise<FileBufferStore[]>;

  constructor({
    fetchTableFileBuffers,
  }: {
    fetchTableFileBuffers: (
      tables: TableConfig[]
    ) => Promise<FileBufferStore[]>;
  }) {
    this.fetchTableFileBuffers = fetchTableFileBuffers;
    this.iFrameManagers.set('1', new IFrameManager('1', this.messageListener));
  }

  private messageListener(
    runnerId: string,
    message: WindowMessage<BrowserRunnerMessage>
  ) {
    switch (message.message.type) {
      case 'RUNNER_GET_FILE_BUFFERS':
        if (this.fetchTableFileBuffers) {
          this.fetchTableFileBuffers(message.message.payload.tables).then(
            (result) => {
              const manager = this.iFrameManagers.get(runnerId);
              if (!manager) {
                return;
              }
              manager.communication.sendResponse(message.uuid, result);
            }
          );
        }

        break;
      default:
        break;
    }
  }
}
