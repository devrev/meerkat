import { FileBufferStore } from '../../file-manager/file-manager-type';
import { DBMEvent, DBMLogger } from '../../logger';
import { Table } from '../../types';
import {
  BROWSER_RUNNER_TYPE,
  BrowserRunnerMessage,
} from '../../window-communication/runner-types';
import { WindowMessage } from '../../window-communication/window-communication';
import { TableConfig } from '../types';
import { IFrameManager } from './iframe-manager';

export interface IFrameRunnerManagerConstructor {
  runnerURL: string;
  origin: string;
  fetchTableFileBuffers: (tables: TableConfig[]) => Promise<FileBufferStore[]>;
  fetchPreQuery: (runnerId: string, tables: Table[]) => string[];
  totalRunners: number;
  logger: DBMLogger;
  /**
   * Instance-level callback for events that are not scoped to a single query
   * (e.g. runner-side `clone_buffer_duration`, emitted inside the iframe where
   * a per-query callback cannot be reached across `postMessage`). Query-
   * lifecycle events are dispatched per-query via
   * {@link IFrameRunnerManager.registerQueryEventCallback}.
   */
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
  // Per-query event callbacks (QueryOptions.onEvent), keyed by the id of the
  // runner executing that query. A runner runs at most one query at a time, so
  // the runnerId that RUNNER_ON_EVENT arrives on uniquely identifies the query
  // whose callback should receive the event — no queryId needs to cross
  // postMessage.
  private perRunnerEventCallbacks: Map<string, (event: DBMEvent) => void> =
    new Map();

  private fetchTableFileBuffers: (
    tables: TableConfig[]
  ) => Promise<FileBufferStore[]>;
  private fetchPreQuery: (runnerId: string, tables: Table[]) => string[];

  constructor({
    runnerURL,
    origin,
    fetchTableFileBuffers,
    fetchPreQuery,
    totalRunners = 2,
    logger,
    onEvent,
  }: IFrameRunnerManagerConstructor) {
    this.logger = logger;
    this.onEvent = onEvent;
    this.runnerURL = runnerURL;
    this.origin = origin;
    this.totalRunners = totalRunners;
    this.fetchTableFileBuffers = fetchTableFileBuffers;
    this.fetchPreQuery = fetchPreQuery;
  }

  public registerQueryEventCallback(
    runnerId: string,
    onEvent?: (event: DBMEvent) => void
  ) {
    if (onEvent) {
      this.perRunnerEventCallbacks.set(runnerId, onEvent);
    }
  }

  public unregisterQueryEventCallback(runnerId: string) {
    this.perRunnerEventCallbacks.delete(runnerId);
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

  private messageListener(
    runnerId: string,
    message: WindowMessage<BrowserRunnerMessage>
  ) {
    switch (message.message.type) {
      case BROWSER_RUNNER_TYPE.RUNNER_GET_FILE_BUFFERS:
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

      case BROWSER_RUNNER_TYPE.RUNNER_ON_EVENT: {
        // The runner decides scope at the emit site and tags the message. A
        // 'query' event belongs to this runner's in-flight query (one query per
        // runner) and goes to that query's callback; anything else goes to the
        // instance sink. A single event never reaches both.
        const payload = message.message.payload;
        const perQueryOnEvent = this.perRunnerEventCallbacks.get(runnerId);
        if (message.message.scope === 'query' && perQueryOnEvent) {
          perQueryOnEvent(payload);
        } else if (this.onEvent) {
          this.onEvent(payload);
        }
        break;
      }

      case BROWSER_RUNNER_TYPE.RUNNER_PRE_QUERY: {
        if (this.fetchPreQuery) {
          const manager = this.iFrameManagers.get(runnerId);
          if (!manager) {
            return;
          }

          const { tables } = message.message.payload;

          const preQueries = this.fetchPreQuery(runnerId, tables);

          manager.communication.sendResponse(message.uuid, preQueries);
        }

        break;
      }

      default:
        break;
    }
  }
}
