import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { v4 as uuidv4 } from 'uuid';
import { DBM } from './dbm';
import { InstanceManagerType } from './instance-manager';
import { DBMConstructorOptions } from './types';

class IFrameManager {
  promiseMap: Map<
    string,
    {
      resolve: (value: unknown) => void;
      reject: (reason?: any) => void;
    }
  > = new Map();

  readyPromise: Promise<void>;
  readyResolve: (() => void) | null = null;
  isReady = false;

  iframe: HTMLIFrameElement;
  constructor() {
    this.iframe = document.createElement('iframe');
    this.iframe.src = 'http://localhost:4200/assets/duckdb-exec.html';
    document.body.appendChild(this.iframe);
    this.readyPromise = new Promise((resolve) => {
      this.readyResolve = resolve;
    });

    // Add event listener for messages
    window.addEventListener('message', this.onMessage.bind(this));
  }

  async ready() {
    if (this.isReady) {
      return;
    }
    return this.readyPromise;
  }

  execQuery(query: string) {
    const uuid = uuidv4();
    this.iframe.contentWindow?.postMessage(
      {
        event_type: 'EXEC_QUERY',
        query,
        uuid,
      },
      '*'
    );
    const promise = new Promise((resolve, reject) => {
      this.promiseMap.set(uuid, { resolve, reject });
    });
    return promise;
  }

  onMessage(event: MessageEvent) {
    const { data } = event;
    console.info('IFrameManager received message:', data);

    if (typeof data !== 'object' || data === null) {
      return;
    }

    const { event_type, uuid, payload, error } = data;

    if (event_type === 'ON_READY') {
      console.log('Received ON_READY message');
      this.isReady = true;
      this.readyResolve?.();
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      //@ts-ignore
      this.iframe.contentWindow.helloWorld = 'bye';
      return;
    }

    if (event_type === 'QUERY_RESULT') {
      const promise = this.promiseMap.get(uuid);
      if (!promise) {
        return;
      }
      if (error) {
        promise.reject(error);
      } else {
        promise.resolve(payload.results);
      }
      this.promiseMap.delete(uuid);
    }
  }

  // Add a method to clean up the event listener when no longer needed
  destroy() {
    window.removeEventListener('message', this.onMessage.bind(this));
    document.body.removeChild(this.iframe);
  }
}

class IframeInstanceManager implements InstanceManagerType {
  private iFrameManagers: IFrameManager[] = [];
  private number = 0;
  private connection: AsyncDuckDBConnection = {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    query: async (text: string) => {
      this.number++;
      //is even
      const isEven = this.number % 2 === 0;
      const results = await this.iFrameManagers[isEven ? 0 : 1].execQuery(text);
      return results;
    },

    close: async () => {
      //no
    },
  };

  constructor() {
    this.iFrameManagers = [];
    this.iFrameManagers.push(new IFrameManager());
    this.iFrameManagers.push(new IFrameManager());
  }

  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  //@ts-ignore
  async getDB() {
    await this.iFrameManagers[0].ready();
    await Promise.all(
      this.iFrameManagers.map((iFrameManager) => iFrameManager.ready())
    );
    const db = {
      connect: () => Promise.resolve(this.connection),
    };

    return db;
  }

  async terminateDB() {
    //no-ops
  }
}

export class DBMParallel extends DBM {
  constructor({
    fileManager,
    logger,
    onEvent,
    options,
    instanceManager,
    onDuckDBShutdown,
  }: DBMConstructorOptions) {
    const iFrameInstanceManager = new IframeInstanceManager();

    super({
      fileManager,
      logger,
      onEvent,
      options,
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      //@ts-ignore
      instanceManager: iFrameInstanceManager,
      onDuckDBShutdown,
    });
  }
}
