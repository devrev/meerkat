import { v4 as uuidv4 } from 'uuid';

export interface WindowMessage<MessageType> {
  timestamp: number;
  message: MessageType;
  uuid: string;
  target_app: string;
}

class Logger {
  private readonly app_name: string;
  private enable_logger = true;

  constructor({ app_name }: { app_name: string }) {
    this.app_name = app_name;
  }

  info(message: any, ...optionalParams: any[]) {
    if (!this.enable_logger) return;
    console.info(`[${this.app_name}]`, message, ...optionalParams);
  }

  warn(message: any, ...optionalParams: any[]) {
    if (!this.enable_logger) return;
    console.warn(`[${this.app_name}]`, message, ...optionalParams);
  }

  error(message: any, ...optionalParams: any[]) {
    if (!this.enable_logger) return;
    console.error(`[${this.app_name}]`, message, ...optionalParams);
  }
}

export interface CommunicationInterface<MessageType> {
  destroy: () => void;
  sendRequest: <Response>(
    message: MessageType
  ) => Promise<WindowMessage<Response>>;
  sendRequestWithoutResponse: (message: MessageType) => void;
  sendResponse: (uuid: string, message: any) => void;
  onMessage: (callback: (message: WindowMessage<MessageType>) => void) => void;
}

export class WindowCommunication<MessageType>
  implements CommunicationInterface<MessageType>
{
  private readonly app_name: string;
  private readonly _targetWindow: Window | undefined;
  private readonly _origin: string;
  private readonly _targetApp: string;
  private gcRunnerIntervalRef: any;
  private messagesPromiseMap: Map<
    string,
    {
      resolve: (value: WindowMessage<any>) => void;
      reject: (reason?: any) => void;
      timestamp: number;
    }
  > = new Map();
  private logger: Logger;

  constructor({
    app_name,
    targetWindow,
    targetApp,
    origin,
  }: {
    app_name: string;
    targetWindow: Window | undefined;
    origin: string;
    targetApp: string;
  }) {
    this.app_name = app_name;
    this._targetWindow = targetWindow;
    this._origin = origin;
    this._targetApp = targetApp;
    this.registerMessageListener();
    //Garbage collector every 30 seconds
    this.gcRunnerIntervalRef = setInterval(this.gc, 1000 * 60 * 5);
    this.logger = new Logger({ app_name: this.app_name });
  }

  private gc = () => {
    this.logger.info(
      `Starting GC with ${this.messagesPromiseMap.size} messages`
    );
    //Delete all messages that are older than 30 seconds
    const now = Date.now();
    this.messagesPromiseMap.forEach((value, key) => {
      if (now - value.timestamp > 30000) {
        this.logger.info('gc', key, value.timestamp, now - value.timestamp);
        this.messagesPromiseMap.get(key)?.reject();
        this.messagesPromiseMap.delete(key);
      }
    });
    this.logger.info(
      `GC finished with ${this.messagesPromiseMap.size} messages`
    );
  };

  private registerMessageListener() {
    window.addEventListener('message', (event) => {
      if (event.origin !== this._origin) {
        this.logger.warn(
          'IframeCommunication: origin mismatch',
          event.origin,
          this._origin
        );
        return;
      }
      if (event.data.target_app !== this.app_name) {
        this.logger.warn(
          'IframeCommunication: target_app mismatch',
          event.data.target_app,
          this.app_name
        );
        return;
      }

      const { message, timestamp, uuid } = event.data;
      const promise = this.messagesPromiseMap.get(uuid);
      if (promise) {
        this.messagesPromiseMap.delete(uuid);
        promise.resolve({
          message,
          target_app: this._targetApp,
          timestamp,
          uuid,
        });
      }
    });
  }

  public destroy() {
    clearInterval(this.gcRunnerIntervalRef);
  }

  /**
   * Request a message to the iframe
   * Every Request will always have a response
   * The response will be a Promise
   * @param message
   * @returns Promise<iFrameMessages>
   */
  public sendRequest<Response>(message: any): Promise<WindowMessage<Response>> {
    if (!this._targetWindow) {
      this.logger.warn('IframeCommunication: iframe has no contentWindow');
      return Promise.reject();
    }
    const uuid = uuidv4();
    const timestamp = Date.now();
    this._targetWindow.postMessage(
      {
        message,
        target_app: this._targetApp,
        timestamp,
        uuid,
      },
      this._origin
    );
    return new Promise((resolve, reject) => {
      this.messagesPromiseMap.set(uuid, { reject, resolve, timestamp });
    });
  }

  public sendRequestWithoutResponse(message: any) {
    if (!this._targetWindow) {
      this.logger.warn('IframeCommunication: iframe has no contentWindow');
      return;
    }
    const uuid = uuidv4();
    const timestamp = Date.now();
    this._targetWindow.postMessage(
      {
        message,
        target_app: this._targetApp,
        timestamp,
        uuid,
      },
      this._origin
    );
  }

  /**
   * Response of a message from the iframe
   * @param uuid
   * @param message
   * @returns
   */
  public sendResponse(uuid: string, message: any) {
    if (!this._targetWindow) {
      this.logger.warn('IframeCommunication: iframe has no contentWindow');
      return;
    }
    this._targetWindow.postMessage(
      {
        message,
        target_app: this._targetApp,
        timestamp: Date.now(),
        uuid,
      },
      this._origin
    );
  }

  public onMessage(callback: (message: WindowMessage<MessageType>) => void) {
    window.addEventListener('message', (event) => {
      if (event.origin !== this._origin) {
        this.logger.warn(
          'IframeCommunication: origin mismatch',
          event.origin,
          this._origin
        );
        return;
      }
      if (event.data.target_app !== this.app_name) {
        this.logger.warn(
          'IframeCommunication: target_app mismatch',
          event.data.target_app,
          this.app_name
        );
        return;
      }
      const { message, timestamp, uuid, target_app } = event.data;
      if (!uuid) {
        return;
      }
      callback({ message, target_app, timestamp, uuid });
    });
  }
}
