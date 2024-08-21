import { BrowserRunnerMessage } from '../../window-communication/runner-types';
import { WindowCommunication } from '../../window-communication/window-communication';

export class IFrameManager {
  iframe: HTMLIFrameElement;
  communication: WindowCommunication<BrowserRunnerMessage>;

  constructor(uuid: string) {
    this.iframe = document.createElement('iframe');
    this.iframe.src = 'http://localhost:4205?uuid=' + uuid;
    document.body.appendChild(this.iframe);
    this.communication = new WindowCommunication<BrowserRunnerMessage>({
      targetWindow: this.iframe.contentWindow as Window,
      origin: '*',
      targetApp: 'dbm',
      app_name: 'dbm',
    });
  }

  destroy() {
    this.iframe.remove();
  }
}
