import { BrowserRunnerMessage } from '../../window-communication/runner-types';
import {
  WindowCommunication,
  WindowMessage,
} from '../../window-communication/window-communication';

export class IFrameManager {
  iframe: HTMLIFrameElement;
  communication: WindowCommunication<BrowserRunnerMessage>;

  constructor(
    uuid: string,
    onMessage: (
      runnerId: string,
      message: WindowMessage<BrowserRunnerMessage>
    ) => any
  ) {
    this.iframe = document.createElement('iframe');
    this.iframe.src = 'https://dbm.devrev-local.ai/runner?uuid=' + uuid;
    document.body.appendChild(this.iframe);
    this.communication = new WindowCommunication<BrowserRunnerMessage>({
      targetWindow: this.iframe.contentWindow as Window,
      origin: 'https://dbm.devrev-local.ai',
      targetApp: 'RUNNER',
      app_name: 'dbm',
    });
    this.iframe.style.visibility = 'hidden';
    this.communication.onMessage((message) => onMessage(uuid, message));
  }

  destroy() {
    this.iframe.remove();
  }
}
