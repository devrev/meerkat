import { BrowserRunnerMessage } from '../../window-communication/runner-types';
import {
  WindowCommunication,
  WindowMessage,
} from '../../window-communication/window-communication';

interface IFrameManagerConstructor {
  runnerURL: string;
  origin: string;
  uuid: string;
  onMessage: (
    runnerId: string,
    message: WindowMessage<BrowserRunnerMessage>
  ) => any;
}

export class IFrameManager {
  iframe: HTMLIFrameElement;
  communication: WindowCommunication<BrowserRunnerMessage>;

  constructor({
    origin,
    uuid,
    onMessage,
    runnerURL,
  }: IFrameManagerConstructor) {
    this.iframe = document.createElement('iframe');
    this.iframe.src = `${runnerURL}?uuid=` + uuid + '&origin=' + origin;
    const runnerDomain = new URL(runnerURL).origin;
    document.body.appendChild(this.iframe);
    this.communication = new WindowCommunication<BrowserRunnerMessage>({
      targetWindow: this.iframe.contentWindow as Window,
      origin: runnerDomain,
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
