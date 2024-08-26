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

function getAppName(appId: string, uuid: string) {
  return appId + '__' + uuid;
}

export class IFrameManager {
  iframe: HTMLIFrameElement;
  communication: WindowCommunication<BrowserRunnerMessage>;
  uuid: string;

  constructor({
    origin,
    uuid,
    onMessage,
    runnerURL,
  }: IFrameManagerConstructor) {
    this.uuid = uuid;
    this.iframe = document.createElement('iframe');
    this.iframe.src = `${runnerURL}?uuid=` + uuid + '&origin=' + origin;
    const runnerDomain = new URL(runnerURL).origin;
    document.body.appendChild(this.iframe);
    this.communication = new WindowCommunication<BrowserRunnerMessage>({
      targetWindow: this.iframe.contentWindow as Window,
      origin: runnerDomain,
      targetApp: getAppName('RUNNER', uuid),
      app_name: getAppName('DBM', uuid),
    });
    this.iframe.style.visibility = 'hidden';
    this.communication.onMessage((message) => onMessage(uuid, message));
  }

  destroy() {
    this.iframe.remove();
  }
}
