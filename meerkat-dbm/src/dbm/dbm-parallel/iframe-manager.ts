import {
  getMainAppName,
  getRunnerAppName,
} from '../../utils/parallel-dbm-utils/get-app-name';
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

    //Move the iframe out of the screen
    this.iframe.style.position = 'absolute';
    this.iframe.style.left = '-10000px';
    this.iframe.style.top = '0';
    this.iframe.style.visibility = 'hidden';

    this.communication = new WindowCommunication<BrowserRunnerMessage>({
      targetWindow: this.iframe.contentWindow as Window,
      origin: runnerDomain,
      targetApp: getRunnerAppName(uuid),
      app_name: getMainAppName(uuid),
    });
    this.communication.onMessage((message) => onMessage(uuid, message));
  }

  destroy() {
    this.communication.destroy();
    this.iframe.remove();
  }
}
