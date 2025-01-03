import { contextBridge, ipcRenderer, IpcRendererEvent } from 'electron';

export enum DBMEvent {
  REGISTER_FILE_BUFFER = 'register-file-buffer',
}

export type Channels = DBMEvent;

const electronHandler = {
  ipcRenderer: {
    invoke: (channel: Channels, ...args: unknown[]) => {
      return ipcRenderer.invoke(channel, ...args); // Added return statement here
    },
    on(channel: Channels, func: (...args: unknown[]) => void) {
      const subscription = (_event: IpcRendererEvent, ...args: unknown[]) =>
        func(...args);
      ipcRenderer.on(channel, subscription);

      return () => {
        ipcRenderer.removeListener(channel, subscription);
      };
    },
    once(channel: Channels, func: (...args: unknown[]) => void) {
      ipcRenderer.once(channel, (_event, ...args) => func(...args));
    },
    send(channel: Channels, ...args: unknown[]) {
      ipcRenderer.send(channel, ...args);
    },
    sendMessage(channel: Channels, ...args: unknown[]) {
      ipcRenderer.send(channel, ...args);
    },
  },
};
contextBridge.exposeInMainWorld('electron', electronHandler);
