import { contextBridge, ipcRenderer, IpcRendererEvent } from 'electron';

export enum NativeAppEvent {
  REGISTER_FILES = 'register-files',
  QUERY = 'query',
  DROP_FILES_BY_TABLE = 'drop-files-by-table',
  GET_FILE_PATHS_FOR_TABLE = 'get-file-paths-for-table',
}

export type Channels = NativeAppEvent;

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

// const API = {
//   registerFiles: async (fileBuffers: FileStore[]) => {
//     // Send the files array wrapped in an object matching the handler's expected structure
//     const response = await ipcRenderer.invoke(DBMEvent.REGISTER_FILES, {
//       files: fileBuffers,
//     });
//     return response;
//   },

//   dropFilesByTable: (tableName: string, fileNames: string[]) => {
//     ipcRenderer.send(DBMEvent.DROP_FILES_BY_TABLE, {
//       tableName,
//       fileNames,
//     });
//   },
//   query: (query: string) => {
//     ipcRenderer.send(DBMEvent.QUERY, query);
//   },
// };

contextBridge?.exposeInMainWorld('electron', electronHandler);
