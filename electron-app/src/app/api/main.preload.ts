import { contextBridge, ipcRenderer } from 'electron';
import { FileStore } from 'meerkat-dbm/src/dbm/dbm-native/native-bridge';

export enum DBMEvent {
  REGISTER_FILES = 'register-files',
  QUERY = 'query',
  DROP_FILES_BY_TABLE = 'drop-files-by-table',
}

const API = {
  registerFiles: (fileBuffers: FileStore[]) => {
    ipcRenderer.send(DBMEvent.REGISTER_FILES, fileBuffers);
  },

  dropFilesByTable: (tableName: string, fileNames: string[]) => {
    ipcRenderer.send(DBMEvent.DROP_FILES_BY_TABLE, {
      tableName,
      fileNames,
    });
  },
  query: (query: string) => {
    ipcRenderer.send(DBMEvent.QUERY, query);
  },
};

contextBridge?.exposeInMainWorld('electron', API);
