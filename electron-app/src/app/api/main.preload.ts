import { contextBridge, ipcRenderer } from 'electron';
import { FileStore } from 'meerkat-dbm/src/dbm/dbm-native/native-bridge';
import { DropTableFilesPayload, NativeAppEvent } from '../types';

export type ContextBridgeApi = {
  registerFiles: (files: FileStore[]) => void;
  query: (query: string) => Promise<{ data: Record<string, unknown> }>;
  getFilePathsForTable: (tableName: string) => Promise<string[]>;
  dropFilesByTableName: (tableData: DropTableFilesPayload) => Promise<void>;
};

const API: ContextBridgeApi = {
  registerFiles: (files: FileStore[]) => {
    ipcRenderer.sendSync(NativeAppEvent.REGISTER_FILES, files);

    return;
  },

  query: (query: string) => {
    return new Promise((resolve) => {
      ipcRenderer
        .invoke(NativeAppEvent.QUERY, query)
        .then((data) => resolve(data));
    });
  },

  getFilePathsForTable: (tableName: string) => {
    return new Promise((resolve) => {
      ipcRenderer
        ?.invoke(NativeAppEvent.GET_FILE_PATHS_FOR_TABLE, tableName)
        .then((data) => resolve(data));
    });
  },

  dropFilesByTableName: (tableData: DropTableFilesPayload) => {
    return new Promise((resolve) => {
      ipcRenderer.invoke(NativeAppEvent.DROP_FILES_BY_TABLE_NAME, tableData);
      resolve();
    });
  },
};

contextBridge?.exposeInMainWorld('api', API);
