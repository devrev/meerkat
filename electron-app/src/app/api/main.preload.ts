import { NativeFileStore } from '@devrev/meerkat-dbm';
import { contextBridge, ipcRenderer } from 'electron';
import { DropTableFilesPayload, NativeAppEvent } from '../types';

export type ContextBridgeApi = {
  registerFiles: (files: NativeFileStore[]) => Promise<void>;
  query: (query: string) => Promise<{ data: Record<string, unknown> }>;
  getFilePathsForTable: (tableName: string) => Promise<string[]>;
  dropFilesByTableName: (tableData: DropTableFilesPayload) => Promise<void>;
};

const API: ContextBridgeApi = {
  registerFiles: (files: NativeFileStore[]) => {
    return ipcRenderer.invoke(NativeAppEvent.REGISTER_FILES, files);
  },

  query: (query: string) => {
    return ipcRenderer.invoke(NativeAppEvent.QUERY, query);
  },

  getFilePathsForTable: (tableName: string) => {
    return ipcRenderer.invoke(
      NativeAppEvent.GET_FILE_PATHS_FOR_TABLE,
      tableName
    );
  },

  dropFilesByTableName: (tableData: DropTableFilesPayload) => {
    return ipcRenderer.invoke(
      NativeAppEvent.DROP_FILES_BY_TABLE_NAME,
      tableData
    );
  },
};

contextBridge?.exposeInMainWorld('api', API);
