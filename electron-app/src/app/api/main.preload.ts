import { contextBridge, ipcRenderer } from 'electron';
import {
  FileBufferMetadata,
  FileUrlMetadata,
} from 'meerkat-dbm/src/dbm/dbm-native/native-bridge';

export enum DBMEvent {
  REGISTER_FILE_BUFFERS = 'register-file-buffers',
  QUERY = 'query',
  DROP_FILES_BY_TABLE = 'drop-files-by-table',
  DOWNLOAD_FILES = 'download-files',
}

const API = {
  registerFiles: (fileBuffers: FileBufferMetadata[]) => {
    ipcRenderer.send(DBMEvent.REGISTER_FILE_BUFFERS, fileBuffers);
  },
  downloadFiles: (files: FileUrlMetadata[]) => {
    ipcRenderer.send(DBMEvent.DOWNLOAD_FILES, files);
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

contextBridge.exposeInMainWorld('electron', API);
