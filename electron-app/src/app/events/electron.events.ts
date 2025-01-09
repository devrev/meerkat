/**
 * This module is responsible on handling all the inter process communications
 * between the frontend to the electron backend.
 */

import { ipcMain } from 'electron';
import { FileStore } from 'meerkat-dbm/src/dbm/dbm-native/native-bridge';
import { NativeAppEvent } from '../api/main.preload';
import duckDB from '../duckdb/duckdb';
import { fileManager } from '../file-manager';
import { fetchParquetFile } from '../utils';

export default class ElectronEvents {
  static bootstrapElectronEvents(): Electron.IpcMain {
    return ipcMain;
  }
}

ipcMain.on('register-files', async (event, data: { files: FileStore[] }) => {
  console.log('registerFiles in electron', data);
  const { files } = data;
  const filePaths = [];

  for (const file of files) {
    switch (file.type) {
      case 'url': {
        const buffer = await fetchParquetFile(file.fileUrl);
        const filePath = fileManager.writeBufferToFile({
          ...file,
          buffer,
        });
        filePaths.push({ filePath, fileName: file.fileName });
        break;
      }
      case 'buffer': {
        const filePath = fileManager.writeBufferToFile({
          ...file,
        });
        filePaths.push({ filePath, fileName: file.fileName });
        break;
      }
      default: {
        throw new Error(`Unhandled file type: ${file.type}`);
      }
    }
  }
});

ipcMain.handle(
  NativeAppEvent.QUERY,
  async (event, { query }: { query: string }) => {
    console.log('query in electron', query);

    const result = await duckDB.executeQuery({ query });
    console.log('resultda', result);
    return { message: { data: result, isError: false } };
  }
);

ipcMain.handle(
  NativeAppEvent.DROP_FILES_BY_TABLE,
  (
    event,
    tableData: {
      tableName: string;
      files: string[];
    }
  ) => {
    fileManager.dropFilesByTableNames(tableData.tableName, tableData.files);
  }
);

ipcMain.handle(
  NativeAppEvent.GET_FILE_PATHS_FOR_TABLE,
  (event, { tableName }: { tableName: string }) => {
    console.log('getFilePathsForTable in electron', tableName);
    return fileManager.getFilePathsForTable(tableName);
  }
);
