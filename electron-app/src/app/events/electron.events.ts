/**
 * This module is responsible on handling all the inter process communications
 * between the frontend to the electron backend.
 */

import { ipcMain } from 'electron';
import { FileStore } from 'meerkat-dbm/src/dbm/dbm-native/native-bridge';
import { DBMEvent } from '../api/main.preload';
import duckDB from '../duckdb/duckdb';
import { fileManager } from '../file-manager';
import { fetchParquetFile } from '../utils';

export default class ElectronEvents {
  static bootstrapElectronEvents(): Electron.IpcMain {
    return ipcMain;
  }
}

ipcMain.on(
  DBMEvent.REGISTER_FILES,
  async (event, data: { files: FileStore[] }) => {
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
          console.warn(`Unhandled file type: ${file.type}`);
          break;
        }
      }
    }
    console.log('filePaths', filePaths);
    return filePaths;
  }
);

ipcMain.on(DBMEvent.QUERY, (event, query: string) => {
  console.log('query in electron', query);
  const result = duckDB.executeQuery({ query, tables: [] });
  return result;
});

ipcMain.handle(
  DBMEvent.DROP_FILES_BY_TABLE,
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

// ipcMain.on(DBMEvent.REGISTER_FILE_BUFFERS, (event, fileBuffer) => {
//   // duckDB.registerFileBuffer(fileBuffer);
// });

// ipcMain.handle('execute-query', async (event, payload) => {
//   try {
//     // Extract query from the payload object
//     const { query, tables, options } = payload;
//     const result = await duckDB.executeQuery(payload);

//     return { message: { data: result, isError: false } };
//   } catch (error) {
//     console.error('Error executing query:', error);
//     return { message: { error: error.message, isError: true } };
//   }
// });
