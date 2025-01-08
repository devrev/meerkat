/**
 * This module is responsible on handling all the inter process communications
 * between the frontend to the electron backend.
 */

import { ipcMain } from 'electron';
import { DBMEvent } from '../api/main.preload';

export default class ElectronEvents {
  static bootstrapElectronEvents(): Electron.IpcMain {
    return ipcMain;
  }
}

ipcMain.on(DBMEvent.DOWNLOAD_FILES, (event, files) => {
  files.forEach((file) => {
    console.log(file);
  });
});

ipcMain.on(DBMEvent.REGISTER_FILE_BUFFERS, (event, fileBuffer) => {
  // duckDB.registerFileBuffer(fileBuffer);
});

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
