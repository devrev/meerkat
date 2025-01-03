/**
 * This module is responsible on handling all the inter process communications
 * between the frontend to the electron backend.
 */

import { app, ipcMain } from 'electron';
import { environment } from '../../environments/environment';
import duckDB from '../duckdb/duckdb';

export default class ElectronEvents {
  static bootstrapElectronEvents(): Electron.IpcMain {
    return ipcMain;
  }
}

// Retrieve app version
ipcMain.handle('register-files', (event) => {
  console.log(`Fetching application version... [v${environment.version}]`);

  return environment.version;
});

ipcMain.on('register-file-buffer', (event, fileBuffer) => {
  duckDB.registerFileBuffer(fileBuffer);
});

ipcMain.on('mount-file-buffer', (event, tables) => {
  // duckDB.mountFileBufferByTables(tables);
});

ipcMain.handle('execute-query', async (event, payload) => {
  try {
    // Extract query from the payload object
    const { query, tables, options } = payload;
    const result = await duckDB.executeQuery(payload);

    return { message: { data: result, isError: false } };
  } catch (error) {
    console.error('Error executing query:', error);
    return { message: { error: error.message, isError: true } };
  }
});

// Handle App termination
ipcMain.on('quit', (event, code) => {
  app.exit(code);
});
