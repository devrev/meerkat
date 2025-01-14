import { NativeFileStore } from '@devrev/meerkat-dbm';
import { ipcMain } from 'electron';
import duckDB from '../duckdb/duckdb';
import { fileManager } from '../file-manager';
import { DropTableFilesPayload, NativeAppEvent } from '../types';
import { fetchParquetFile } from '../utils';
export default class ElectronEvents {
  static bootstrapElectronEvents(): Electron.IpcMain {
    return ipcMain;
  }
}

ipcMain.handle(
  NativeAppEvent.REGISTER_FILES,
  async (event, files: NativeFileStore[]) => {
    for (const file of files) {
      switch (file.type) {
        case 'url': {
          const buffer = await fetchParquetFile(file.url);

          await fileManager.writeFileBuffer({
            ...file,
            buffer,
          });

          break;
        }
        case 'buffer': {
          await fileManager.writeFileBuffer(file);

          break;
        }

        default: {
          throw new Error(`Unhandled file type ${file.type}`);
        }
      }
    }

    return;
  }
);

ipcMain.handle(NativeAppEvent.QUERY, async (event, query: string) => {
  const result = await duckDB.executeQuery({ query });

  return { data: result };
});

ipcMain.handle(
  NativeAppEvent.DROP_FILES_BY_TABLE_NAME,
  async (event, tableData: DropTableFilesPayload) => {
    await fileManager.deleteTableFiles(tableData.tableName, tableData.files);

    return;
  }
);

ipcMain.handle(
  NativeAppEvent.GET_FILE_PATHS_FOR_TABLE,
  async (event, tableName: string) => {
    const filePaths = await fileManager.getTableFilePaths(tableName);

    return filePaths;
  }
);
