import { ipcMain } from 'electron';
import { FileStore } from 'meerkat-dbm/src/dbm/dbm-native/native-bridge';
import duckDB from '../duckdb/duckdb';
import { fileManager } from '../file-manager';
import { DropTableFilesPayload, NativeAppEvent } from '../types';
import { fetchParquetFile } from '../utils';
export default class ElectronEvents {
  static bootstrapElectronEvents(): Electron.IpcMain {
    return ipcMain;
  }
}

ipcMain.on(NativeAppEvent.REGISTER_FILES, async (event, files: FileStore[]) => {
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
      case 'json': {
        const jsonPath = `${file.fileName.split('.')[0]}.json`;

        await fileManager.writeFileJson({
          ...file,
          fileName: jsonPath,
        });

        const buffer = await duckDB.jsonToBuffer(
          fileManager.getPath(file.tableName, jsonPath)
        );

        await fileManager.writeFileBuffer({
          ...file,
          buffer,
        });

        await fileManager.deleteTableFiles(file.tableName, [jsonPath]);
        console.log('file deleted');
        break;
      }
      default: {
        throw new Error(`Unhandled file type: ${file.type}`);
      }
    }
  }

  event.returnValue = 'SUCCESS';
});

ipcMain.handle(NativeAppEvent.QUERY, async (event, query: string) => {
  console.log('query', query);

  const result = await duckDB.executeQuery({ query });

  return { data: result };
});

ipcMain.handle(
  NativeAppEvent.DROP_FILES_BY_TABLE_NAME,
  async (event, tableData: DropTableFilesPayload) => {
    await fileManager.deleteTableFiles(tableData.tableName, tableData.files);
  }
);

ipcMain.handle(
  NativeAppEvent.GET_FILE_PATHS_FOR_TABLE,
  async (event, tableName: string) => {
    return await fileManager.getTableFilePaths(tableName);
  }
);
