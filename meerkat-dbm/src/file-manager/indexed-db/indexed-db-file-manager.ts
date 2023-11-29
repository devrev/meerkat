import { InstanceManagerType } from '../../dbm/instance-manager';
import { mergeFileBufferStoreIntoTable } from '../../utils/merge-file-buffer-store-into-table';
import {
  FileBufferStore,
  FileData,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';
import { DuckDBFilesDatabase } from './duckdb-files-database';

export class IndexedDBFileManager implements FileManagerType {
  // IndexedDB instance
  private indexedDB: DuckDBFilesDatabase;

  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;
  instanceManager: InstanceManagerType;

  constructor({
    fetchTableFileBuffers,
    instanceManager,
  }: FileManagerConstructorOptions) {
    this.fetchTableFileBuffers = fetchTableFileBuffers;
    this.instanceManager = instanceManager;
    this.indexedDB = new DuckDBFilesDatabase();
  }

  /**
   * Clear all data from the IndexedDB
   */
  private async _flushDB(): Promise<void> {
    await this.indexedDB.tablesKey.clear();
    await this.indexedDB.files.clear();
  }

  async initializeDB(): Promise<void> {
    return;
  }

  async bulkRegisterFileBuffer(fileBuffers: FileBufferStore[]): Promise<void> {
    const tableNames = Array.from(
      new Set(fileBuffers.map((fileBuffer) => fileBuffer.tableName))
    );

    const currentTableData = await this.indexedDB.tablesKey.toArray();

    const updatedTableMap = mergeFileBufferStoreIntoTable(
      fileBuffers,
      currentTableData
    );

    /**
     * Extracts the tables and files data from the tablesMap and fileBuffers
     * in format that can be stored in IndexedDB
     */
    const updatedTableData = tableNames.map((tableName) => {
      return { tableName, files: updatedTableMap.get(tableName)?.files ?? [] };
    });

    const newFilesData = fileBuffers.map((fileBuffer) => {
      return { buffer: fileBuffer.buffer, fileName: fileBuffer.fileName };
    });

    // Update the tables and files table in IndexedDB
    await this.indexedDB
      .transaction(
        'rw',
        this.indexedDB.tablesKey,
        this.indexedDB.files,
        async () => {
          await this.indexedDB.tablesKey.bulkPut(updatedTableData);

          await this.indexedDB.files.bulkPut(newFilesData);
        }
      )
      .catch((error) => {
        console.error(error);
      });

    const db = await this.instanceManager.getDB();

    // Register the file buffers in the DuckDB instance
    const promiseArr = fileBuffers.map((fileBuffer) =>
      db.registerFileBuffer(fileBuffer.fileName, fileBuffer.buffer)
    );

    await Promise.all(promiseArr);
  }

  async registerFileBuffer(fileBuffer: FileBufferStore): Promise<void> {
    const { buffer, fileName, tableName } = fileBuffer;

    const currentTableData = await this.indexedDB.tablesKey.toArray();

    const updatedTablesMap = mergeFileBufferStoreIntoTable(
      [fileBuffer],
      currentTableData
    );

    // Update the tables and files table in IndexedDB
    await this.indexedDB
      .transaction(
        'rw',
        this.indexedDB.tablesKey,
        this.indexedDB.files,
        async () => {
          await this.indexedDB.tablesKey.put({
            tableName: fileBuffer.tableName,
            files: updatedTablesMap.get(tableName)?.files ?? [],
          });

          await this.indexedDB.files.put({ fileName, buffer });
        }
      )
      .catch((error) => {
        console.error(error);
      });

    const db = await this.instanceManager.getDB();

    // Register the file buffer in the DuckDB instance
    return db.registerFileBuffer(fileBuffer.fileName, fileBuffer.buffer);
  }

  async getFileBuffer(fileName: string): Promise<Uint8Array | undefined> {
    // Retrieve file data from IndexedDB
    const fileData = await this.indexedDB.files.get(fileName);

    return fileData?.buffer;
  }

  async mountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    const tableData = await this.indexedDB.tablesKey.bulkGet(tableNames);

    const promises = tableData.map(async (table) => {
      // Retrieve file names for the specified table
      const filesList = (table?.files || []).map(
        (fileData) => fileData.fileName
      );

      const filesData = await this.indexedDB?.files.bulkGet(filesList);

      // Register file buffers from IndexedDB for each table
      await Promise.all(
        filesData.map(async (file) => {
          if (file) {
            const db = await this.instanceManager.getDB();

            await db.registerFileBuffer(file.fileName, file.buffer);
          }
        })
      );
    });

    await Promise.all(promises);
  }

  async unmountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    const tableData = await this.indexedDB.tablesKey.bulkGet(tableNames);

    const promises = tableData.map(async (table) => {
      // Unregister file buffers from DuckDB for each table
      return Promise.all(
        (table?.files || []).map(async (file) => {
          const db = await this.instanceManager.getDB();
          await db.registerEmptyFileBuffer(file.fileName);
        })
      );
    });

    await Promise.all(promises);
  }

  /**
   * Get the list of files for the specified table name
   */
  async getFilesByTableName(tableName: string): Promise<FileData[]> {
    const tableData = await this.indexedDB.tablesKey.get(tableName);

    return tableData?.files ?? [];
  }

  /**
   * Drop the specified files by tableName from the IndexedDB
   */
  async dropFilesByTableName(
    tableName: string,
    fileNames: string[]
  ): Promise<void> {
    const tableData = await this.indexedDB.tablesKey.get(tableName);

    if (tableData) {
      // Retrieve the files that are not dropped
      const updatedFiles = tableData.files.filter(
        (file) => !fileNames.includes(file.fileName)
      );

      await this.indexedDB.tablesKey.put({
        tableName,
        files: updatedFiles,
      });
    }

    // Remove the files from the IndexedDB
    await this.indexedDB.files.bulkDelete(fileNames);
  }
}
