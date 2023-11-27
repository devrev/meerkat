import { InstanceManagerType } from '../../dbm/instance-manager';
import { mergeFileBufferStoreIntoTable } from '../../utils/merge-file-buffer-store-into-table';
import {
  FileBufferStore,
  FileManagerConstructorOptions,
  FileManagerType,
  Table,
} from '../file-manager-type';
import { DuckDBDatabase } from './duckdb-database';
export class IndexedDBFileManager implements FileManagerType {
  private indexedDB: DuckDBDatabase;
  private tables: Map<string, Table>;

  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;
  instanceManager: InstanceManagerType;

  constructor({
    fetchTableFileBuffers,
    instanceManager,
  }: FileManagerConstructorOptions) {
    this.fetchTableFileBuffers = fetchTableFileBuffers;
    this.instanceManager = instanceManager;
    this.indexedDB = new DuckDBDatabase();
    this.tables = new Map();
  }

  /**
   * Clear all data from the IndexedDB
   */
  private async _flushDB(): Promise<void> {
    await this.indexedDB.tablesKey.clear();
    await this.indexedDB.files.clear();
  }

  async initializeDB(): Promise<void> {
    // Clear the database when initialized
    await this._flushDB();
  }

  async bulkRegisterFileBuffer(fileBuffers: FileBufferStore[]): Promise<void> {
    const tableNames = Array.from(
      new Set(fileBuffers.map((fileBuffer) => fileBuffer.tableName))
    );

    const updatedTableMap = mergeFileBufferStoreIntoTable(
      fileBuffers,
      this.tables
    );

    /**
     * Extracts the tables and files data from the tablesMap and fileBuffers
     * in format that can be stored in IndexedDB
     */
    const tableData = tableNames.map((tableName) => {
      return { tableName, files: updatedTableMap.get(tableName)?.files ?? [] };
    });

    const fileData = fileBuffers.map((fileBuffer) => {
      return { buffer: fileBuffer.buffer, fileName: fileBuffer.fileName };
    });

    // Update the tables and files table in IndexedDB
    await this.indexedDB
      .transaction(
        'rw',
        this.indexedDB.tablesKey,
        this.indexedDB.files,
        async () => {
          await this.indexedDB.tablesKey.bulkPut(tableData);

          await this.indexedDB.files.bulkPut(fileData);

          this.tables = updatedTableMap;
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

    const updatedTableMap = mergeFileBufferStoreIntoTable(
      [fileBuffer],
      this.tables
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
            files: updatedTableMap.get(tableName)?.files ?? [],
          });

          await this.indexedDB.files.put({ fileName, buffer });

          this.tables = updatedTableMap;
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
    const tableData = tableNames.map((tableName) => {
      return this.tables.get(tableName);
    });

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
    const tableData = tableNames.map((tableName) => {
      return this.tables.get(tableName);
    });

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
}
