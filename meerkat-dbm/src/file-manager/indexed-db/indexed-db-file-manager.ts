import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import {
  FileBufferStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';
import { DuckDBDatabase } from './duckdb-database';


export class IndexedDBFileManager implements FileManagerType {
  private indexedDB: DuckDBDatabase;

  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;
  db: AsyncDuckDB;

  constructor({ fetchTableFileBuffers, db }: FileManagerConstructorOptions) {
    this.fetchTableFileBuffers = fetchTableFileBuffers;
    this.db = db;
    this.indexedDB = new DuckDBDatabase();
  }

  /**
   * Clear all data from the IndexedDB
   */
  private async _flushDB(): Promise<void> {
    await this.indexedDB.tablesKey.clear();
    await this.indexedDB.files.clear();
  }

  /**
   * Register a file buffer in both tablesKey and files table in IndexedDB
   */
  private async _registerFileInIndexedDB(file: FileBufferStore) {
    const { buffer, fileName, tableName, ...fileData } = file;

    // Retrieve data for the specified table from IndexedDB
    const tableData = await this.indexedDB.tablesKey.get(tableName);

    /**
     * Check if the table already exists in IndexedDB if it does, then update the existing table entry
     * else create a new entry for the table
     */
    if (tableData) {
      const fileList = tableData.files.map((file) => file.fileName);

      // Check if the file is not already registered, then update the existing table entry
      if (!fileList.includes(fileName)) {
        await this.indexedDB.tablesKey
          .where('tableName')
          .equals(tableName)
          .modify((prevData) => {
            prevData.files.push({ fileName, ...fileData });
          });
      }
    } else {
      await this.indexedDB.tablesKey.add({
        tableName: tableName,
        files: [{ fileName, ...fileData }],
      });
    }

    // Store file buffer in the 'files' object store of IndexedDB
    await this.indexedDB.files.put({ fileName, buffer });
  }

  async initializeDB(): Promise<void> {
    // Clear the database when initialized
    await this._flushDB();
  }

  async bulkRegisterFileBuffer(fileBuffers: FileBufferStore[]): Promise<void> {
    for (const fileBuffer of fileBuffers) {
      await this.registerFileBuffer(fileBuffer);
    }
  }

  async registerFileBuffer(fileBuffer: FileBufferStore): Promise<void> {
    await this._registerFileInIndexedDB(fileBuffer);

    // Register the file buffer in the DuckDB instance
    return this.db.registerFileBuffer(fileBuffer.fileName, fileBuffer.buffer);
  }

  async getFileBuffer(fileName: string): Promise<Uint8Array | undefined> {
    // Retrieve file data from IndexedDB
    const fileData = await this.indexedDB.files.get(fileName);

    return fileData?.buffer;
  }

  async mountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    const tableData = await this.indexedDB?.tablesKey.bulkGet(tableNames);

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
            await this.db.registerFileBuffer(file.fileName, file.buffer);
          }
        })
      );
    });

    await Promise.all(promises);
  }

  async unmountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    const tableData = await this.indexedDB?.tablesKey.bulkGet(tableNames);

    const promises = tableData.map(async (table) => {
      // Unregister file buffers from DuckDB for each table
      return Promise.all(
        (table?.files || []).map(async (file) => {
          await this.db.registerEmptyFileBuffer(file.fileName);
        })
      );
    });

    await Promise.all(promises);
  }
}
