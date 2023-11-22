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

    // Clear the database when initialized
    this._flushDB();
  }

  /**
   * Clear all data from the IndexedDB database
   */
  private _flushDB(): void {
    if (!this.indexedDB) throw new Error('indexedDB is not initialized');

    this.indexedDB.datasets.clear();
    this.indexedDB.files.clear();
  }

  /**
   * Register a file buffer in both datasets and files table in IndexedDB
   */
  private async _registerFileInIndexedDB(file: FileBufferStore) {
    if (!this.indexedDB) console.error('indexedDB is not initialized');

    const { buffer, fileName, tableName, ...fileData } = file;

    // Retrieve data for the specified table from IndexedDB
    const tableData = await this.indexedDB.datasets.get(tableName);

    /**
     * Check if the table already exists in IndexedDB if it does, then update the existing table entry
     * else create a new entry for the table
     */
    if (tableData) {
      const fileList = tableData.files.map((file) => file.fileName);

      // Check if the file is not already registered, then update the existing table entry
      if (!fileList.includes(fileName)) {
        await this.indexedDB.datasets
          .where('tableName')
          .equals(tableName)
          .modify((prevData) => {
            prevData.files.push({ fileName, ...fileData });
          });
      }
    } else {
      await this.indexedDB.datasets.add({
        tableName: tableName,
        files: [{ fileName, ...fileData }],
      });
    }

    // Store file buffer in the 'files' object store of IndexedDB
    await this.indexedDB.files.put({ fileName, buffer });
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
    if (!this.indexedDB) throw new Error('indexedDB is not initialized');

    // Retrieve file data from IndexedDB
    const fileData = await this.indexedDB.files.get(fileName);

    return fileData?.buffer;
  }

  async mountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    if (!this.indexedDB) throw new Error('indexedDB is not initialized');

    const promiseArr = tableNames.map(async (tableName) => {
      // Retrieve table data from IndexedDB
      const tableData = await this.indexedDB?.datasets.get(tableName);

      /**
       * Retrieve file buffers from indexedDB for that table and
       * register them in DuckDB
       */
      return Promise.all(
        (tableData?.files || []).map(async (file) => {
          const fileData = await this.indexedDB?.files.get(file.fileName);

          if (fileData) {
            await this.db.registerFileBuffer(
              fileData.fileName,
              fileData.buffer
            );
          }
        })
      );
    });

    await Promise.all(promiseArr);
  }

  async unmountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    if (!this.indexedDB) throw new Error('indexedDB is not initialized');

    const promiseArr = tableNames.map(async (tableName) => {
      // Retrieve table data from IndexedDB
      const tableData = await this.indexedDB?.datasets.get(tableName);

      /**
       * Unregister file buffers from DuckDB for that table
       */
      return Promise.all(
        (tableData?.files || []).map(async (file) => {
          await this.db.registerEmptyFileBuffer(file.fileName);
        })
      );
    });

    await Promise.all(promiseArr);
  }
}
