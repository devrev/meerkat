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
    if (!this.indexedDB) console.error('indexedDB is not initialized');

    this.indexedDB.datasets.clear();
    this.indexedDB.files.clear();
  }

  async bulkRegisterFileBuffer(fileBuffers: FileBufferStore[]): Promise<void> {
    for (const fileBuffer of fileBuffers) {
      await this.registerFileBuffer(fileBuffer);
    }
  }

  async registerFileBuffer(props: FileBufferStore): Promise<void> {
    if (!this.indexedDB) console.error('indexedDB is not initialized');

    const { buffer, fileName, tableName, ...fileData } = props;

    // Check if indexedDB is initialized then write the file info in indexedDB
    if (this.indexedDB) {
      const tableData = await this.indexedDB.datasets.get(tableName);
      if (tableData) {
        const fileList = tableData.files.map((file) => file.fileName);

        // Check if the file is already registered if not update existing table entry with new file data
        if (!fileList.includes(fileName)) {
          await this.indexedDB.datasets
            .where('tableName')
            .equals(tableName)
            .modify((prevData) => {
              prevData.files.push({ fileName, ...fileData });
            });
        }
      } else {
        // Add a new entry for the table with file data
        await this.indexedDB.datasets.add({
          tableName: tableName,
          files: [{ fileName, ...fileData }],
        });
      }

      // Store file buffer in the files object store
      await this.indexedDB.files.put({ fileName, buffer });
    }

    // Register the file buffer in the DuckDB instance
    return this.db.registerFileBuffer(fileName, buffer);
  }

  async getFileBuffer(fileName: string): Promise<Uint8Array | undefined> {
    if (!this.indexedDB) throw new Error('indexedDB is not initialized');

    const fileData = await this.indexedDB.files.get(fileName);
    return fileData?.buffer;
  }

  async mountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    if (!this.indexedDB) throw new Error('indexedDB is not initialized');

    for (const tableName of tableNames) {
      // Retrieve table data from indexedDB
      const tableData = await this.indexedDB?.datasets.get(tableName);

      // Retrieve file buffers from indexedDB and register them in DuckDB
      const promiseArr =
        tableData?.files.map(async (file) => {
          const fileData = await this.indexedDB?.files.get(file.fileName);

          if (fileData) {
            await this.db.registerFileBuffer(
              fileData.fileName,
              fileData.buffer
            );
          }
        }) || [];

      await Promise.all(promiseArr);
    }
  }

  async unmountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    if (!this.indexedDB) throw new Error('indexedDB is not initialized');

    for (const tableName of tableNames) {
      // Retrieve table data from indexedDB
      const tableData = await this.indexedDB?.datasets.get(tableName);

      // Unregister file buffers from DuckDB
      const promiseArr =
        tableData?.files.map(async (file) =>
          this.db.registerEmptyFileBuffer(file.fileName)
        ) || [];

      await Promise.all(promiseArr);
    }
  }
}
