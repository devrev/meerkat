import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import {
  FileBufferStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';
import { DuckDBDatabase } from './duckdb-database';

export class IndexedDBFileManager implements FileManagerType {
  private indexedDB: DuckDBDatabase | null = null;

  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;
  db: AsyncDuckDB;

  constructor({ fetchTableFileBuffers, db }: FileManagerConstructorOptions) {
    this.fetchTableFileBuffers = fetchTableFileBuffers;
    this.db = db;
    this._openDatabase();
  }

  // Initialize the IndexedDB database
  private _openDatabase(): void {
    this.indexedDB = new DuckDBDatabase();

    // Clear the database when initialized
    this._flushDB();
  }

  // Clear all data from the IndexedDB database
  private _flushDB(): void {
    if (!this.indexedDB) throw new Error('indexedDB is not initialized');

    this.indexedDB.datasets.clear();
    this.indexedDB.files.clear();
  }

  // Bulk register file buffers in parallel
  async bulkRegisterFileBuffer(props: FileBufferStore[]): Promise<void> {
    const promiseArr = props.map((fileBuffer) =>
      this.registerFileBuffer(fileBuffer)
    );

    const output = await Promise.all(promiseArr);

    console.info('bulkRegisterFileBuffer done', output);
  }

  // Register a single file buffer
  async registerFileBuffer(props: FileBufferStore): Promise<void> {
    if (!this.indexedDB) console.error('indexedDB is not initialized');

    const { buffer, fileName, ...fileData } = props;

    if (this.indexedDB) {
      const tableData = await this.indexedDB.datasets.get(fileData.tableName);

      if (tableData) {
        const fileList = tableData.files.map((file) => file.fileName);

        // Check if the file is already registered if not update existing table entry with new file data
        if (!fileList.includes(fileName)) {
          await this.indexedDB.datasets
            .where('tableName')
            .equals(fileData.tableName)
            .modify((prevData) => {
              prevData.files.push({ fileName, ...fileData });
            });
        }
      } else {
        // Add a new entry for the table with file data
        await this.indexedDB.datasets.add({
          tableName: fileData.tableName,
          files: [{ fileName, ...fileData }],
        });
      }

      // Store file buffer in the files object store
      await this.indexedDB.files.put({ fileName, buffer });
    }

    // Register the file buffer with the DuckDB instance
    return this.db.registerFileBuffer(props.fileName, props.buffer);
  }

  // Retrieve a file buffer from indexedDB
  async getFileBuffer(fileName: string): Promise<Uint8Array | undefined> {
    if (!this.indexedDB) throw new Error('indexedDB is not initialized');

    const fileData = await this.indexedDB.files.get(fileName);
    return fileData?.buffer;
  }

  // Mount file buffers by table names
  async mountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    if (!this.indexedDB) throw new Error('indexedDB is not initialized');

    const promiseArr = tableNames.map(async (tableName) => {
      // Retrieve table data from indexedDB
      const tableData = await this.indexedDB?.datasets.get(tableName);

      await Promise.all(
        // Retrieve file buffers from indexedDB and register them in DuckDB
        tableData?.files.map(async (file) => {
          const fileData = await this.indexedDB?.files.get(file.fileName);

          if (fileData)
            return this.db.registerFileBuffer(tableName, fileData.buffer);
        }) || []
      );
    });

    await Promise.all(promiseArr);
  }

  // Unmount file buffers by table names
  async unmountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    const promiseArr = tableNames.map((tableName) =>
      this.db.registerEmptyFileBuffer(tableName)
    );

    await Promise.all(promiseArr);
  }
}
