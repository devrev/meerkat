import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import { DuckDBDatabase } from './duckdb-database';
import {
  FileBufferStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from './file-manager-type';

const TABLE_OBJECT_STORE_NAME = 'tables';
const FILES_OBJECT_STORE_NAME = 'files';

interface IndexedDBFileManagerConstructorOptions
  extends FileManagerConstructorOptions {
  dbName?: string;
  version?: number;
}

export class IndexedDBFileManager implements FileManagerType {
  private indexedDB: DuckDBDatabase | null = null;

  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;
  db: AsyncDuckDB;

  constructor({
    fetchTableFileBuffers,
    db,
    dbName = 'DuckDBDatabase',
    version = 1,
  }: IndexedDBFileManagerConstructorOptions) {
    this.fetchTableFileBuffers = fetchTableFileBuffers;
    this.db = db;
    this._openDatabase(dbName, version);
  }

  private async _openDatabase(dbName: string, version: number): Promise<void> {
    this.indexedDB = new DuckDBDatabase();
    this._flushDB();
  }

  private async _flushDB(): Promise<void> {
    if (!this.indexedDB) throw new Error('indexedDB is not initialized');
  }

  async bulkRegisterFileBuffer(props: FileBufferStore[]): Promise<void> {
    const promiseArr = props.map((fileBuffer) =>
      this.registerFileBuffer(fileBuffer)
    );
    await Promise.all(promiseArr);
  }

  registerFileBuffer(props: FileBufferStore): Promise<void> {
    if (!this.indexedDB) throw new Error('indexedDB is not initialized');

    this.indexedDB.files.put({
      buffer: props.buffer,
      fileName: props.fileName,
    });

    return this.db.registerFileBuffer(props.fileName, props.buffer);
  }

  async getFileBuffer(fileName: string): Promise<Uint8Array | undefined> {
    if (!this.indexedDB) throw new Error('indexedDB is not initialized');

    const fileData = await this.indexedDB.files.get(fileName);

    return fileData?.buffer;
  }

  async mountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    if (!this.indexedDB) throw new Error('indexedDB is not initialized');

    // not needed for memory file manager
  }

  async unmountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    // not needed for memory file manager
  }
}
