import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import {
  FileBufferStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from './file-manager-type';

export class MemoryDBFileManager implements FileManagerType {
  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;
  db: AsyncDuckDB;

  constructor(props: FileManagerConstructorOptions) {
    this.fetchTableFileBuffers = props.fetchTableFileBuffers;
    this.db = props.db;
  }

  async bulkRegisterFileBuffer(props: FileBufferStore[]): Promise<void> {
    const promiseArr = props.map((fileBuffer) =>
      this.registerFileBuffer(fileBuffer)
    );
    console.info('bulkRegisterFileBuffer', promiseArr);
    const output = await Promise.all(promiseArr);
    console.info('bulkRegisterFileBuffer done', output);
    console.info('bulkRegisterFileBuffer done', promiseArr);
  }

  registerFileBuffer(props: FileBufferStore): Promise<void> {
    console.info('registerFileBuffer', props);
    return this.db.registerFileBuffer(props.fileName, props.buffer);
  }

  getFileBuffer(name: string): Promise<Uint8Array> {
    throw new Error('Method not implemented.');
  }

  async mountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    // not needed for memory file manager
  }

  async unmountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    // not needed for memory file manager
  }
}