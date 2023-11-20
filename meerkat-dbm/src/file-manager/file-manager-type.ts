import { AsyncDuckDB } from '@duckdb/duckdb-wasm';

export interface FileBufferStore {
  tableName: string;
  fileName: string;
  buffer: Uint8Array;
  staleTime?: number;
  cacheTime?: number;
  metadata?: object;
}

export interface FileManagerType {
  bulkRegisterFileBuffer: (props: FileBufferStore[]) => Promise<void>;
  registerFileBuffer: (props: FileBufferStore) => Promise<void>;
  getFileBuffer: (name: string) => Promise<Uint8Array | undefined>;
  mountFileBufferByTableNames: (tableName: string[]) => Promise<void>;
  unmountFileBufferByTableNames: (tableName: string[]) => Promise<void>;
}

export interface FileManagerConstructorOptions {
  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;
  db: AsyncDuckDB;
}
