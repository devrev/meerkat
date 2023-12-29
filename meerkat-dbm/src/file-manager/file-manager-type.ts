import { InstanceManagerType } from '../dbm/instance-manager';

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
  getTableByName(tableName: string): Promise<Table | undefined>;
  dropFilesByTableName(tableName: string, fileNames: string[]): Promise<void>;
  getFilesNameForTables(tableNames: string[]): Promise<
    {
      tableName: string;
      files: string[];
    }[]
  >;
  onDBShutdownHandler: () => Promise<void>;
}

export interface FileManagerConstructorOptions {
  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;
  instanceManager: InstanceManagerType;
  options?: {
    /**
     * Maximum size of the file in DB in bytes
     */
    maxFileSize?: number;
  };
}

export const FILE_TYPES = {
  PARQUET: 'parquet',
} as const;

export type FileType = (typeof FILE_TYPES)[keyof typeof FILE_TYPES];

export interface Table {
  tableName: string;
  files: FileData[];
  totalSize?: number;
  metadata?: object;
}

export interface FileData {
  fileName: string;
  fileType?: FileType;
  size?: number;
  staleTime?: number;
  cacheTime?: number;
  metadata?: object;
}

export interface File {
  fileName: string;
  buffer: Uint8Array;
}
