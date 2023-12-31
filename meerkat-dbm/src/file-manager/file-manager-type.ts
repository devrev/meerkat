import { InstanceManagerType } from '../dbm/instance-manager';
import { Table, TableWiseFiles } from '../types';

export interface FileManagerType {
  /**
   * @description
   * Registers multiple file buffers in the file manager.
   * @param props - An array of FileBufferStore objects.
   */
  bulkRegisterFileBuffer: (props: FileBufferStore[]) => Promise<void>;

  /**
   * @description
   * Registers a single file buffer in the file manager.
   * @param props - The FileBufferStore object to register.
   */
  registerFileBuffer: (props: FileBufferStore) => Promise<void>;

  /**
   * @description
   * Registers a single JSON file in the file manager.
   * @param props - The FileJsonStore object to register.
   */
  registerJSON: (props: FileJsonStore) => Promise<void>;

  /**
   * @description
   * Retrieves the file buffer associated with a given file name.
   * @param fileName - The name of the file buffer.
   * @returns Uint8Array if found.
   */
  getFileBuffer: (fileName: string) => Promise<Uint8Array | undefined>;

  /**
   * @description
   * Mounts or registers file buffers based on an array of table names in DuckDB.
   * @param tableNames - An array of table names.
   */
  mountFileBufferByTableNames: (tableNames: string[]) => Promise<void>;

  /**
   * @description
   * Retrieves the data for a specific table.
   * @param tableName - The name of the table.
   * @returns Table object if found.
   */
  getTableData: (tableName: string) => Promise<Table | undefined>;

  /**
   * @description
   * Sets metadata for a specific table.
   * @param tableName - The name of the table.
   * @param metadata - The metadata object to set.
   */
  setTableMetadata: (tableName: string, metadata: object) => Promise<void>;

  /**
   * @description
   * Drops files associated with a table by their names.
   * @param tableName - The name of the table.
   * @param fileNames - An array of file names to drop.
   */
  dropFilesByTableName: (
    tableName: string,
    fileNames: string[]
  ) => Promise<void>;

  /**
   * @description
   * Retrieves file names associated with specified tables.
   * @param tableNames - An array of table names.
   * @returns Array of objects containing table names and associated files.
   */
  getFilesNameForTables: (tableNames: string[]) => Promise<TableWiseFiles[]>;

  /**
   * @description
   * Handler to be executed on database shutdown.
   */
  onDBShutdownHandler: () => Promise<void>;
}

export type BaseFileStore = {
  tableName: string;
  fileName: string;
  staleTime?: number;
  cacheTime?: number;
  metadata?: object;
};

export type FileBufferStore = BaseFileStore & {
  buffer: Uint8Array;
};

export type FileJsonStore = BaseFileStore & {
  json: object;
};


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

