import { InstanceManagerType } from '../dbm/instance-manager';
import { TableWiseFiles } from '../types/common-types';
import { Table } from '../types/db-table-types';

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
   * Retrieves the file buffer associated with a given file name.
   * @param fileName - The name of the file buffer.
   * @returns A Promise resolving to a Uint8Array if found.
   */
  getFileBuffer: (fileName: string) => Promise<Uint8Array | undefined>;

  /**
   * Mounts file buffers based on an array of table names in DuckDB.
   * @param tableNames - An array of table names.
   */
  mountFileBufferByTableNames: (tableNames: string[]) => Promise<void>;

  /**
   * Retrieves the data for a specific table.
   * @param tableName - The name of the table.
   * @returns A Promise resolving to a Table object or undefined if not found.
   */
  getTableData: (tableName: string) => Promise<Table | undefined>;

  /**
   * Sets metadata for a specific table.
   * @param tableName - The name of the table.
   * @param metadata - The metadata object to set.
   */
  setTableMetadata: (tableName: string, metadata: object) => Promise<void>;

  /**
   * Drops files associated with a table by their names.
   * @param tableName - The name of the table.
   * @param fileNames - An array of file names to drop.
   */
  dropFilesByTableName: (
    tableName: string,
    fileNames: string[]
  ) => Promise<void>;

  /**
   * Retrieves file names associated with specified tables.
   * @param tableNames - An array of table names.
   * @returns A Promise resolving to an array of objects containing table names and associated files.
   */
  getFilesNameForTables: (tableNames: string[]) => Promise<TableWiseFiles[]>;

  /**
   * Handler to be executed on database shutdown.
   * @returns A Promise resolving when the shutdown handling is complete.
   */
  onDBShutdownHandler: () => Promise<void>;
}

export interface FileBufferStore {
  tableName: string;
  fileName: string;
  buffer: Uint8Array;
  staleTime?: number;
  cacheTime?: number;
  metadata?: object;
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

