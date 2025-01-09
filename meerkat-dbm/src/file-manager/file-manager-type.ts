import { InstanceManagerType } from '../dbm/instance-manager';
import { TableConfig } from '../dbm/types';
import { DBMEvent, DBMLogger } from '../logger';
import { Table, TableWiseFiles } from '../types';

export interface FileManagerConstructorOptions {
  /**
   * @description
   * It manages the lifecycle of the DuckDB database instance.
   * It provides methods for obtaining an initialized DuckDB instance and terminating the instance.
   */
  instanceManager: InstanceManagerType;

  /**
   * @description
   * Represents an logger instance, which will be used for logging messages throughout the File Manager's execution.
   */
  logger?: DBMLogger;

  /**
   * @description
   * A callback function that handles events emitted by the File Manager.
   */
  onEvent?: (event: DBMEvent) => void;

  /**
   * @description
   * Configuration options for the File Manager.
   */
  options?: {
    /**
     * Maximum size of the file in DB in bytes
     */
    maxFileSize?: number;
  };

  /**
   * @description
   * Returns the file buffers for a given table name.
   */
  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;
}

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
   * Registers multiple JSON files in the file manager.
   * It converts JSON objects to Uint8Arrays by writing them to Parquet files in a DuckDB database and registers them.
   * Also emits an event with the time taken for the conversion.
   * @param props - An array of FileJsonStore objects.
   */
  bulkRegisterJSON: (props: FileJsonStore[]) => Promise<void>;

  /**
   * @description
   * Registers a single JSON file in the file manager.
   * It converts a JSON object to a Uint8Array by writing it to a Parquet file in a DuckDB database and registers it.
   * Also emits an event with the time taken for the conversion.
   * @param props - The FileJsonStore object to register.
   */
  registerJSON: (props: FileJsonStore) => Promise<void>;

  /**
   * @description
   * Mounts or registers file buffers based on an array of table names in DuckDB.
   * @param tables - An array of table.
   */
  mountFileBufferByTables: (tables: TableConfig[]) => Promise<void>;

  /**
   * @description
   * Retrieves the data for a specific table.
   * @param tableName - The name of the table.
   * @returns Table object if found.
   */
  getTableData: (table: TableConfig) => Promise<Table | undefined>;

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
   * @param tables - An array of tables.
   * @returns Array of objects containing table names and associated files.
   */
  getFilesNameForTables: (tables: TableConfig[]) => Promise<TableWiseFiles[]>;

  /**
   * @description
   * Handler to be executed on database shutdown.
   */
  onDBShutdownHandler: () => Promise<void>;
}

export type BaseFileStore = {
  tableName: string;
  fileName: string;
  partitions?: string[];
  metadata?: object;
};

export type FileBufferStore = BaseFileStore & {
  buffer: Uint8Array;
};

export type FileJsonStore = BaseFileStore & {
  json: object;
};
