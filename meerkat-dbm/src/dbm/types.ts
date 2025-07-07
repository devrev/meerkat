import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { FileManagerType } from '../file-manager/file-manager-type';
import { DBMEvent, DBMLogger } from '../logger';
import { Table } from '../types';
import { InstanceManagerType } from './instance-manager';

export interface DBMConstructorOptions {
  /**
   * @description
   * It handles all file operations such as file registration, file retrieval, file deletion
   * including mounting and unmounting of files in DuckDB instance.
   */
  fileManager: FileManagerType;

  /**
   * @description
   * It manages the lifecycle of the DuckDB database instance.
   * It provides methods for obtaining an initialized DuckDB instance and terminating the instance.
   */
  instanceManager: InstanceManagerType;

  /**
   * @description
   * Represents an logger instance, which will be used for logging messages throughout the DBM's execution.
   */
  logger: DBMLogger;

  /**
   * @description
   * A callback function that handles events emitted by the DBM.
   */
  onEvent?: (event: DBMEvent) => void;

  /**
   * @description
   * A callback function that handles shutdown event of DuckDB.
   */
  onDuckDBShutdown?: () => void;

  /**
   * @description
   * Configuration options for the DBM.
   */
  options?: {
    /**
     * @description
     * Denotes the shutdown time for the database after inactivity, in milliseconds.
     * If not specified, the DB will not shutdown.
     */
    shutdownInactiveTime?: number;
  };

  /**
   * @description
   * A callback function that will be executed after a new DuckDB connection is created.
   */
  onCreateConnection?: (
    connection: AsyncDuckDBConnection
  ) => void | Promise<void>;
}

/**
 * Configuration options for query execution.
 */
export interface QueryOptions {
  /**
   * @description
   * A callback function which will be executed before the query is executed.
   * @param tables - An array of tables with associated file names.
   */
  preQuery?: (tables: Table[]) => Promise<void>;

  /**
   * @description
   * Additional information for the query, which will be emitted in the DBM events.
   */
  metadata?: object;

  /**
   * @description
   * An AbortSignal object instance which can be used to abort the query execution.
   */
  signal?: AbortSignal;
}

export interface TableConfig {
  /**
   * @description
   * Name of the table.
   */
  name: string;
  /**
   * @description
   * Partitions of the table.
   */
  partitions?: string[];
}

export interface QueryQueueItem {
  query: string;
  tables: TableConfig[];
  promise: {
    resolve: (value: any) => void;
    reject: (reason?: any) => void;
  };
  /**
   * Timestamp indicating when the query was added to the queue.
   */
  timestamp: number;
  connectionId: string;
  options?: QueryOptions;
}

export interface TableLock {
  readersCount: number;
  writer: boolean;
  readersQueue: (() => void)[];
  writersQueue: (() => void)[];
}
