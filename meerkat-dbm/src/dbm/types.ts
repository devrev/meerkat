import { FileManagerType } from '../file-manager/file-manager-type';
import { DBMEvent } from '../logger/event-types';
import { DBMLogger } from '../logger/logger-types';
import { TableWiseFiles } from '../types/common-types';
import { InstanceManagerType } from './instance-manager';

export interface DBMConstructorOptions {
  /**
   * @description
   * It handles all file operations such as file registration, file retrieval, file deletion
   * including mounting and unmounting of files in DuckDB instance,
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
}

/**
 * Configuration options for query execution.
 */
export type QueryOptions = {
  /**
   * A callback function which will be executed before the query is executed.
   * @param tableWiseFiles - An array of tables with associated file names.
   */
  preQuery?: (tableWiseFiles: TableWiseFiles[]) => Promise<void>;

  /**
   * Additional information for the query, which will be emitted in the DBM events.
   */
  metadata?: object;
};
