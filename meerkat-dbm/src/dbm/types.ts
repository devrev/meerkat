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
   * Instance-level callback for events that are NOT scoped to a single query.
   * It receives cross-query and load-time events — `query_queue_length` (a
   * queue gauge that spans queries), `json_to_buffer_conversion_duration`
   * (emitted at `registerJSON`/load time, outside any `queryWithTables` run),
   * and runner-side `clone_buffer_duration` (emitted inside the iframe runner,
   * which a per-query callback cannot cross via `postMessage`).
   *
   * For events that DO belong to one query run — `mount_file_buffer_duration`,
   * `query_execution_duration`, `query_queue_duration` — use the per-query
   * {@link QueryOptions.onEvent}. Routing is exclusive: when a query supplies
   * its own callback those events go there and NOT here; when it does not, they
   * fall back to this instance sink.
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

    /**
     * @description
     * Optional intermediate "recycle" before the final shutdown. After this much
     * inactivity (in ms), the DB is torn down and immediately re-instantiated —
     * reclaiming the worker's high-water memory while keeping the engine warm for
     * the next query. Fires at most once per idle period. Requires
     * `shutdownInactiveTime` to be set and larger than this value (the recycle is
     * the early action; the shutdown is the late one). Gated by `shouldRecycle`.
     * If not specified, no intermediate recycle happens.
     */
    recycleInactiveTime?: number;

    /**
     * @description
     * Predicate consulted before an idle recycle. Return false to skip the recycle
     * (e.g. when the engine holds no data worth reclaiming, so the recycle would
     * only cost a needless re-instantiate). Only consulted when `recycleInactiveTime`
     * is set. Defaults to always-recycle when omitted.
     */
    shouldRecycle?: () => boolean | Promise<boolean>;
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
   * Per-query event callback for the query-lifecycle events of THIS query —
   * `mount_file_buffer_duration`, `query_execution_duration` and
   * `query_queue_duration` — so a caller can scope those timings to a single
   * query run without correlating on `metadata`. Routing is exclusive: when
   * this callback is supplied those events go here and NOT to the instance-level
   * {@link DBMConstructorOptions.onEvent}.
   *
   * Cross-query and load-time events (`query_queue_length`,
   * `json_to_buffer_conversion_duration`, runner-side `clone_buffer_duration`)
   * are NOT delivered here — they have no single owning query and reach only the
   * instance sink.
   *
   * For the parallel/iframe path the callback stays in the calling window; the
   * runner manager dispatches to it by the id of the runner executing the query
   * (one query per runner), so no query id crosses `postMessage`.
   */
  onEvent?: (event: DBMEvent) => void;

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
