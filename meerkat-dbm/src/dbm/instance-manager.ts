import { AsyncDuckDB } from '@duckdb/duckdb-wasm';

export interface InstanceManagerType {
  /**
   * @description
   * Get the instance of the database after initializing it
   */
  getDB: () => Promise<AsyncDuckDB>;
  /**
   * @description
   * Terminate the database instance, primarily used for memory cleanup
   */
  terminateDB: () => Promise<void>;
}
