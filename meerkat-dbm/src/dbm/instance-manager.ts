import { AsyncDuckDB } from '@duckdb/duckdb-wasm';

export interface InstanceManagerType {
  /**
   * Get the instance of the database after initializing it
   */
  getDB: () => Promise<AsyncDuckDB>;
  /**
   * Terminate the database instance, primarily used for memory cleanup
   */
  terminateDB: () => Promise<void>;
}
