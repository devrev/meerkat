import { Database } from 'duckdb';

/**
 * DuckDBSingleton is designed as a Singleton class, which ensures that only one Database connection exists across the entire application.
 * This reduces the overhead involved in establishing new connections for each database request.
 *
 * The `getInstance` method returns a DuckDB Database instance. If an instance doesn't already exist, it creates one.
 * Subsequent calls to `getInstance` will return the pre-existing instance, ensuring there is only a single connection to the DuckDB instance throughout the use of the application.
 *
 * Usage: let duckDbConnection = DuckDBSingleton.getInstance();
 *
 * Note: In case of in-memory database, `new Database(':memory:')` in getInstance method. In-memory databases are faster for read/write operations
 * but are not persistent; they lose data as soon as the program ends or the machine is turned off, which is okay for our use-case.
 */
export class DuckDBSingleton {
  private static instance: Database;

  private constructor() {
    // private to prevent direct instantiation.
  }

  static getInstance(): Database {
    if (!DuckDBSingleton.instance) {
      DuckDBSingleton.instance = new Database(':memory:');
    }
    return DuckDBSingleton.instance;
  }
}

expory const 
