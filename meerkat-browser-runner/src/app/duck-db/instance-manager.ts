import { AsyncDuckDB, Logger } from '@devrev/duckdb-wasm';
import { InstanceManagerType } from '@devrev/meerkat-dbm';
import { DuckDbBundleManagerInstance } from './duck-db-bundle-manager';

export class InstanceManager implements InstanceManagerType {
  private db: AsyncDuckDB | null = null;

  private async initDB(): Promise<AsyncDuckDB> {
    const bundle = await DuckDbBundleManagerInstance.resolveBundle();

    if (!bundle.mainWorker) {
      throw new Error('No main worker found');
    }

    const worker = new Worker(bundle.mainWorker);

    const logger = {
      log: (message: string) => {
        //no-op
      },
    } as unknown as Logger;

    const duckDBInstance = new AsyncDuckDB(logger, worker);
    await duckDBInstance.instantiate(bundle.mainModule, bundle.pthreadWorker);

    /**
     * Creating the system schema to avoid conflicting views with the system datasets
     */
    const connection = await duckDBInstance.connect();
    await connection.query('create schema system;');
    await connection.close();

    return duckDBInstance;
  }

  async getDB() {
    if (!this.db) {
      console.info('Creating new DB');
      this.db = await this.initDB();
    }
    return this.db;
  }

  async terminateDB() {
    console.info('Terminating DB');
    await this.db?.terminate();
    this.db = null;
  }
}
