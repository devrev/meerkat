import * as duckdb from '@devrev/duckdb-wasm';
import { AsyncDuckDB } from '@devrev/duckdb-wasm';
import { InstanceManagerType } from '@devrev/meerkat-dbm';
import { DuckDbBundleManagerInstance } from './duck-db-bundle-manager';
const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

export class InstanceManager implements InstanceManagerType {
  private db: AsyncDuckDB | null = null;

  private async initDB(): Promise<AsyncDuckDB> {
    return new Promise((resolve, reject) => {
      DuckDbBundleManagerInstance.resolveBundle()
        .then(async (bundle) => {
          if (bundle.mainWorker) {
            const worker = new Worker(bundle.mainWorker);

            /**
             * Duckdb logger is having memory leak issues
             */
            const logger = {
              log: (message: string) => {
                //no-op
              },
            } as unknown as duckdb.Logger;

            console.log('worker', worker);
            const duckDBInstance = new AsyncDuckDB(logger, worker);
            await duckDBInstance.instantiate(
              bundle.mainModule,
              bundle.pthreadWorker
            );

            /**
             * creating the system schema to avoid confilcting views with the system datasets
             */
            const connection = await duckDBInstance.connect();
            await connection.query('create schema system;');
            await connection.close();

            resolve(duckDBInstance);
          } else {
            reject('No main worker found');
          }
        })
        .catch((error) => reject(error));
    });
  }

  async getDB() {
    if (!this.db) {
      console.info('Creating new DB');
      this.db = await this.initDB();
    }
    return this.db;
  }

  async terminateDB() {
    console.info('terminateDB');
    await this.db?.terminate();
    this.db = null;
  }
}
