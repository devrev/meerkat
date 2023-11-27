import { InstanceManagerType } from '@devrev/meerkat-dbm';
import * as duckdb from '@duckdb/duckdb-wasm';
import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

export class InstanceManager implements InstanceManagerType {
  private db: AsyncDuckDB | null = null;

  private async initDB() {
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker!}");`], {
        type: 'text/javascript',
      })
    );

    // Instantiate the asynchronus version of DuckDB-wasm
    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(worker_url);
    return db;
  }

  async getDB() {
    if (!this.db) {
      this.db = await this.initDB();
    }
    return this.db;
  }

  async terminateDB() {
    await this.db?.terminate();
    this.db = null;
  }
}
