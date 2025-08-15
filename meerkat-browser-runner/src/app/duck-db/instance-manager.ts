import { InstanceManagerType } from '@devrev/meerkat-dbm';
import * as duckdb from '@duckdb/duckdb-wasm';
import { AsyncDuckDB, LogEntryVariant } from '@duckdb/duckdb-wasm';
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
    const logger = {
      log: (msg: LogEntryVariant) => console.log(msg),
    };
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    URL.revokeObjectURL(worker_url);
    return db;
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
