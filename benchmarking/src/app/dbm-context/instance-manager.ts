import { InstanceManagerType } from '@devrev/meerkat-dbm';
import * as duckdb from '@duckdb/duckdb-wasm';
import { AsyncDuckDB, LogEntryVariant } from '@duckdb/duckdb-wasm';
const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

const jsBundle = {
  mvp: {
    mainModule: 'https://dbm.devrev-local.ai/duckdb/duckdb-mvp.wasm',
    mainWorker:
      'https://dbm.devrev-local.ai/duckdb/duckdb-browser-mvp.worker.js',
  },
  eh: {
    mainModule: 'https://dbm.devrev-local.ai/duckdb/duckdb-eh.wasm',
    mainWorker:
      'https://dbm.devrev-local.ai/duckdb/duckdb-browser-eh.worker.js',
  },
  coi: {
    mainModule: 'https://dbm.devrev-local.ai/duckdb/duckdb-coi.wasm',
    mainWorker:
      'https://dbm.devrev-local.ai/duckdb/duckdb-browser-coi.worker.js',
    pthreadWorker:
      'https://dbm.devrev-local.ai/duckdb/duckdb-browser-coi.pthread.worker.js',
  },
};

const isCOI = true;

export class InstanceManager implements InstanceManagerType {
  private db: AsyncDuckDB | null = null;

  private async initDB() {
    let worker: Worker, bundle: duckdb.DuckDBBundle;
    if (isCOI) {
      bundle = await duckdb.selectBundle(jsBundle);
      worker = new Worker(bundle.mainWorker!);
    } else {
      bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
      const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker!}");`], {
          type: 'text/javascript',
        })
      );
      worker = new Worker(worker_url);
    }

    const logger = {
      log: (msg: LogEntryVariant) => console.log(msg),
    };
    const db = new duckdb.AsyncDuckDB(logger, worker);

    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    db.open({ maximumThreads: 2 });

    const con = await db.connect();

    // URL.revokeObjectURL(worker_url);
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
