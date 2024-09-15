import { DBM, FileManagerType } from '@devrev/meerkat-dbm';
import * as duckdb from '@duckdb/duckdb-wasm';
import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import React, { useState } from 'react';
import { useClassicEffect } from '../hooks/use-classic-effect';
const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

export const DBMContext = React.createContext<{
  dbm: DBM;
  fileManager: FileManagerType;
}>(null as any);

export const useAsyncDuckDB = () => {
  const [dbState, setdbState] = useState<AsyncDuckDB | null>(null);

  useClassicEffect(() => {
    (async () => {
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
      setdbState(db);
    })();
  }, []);
  return dbState;
};
