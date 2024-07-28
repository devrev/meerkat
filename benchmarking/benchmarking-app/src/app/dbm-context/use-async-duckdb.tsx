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
      const jsBundle = {
        mvp: {
          mainModule: 'http://localhost:4200/assets/duckdb/duckdb-mvp.wasm',
          mainWorker:
            'http://localhost:4200/assets/duckdb/duckdb-browser-mvp.worker.js',
        },
        eh: {
          mainModule: 'http://localhost:4200/assets/duckdb/opfs/duckdb-eh.wasm',
          mainWorker:
            'http://localhost:4200/assets/duckdb/opfs/duckdb-browser-eh.worker.js',
        },
        coi: {
          mainModule: 'http://localhost:4200/assets/duckdb/duckdb-coi.wasm',
          mainWorker:
            'http://localhost:4200/assets/duckdb/duckdb-browser-coi.worker.js',
          pthreadWorker:
            'http://localhost:4200/assets/duckdb/duckdb-browser-coi.pthread.worker.js',
        },
      };

      const bundle = await duckdb.selectBundle(jsBundle);
      console.log(bundle);
      // const worker_url = URL.createObjectURL(
      //   new Blob([`importScripts("${bundle.mainWorker!}");`], {
      //     type: 'text/javascript',
      //   })
      // );

      // Instantiate the asynchronus version of DuckDB-wasm
      const worker = new Worker(bundle.mainWorker!);
      const logger = new duckdb.ConsoleLogger();
      const db = new duckdb.AsyncDuckDB(logger, worker);
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

      // await db.open({ maximumThreads: 4 });

      const dirHandle = await navigator.storage.getDirectory();
      // console.log(createOPFSFileHandle);
      // const dbHandle = await createOPFSFileHandle(`attach.db`, dirHandle, {
      //   create: true,
      //   emptyAsAbsent: true,
      // });
      // const walHandle = await createOPFSFileHandle(`attach.db.wal`, dirHandle, {
      //   create: true,
      //   emptyAsAbsent: true,
      // });

      db.open({
        // path: 'opfs://test.db',
        accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
      });
      // URL.revokeObjectURL(worker_url);
      setdbState(db);
    })();
  }, []);
  return dbState;
};
