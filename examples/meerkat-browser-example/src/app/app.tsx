// duckdb_wasm and duckdb_wasm_next will be available once the promises are resolved

import { useEffect } from 'react';
import NxWelcome from './nx-welcome';

import * as duckdb from '@kunal-mohta/duckdb-wasm';
import { fileLoadingBenchmark } from './benchmarking/file-loading';

const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
let didAlreadyRender = false;
export function App() {
  // Ensure MANUAL_BUNDLES is available before using it

  useEffect(() => {
    (async () => {
      if (didAlreadyRender) {
        return;
      }
      didAlreadyRender = true;
      // Select a bundle based on browser checks
      const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

      const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker!}");`], {
          type: 'text/javascript',
        })
      );
      const worker = new Worker(worker_url);
      const logger = new duckdb.ConsoleLogger();
      const start = performance.now();
      const db = new duckdb.AsyncDuckDB(logger, worker);
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      const end = performance.now();
      console.log(`DuckDB-wasm took ${end - start} milliseconds to load`);
      const c = await db.connect();

      await fileLoadingBenchmark(db);
    })();
    // Instantiate the asynchronus version of DuckDB-wasm
  }, []);

  return (
    <div>
      <NxWelcome title="meerkat-browser-example" />
    </div>
  );
}

export default App;
