// duckdb_wasm and duckdb_wasm_next will be available once the promises are resolved

import { useEffect } from 'react';
import NxWelcome from './nx-welcome';

import * as duckdb from '@duckdb/duckdb-wasm';
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

      // const output = await c.query('SELECT 42');
      // console.log('output', output);

      // const file: any = await axios({
      //   method: 'get',
      //   url: 'http://localhost:3333/api/file',
      //   responseType: 'arraybuffer',
      // });
      // await db.registerFileBuffer('taxi.parquet', new Uint8Array(file.data));

      // await db.collectFileStatistics('taxi.parquet', true);

      // const temp = await db.exportFileStatistics('taxi.parquet');
      // console.info('temp', temp);
      // file = null;

      // const ar1 = await c.query('.files');
      // const p1 = ar1.toArray().map((row) => row.toJSON());
      // console.info('p1', p1);
      // await c.query('DROP TABLE IF EXISTS taxi');
      // await c.query('DROP TABLE IF EXISTS taxi.parquet');
      // const queryStart = performance.now();
      // const arrowResult = await c.query(
      //   `SELECT CAST(count(*) as VARCHAR) as total_count FROM taxi.parquet`
      // );
      // const queryEnd = performance.now();
      // console.log(
      //   `DuckDB-wasm took ${queryEnd - queryStart} milliseconds to query`
      // );
      // const parsedOutputQuery = arrowResult
      //   .toArray()
      //   .map((row) => row.toJSON());
      // console.info('parsedOutputQuery', parsedOutputQuery);

      // await db.dropFile('taxi.parquet');

      // await c.close();

      // const temp1 = await db.exportFileStatistics('taxi.parquet');
      // console.info('temp1', temp1);

      // const ar2 = await c.query('.files');
      // const p2 = ar2.toArray().map((row) => row.toJSON());
      // console.info('p1', p2);
      // await db.reset();
      // await db.terminate();
      // await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

      // const start1 = performance.now();
      // const worker1 = new Worker(worker_url);

      // const db1 = new duckdb.AsyncDuckDB(logger, worker1);
      // await db1.instantiate(bundle.mainModule, bundle.pthreadWorker);
      // const end1 = performance.now();
      // console.log(`DuckDB-wasm took ${end1 - start1} milliseconds to load`);
      // // file = null;
      // console.info('Dropped the file');
      // await db.reset();

      // // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // //@ts-ignore
      // window.thefile = file.data;
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
