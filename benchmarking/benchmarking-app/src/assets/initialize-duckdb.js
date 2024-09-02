import * as duckdb from 'duckdb-wasm';

export const initializeDuckDB = async (db) => {
  console.log('Initializing DuckDB');
  try {
    const JSDELIVR_BUNDLES = await duckdb.getJsDelivrBundles();
    console.log('Got JSDelivr bundles');

    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
    console.log('Selected bundle:', bundle);

    const wasmWorker = new Worker(bundle.mainWorker);
    const logger = new duckdb.ConsoleLogger();
    db = new duckdb.AsyncDuckDB(logger, wasmWorker);
    await db.instantiate(bundle.mainModule);

    console.log('DuckDB initialized successfully');
    return db;
  } catch (error) {
    console.error('Error initializing DuckDB:', error);
    throw error;
  }
};
