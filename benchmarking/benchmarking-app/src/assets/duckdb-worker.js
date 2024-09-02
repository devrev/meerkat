// duckdb-worker.js
console.log('Worker script started');

importScripts(
  'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm/dist/duckdb-browser.js'
);

console.log('DuckDB script imported');

let db;

async function initializeDuckDB() {
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
}

// Initialize DuckDB immediately
initializeDuckDB()
  .then(() => {
    console.log('DuckDB initialization complete');
    self.postMessage({ type: 'ready' });
  })
  .catch((error) => {
    console.error('Failed to initialize DuckDB:', error);
    self.postMessage({
      type: 'error',
      message: 'Failed to initialize DuckDB: ' + error.message,
    });
  });

self.onmessage = async function (e) {
  console.log('Received message in worker:', e.data);

  if (!db) {
    self.postMessage({
      type: 'error',
      message: 'DuckDB is not initialized yet',
    });
    return;
  }

  if (e.data.type === 'query') {
    try {
      const conn = await db.connect();
      const result = await conn.query(e.data.sql);
      await conn.close();
      console.log('Query executed successfully:', result);
      self.postMessage({ type: 'result', data: result });
    } catch (error) {
      console.error('Error executing query:', error);
      self.postMessage({
        type: 'error',
        message: 'Query error: ' + error.message,
      });
    }
  }
};

console.log('Worker script setup complete');
