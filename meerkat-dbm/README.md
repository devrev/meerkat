# @devrev/meerkat-dbm

`@devrev/meerkat-dbm` is a browser-first database management layer built on [duckdb-wasm](https://github.com/duckdb/duckdb-wasm). It orchestrates query execution, manages DuckDB instances, caches files, persists data in browser storage, and optimizes memory usage to enable robust, high-performance data processing in web applications.

It's designed to bring the power of analytical SQL to the browser without compromising application stability or user experience. Whether you're building a data-intensive dashboard, an interactive reporting tool, or an offline-first application, Meerkat DBM provides the foundation you need.

## Architecture

Meerkat DBM is composed of several key components that work together to manage data and execute queries in the browser:

- **DBM (Database Manager)**: The central orchestrator. It receives queries, manages the execution lifecycle, and coordinates with other components.
- **FileManager**: Handles all aspects of data storage and retrieval. It can manage data in-memory or persist it to IndexedDB.
- **InstanceManager**: A user-implemented component responsible for creating, managing, and terminating `duckdb-wasm` instances.
- **DuckDB Instances**: The underlying `duckdb-wasm` engines where queries are executed, running in the main thread or in iFrames for parallelism.

This modular design provides a clear separation of concerns for managing complex data workflows in the browser.

## Why Meerkat DBM?

While `duckdb-wasm` is incredibly powerful, using it directly in a complex web application can be challenging. Meerkat DBM provides a structured, production-ready layer that solves common problems:

- **🧠 Memory Safety**: Prevents Out-Of-Memory (OOM) errors by managing query queues and memory swapping, ensuring your app remains stable even with large datasets.
- **💾 Persistence**: Offers seamless IndexedDB storage, allowing data to persist across browser sessions.
- **🗂️ Advanced File Management**: Simplifies handling of various file formats (Parquet, JSON, remote URLs) with intelligent caching and partitioning.
- **⚡ Parallel Processing**: Unlocks high-performance analytics with an optional iframe-based architecture for parallel query execution, preventing UI freezes.

## Key Features

### 🚀 Database Management

- **Instance Management**: Automated lifecycle management for DuckDB instances.
- **Connection Pooling**: Efficient management of database connections.
- **Query Queueing**: Intelligent scheduling of queries for sequential or parallel execution.
- **Table Locking**: Ensures thread-safe table operations during concurrent access.

### 📂 File Management

- **Multiple Formats**: Native support for Parquet, JSON, and URL-based files.
- **Bulk Operations**: High-performance APIs for registering and processing files in bulk.
- **Partitioning**: Support for table partitioning to efficiently manage and query large datasets.
- **Metadata Handling**: Rich metadata support for tables and files.
- **Multiple Storage Modes**: Flexible storage options, including in-memory and IndexedDB.

### 🔄 Parallelism & Communication

- **Inter-Window Messaging**: Seamless communication between the main thread and worker iframes.
- **Work Distribution**: Distributes query workloads across multiple isolated contexts for true parallel processing.
- **Event System**: A comprehensive event system for monitoring operations and state changes.

## File Manager Types

Meerkat DBM offers different file managers to suit your application's needs for performance, persistence, and memory usage.

| File Manager Type                   | Query Execution        | Storage   | Best For                               | Parallelism        | Persistence |
| ----------------------------------- | ---------------------- | --------- | -------------------------------------- | ------------------ | ----------- |
| **Memory File Manager**             | Sequential             | In-memory | Predictable memory use, OOM prevention | No                 | No          |
| **IndexedDB File Manager**          | Sequential             | IndexedDB | Large datasets, session persistence    | No                 | Yes         |
| **Parallel IndexedDB File Manager** | Parallel (via iframes) | IndexedDB | High performance + persistence         | Yes (iframe-based) | Yes         |

## Installation

```bash
npm install @devrev/meerkat-dbm @duckdb/duckdb-wasm
```

## Usage

### 1. Implement the InstanceManager

Meerkat DBM requires you to provide an `InstanceManager`. This decouples the library from a specific `duckdb-wasm` version, giving you full control over its instantiation and configuration.

```typescript
// src/instance-manager.ts
import * as duckdb from '@duckdb/duckdb-wasm';
import { InstanceManagerType } from '@devrev/meerkat-dbm';

// Select the desired DuckDB bundle
const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

export class InstanceManager implements InstanceManagerType {
  private db: duckdb.AsyncDuckDB | null = null;

  private async initDB(): Promise<duckdb.AsyncDuckDB> {
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

    const worker_url = URL.createObjectURL(new Blob([`importScripts("${bundle.mainWorker!}");`], { type: 'text/javascript' }));

    const worker = new Worker(worker_url);
    const logger = { log: (msg: any) => console.log(msg) };
    const db = new duckdb.AsyncDuckDB(logger, worker);

    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    URL.revokeObjectURL(worker_url);
    return db;
  }

  async getDB(): Promise<duckdb.AsyncDuckDB> {
    if (!this.db) {
      this.db = await this.initDB();
    }
    return this.db;
  }

  async terminateDB(): Promise<void> {
    if (this.db) {
      await this.db.terminate();
      this.db = null;
    }
  }
}
```

### 2. Example: Sequential Queries with Persistent Storage

This example uses the `DBM` with an `IndexedDBFileManager` for safe, sequential query execution and data persistence across browser sessions.

```typescript
import { DBM, IndexedDBFileManager } from '@devrev/meerkat-dbm';
import { InstanceManager } from './instance-manager';

// 1. Create the managers
const instanceManager = new InstanceManager();
const fileManager = new IndexedDBFileManager({
  instanceManager,
  // This function is called by Meerkat to fetch file data when needed
  fetchTableFileBuffers: async (tableName) => {
    // In a real app, you would fetch data from a indexdb
    return [];
  },
});

// 2. Create the DBM instance
const dbm = new DBM({
  instanceManager,
  fileManager,
  onEvent: (event) => console.info('DBM Event:', event),
  options: {
    // Automatically shut down the DuckDB instance after 5s of inactivity
    shutdownInactiveTime: 5000,
  },
});

// 3. Register data
await fileManager.registerJSON({
  tableName: 'sales',
  fileName: 'sales.json',
  json: [
    { id: 1, product: 'Laptop', amount: 1200 },
    { id: 2, product: 'Mouse', amount: 25 },
    { id: 3, product: 'Keyboard', amount: 75 },
  ],
});

// 4. Run a query
const results = await dbm.query('SELECT * FROM sales WHERE amount > 50');
console.log(results);

// Expected output:
// [
//   { id: 1, product: 'Laptop', amount: 1200 },
//   { id: 3, product: 'Keyboard', amount: 75 },
// ]
```

### 3. Example: Parallel Queries with IFrame Runners

This setup uses `DBMParallel` and `ParallelIndexedDBFileManager` for maximum performance, executing queries in parallel across multiple iframe-based DuckDB instances.

```typescript
import { DBMParallel, IFrameRunnerManager, ParallelIndexedDBFileManager } from '@devrev/meerkat-dbm';
import log from 'loglevel';
import { InstanceManager } from './instance-manager';

// 1. Create instance and file managers
const instanceManager = new InstanceManager();
const fileManager = new ParallelIndexedDBFileManager({
  instanceManager,
  fetchTableFileBuffers: async (table) => [],
  logger: log,
});

// 2. Set up the iframe runner manager for parallel execution
const iframeManager = new IFrameRunnerManager({
  // URL to the runner HTML file that hosts the DuckDB instance
  runnerURL: 'http://localhost:4204/runner/indexeddb-runner.html',
  origin: 'http://localhost:4204',
  totalRunners: 4, // Number of parallel iframes
  fetchTableFileBuffers: async (table) => [],
  logger: log,
});

// 3. Create the parallel DBM instance
const parallelDBM = new DBMParallel({
  instanceManager,
  fileManager,
  iFrameRunnerManager: iframeManager,
  logger: log,
  options: {
    shutdownInactiveTime: 10000,
  },
});

// 4. Register data
await fileManager.bulkRegisterJSON([
  {
    tableName: 'transactions',
    fileName: 'transactions.json',
    json: [
      { id: 1, product_id: 101, amount: 1200 },
      { id: 2, product_id: 102, amount: 25 },
    ],
  },
  {
    tableName: 'products',
    fileName: 'products.json',
    json: [
      { id: 101, name: 'Laptop', category: 'Electronics' },
      { id: 102, name: 'Mouse', category: 'Accessories' },
    ],
  },
]);

// 5. Execute queries in parallel
const [transactions, analysis] = await Promise.all([
  parallelDBM.query('SELECT * FROM transactions WHERE amount > 100'),
  parallelDBM.query(`
    SELECT p.category, COUNT(*) as product_count 
    FROM transactions t 
    JOIN products p ON t.product_id = p.id 
    GROUP BY p.category
  `),
]);

console.log('High-Value Transactions:', transactions);
console.log('Category Analysis:', analysis);
```

## API Overview

- **`DBM`**: The main class for sequential query execution and database management.
- **`DBMParallel`**: Extends `DBM` to support parallel query execution using iframe runners.
- **`MemoryFileManager`**: An in-memory file manager. Data is lost when the session ends.
- **`IndexedDBFileManager`**: A file manager that persists data in IndexedDB.
- **`ParallelIndexedDBFileManager`**: An IndexedDB-based file manager optimized for use with `DBMParallel`.
- **`IFrameRunnerManager`**: Manages the pool of iframe runners for parallel query execution.
- **`InstanceManagerType`**: The interface your custom `InstanceManager` must implement to manage the `duckdb-wasm` lifecycle.

## License

This project is licensed under the MIT License.
