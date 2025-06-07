# Meerkat DBM

Meerkat-dbm is a wrapper built on top of [duckdb-wasm](https://github.com/duckdb/duckdb-wasm), designed to manage the database. It handles query execution, manages connections, fetches files, maintains files in cache/browser storage, and oversees swap memory.

## Overview

Meerkat DBM addresses critical duckdb-wasm limitations:

- **Memory Safety**: Prevents OOM errors through sequential execution
- **Persistence**: IndexedDB storage for data across sessions
- **File Management**: Handles Parquet, JSON, and URL-based files
- **Parallel Processing**: Optional iframe-based parallelism

## Installation

```bash
npm install @devrev/meerkat-dbm
```

## Key Features

### 🚀 Database Management

- **Instance Management**: Automated DuckDB instance lifecycle
- **Connection Pooling**: Efficient connection management
- **Query Queue**: Parallel query execution with intelligent scheduling
- **Table Locking**: Thread-safe table operations

### 📁 File Management

- **Multiple Formats**: Support for Parquet, JSON, and URL-based files
- **Bulk Operations**: Efficient bulk file registration and processing
- **Partitioning**: Table partitioning support for large datasets
- **Metadata Handling**: Rich metadata support for tables and files
- **Multiple Storage Modes**: Choose between different file manager types based on your needs

### 🔄 Window Communication

- **Inter-Window Messaging**: Seamless communication between main and worker threads
- **Parallel Processing**: Distribute work across multiple contexts
- **Event Handling**: Comprehensive event system for monitoring operations

## File Manager Types

### Memory File Manager

- **Execution**: Sequential queries
- **Storage**: In-memory
- **Use Case**: Predictable memory usage, OOM prevention

### IndexedDB File Manager

- **Execution**: Sequential queries
- **Storage**: IndexedDB (persistent)
- **Use Case**: Large datasets, session persistence

### Parallel IndexedDB File Manager

- **Execution**: Parallel queries via iframes
- **Storage**: IndexedDB (persistent)
- **Use Case**: High performance + persistence

## Key Features

- **Sequential Execution**: Prevents OOM errors
- **IndexedDB Storage**: Persistent data across sessions
- **Memory Swapping**: LRU and query-aware caching
- **Parallel Processing**: iframe-based architecture for performance

## Usage

### Example 1: DBM with Memory File Manager

Sequential execution with in-memory storage:

```typescript
import { DBM, MemoryFileManager, InstanceManager } from '@devrev/meerkat-dbm';

// Initialize components
const instanceManager = new InstanceManager();

const fileManager = new MemoryFileManager({
  instanceManager,
  logger,
  fetchTableFileBuffers: async (tableName) => {
    const response = await fetch(`/api/data/${tableName}.parquet`);
    const buffer = new Uint8Array(await response.arrayBuffer());
    return [{ tableName, fileName: `${tableName}.parquet`, buffer }];
  },
});

// Create DBM instance
const dbm = new DBM({
  fileManager,
  instanceManager,
  logger,
});

// Register data
await fileManager.registerJSON({
  tableName: 'sales',
  fileName: 'sales.parquet',
  json: [
    { id: 1, product: 'Laptop', amount: 1200 },
    { id: 2, product: 'Mouse', amount: 25 },
  ],
});

// Execute queries sequentially
const results = await dbm.query('SELECT * FROM sales WHERE amount > 50');
console.log(results);
```

### Example 2: Parallel DBM with Parallel IndexedDB File Manager

Parallel execution with persistent storage:

```typescript
import { ParallelDBM, ParallelIndexedDBFileManager, InstanceManager, Logger } from '@devrev/meerkat-dbm';

// Initialize components
const logger = new Logger();
const instanceManager = new InstanceManager({ logger });
const fileManager = new ParallelIndexedDBFileManager({
  instanceManager,
  logger,
  fetchTableFileBuffers: async (tableName) => {
    const response = await fetch(`/api/data/${tableName}.parquet`);
    const buffer = new Uint8Array(await response.arrayBuffer());
    return [{ tableName, fileName: `${tableName}.parquet`, buffer }];
  },
});

// Create Parallel DBM instance
const parallelDBM = new ParallelDBM({
  fileManager,
  instanceManager,
  logger,
});

// Register data with partitioning
await fileManager.bulkRegisterJSON([
  {
    tableName: 'transactions',
    fileName: 'transactions.parquet',
    partitions: ['2024'],
    json: [
      { id: 1, product: 'Laptop', amount: 1200 },
      { id: 2, product: 'Mouse', amount: 25 },
    ],
  },
  {
    tableName: 'products',
    fileName: 'products.parquet',
    json: [
      { id: 1, name: 'Laptop', category: 'Electronics' },
      { id: 2, name: 'Mouse', category: 'Electronics' },
    ],
  },
]);

// Execute queries in parallel
const [sales, analysis] = await Promise.all([
  parallelDBM.query('SELECT * FROM transactions WHERE amount > 100'),
  parallelDBM.query(`
    SELECT p.category, COUNT(*) as count 
    FROM transactions t 
    JOIN products p ON t.product = p.name 
    GROUP BY p.category
  `),
]);

console.log('Sales:', sales);
console.log('Analysis:', analysis);
```

## API

```typescript
// Core imports
import { DBM, MemoryFileManager, ParallelDBM, ParallelIndexedDBFileManager } from '@devrev/meerkat-dbm';

// File registration
await fileManager.registerJSON({ tableName, fileName, json });
await fileManager.bulkRegisterJSON([...]);

// Query execution
const result = await dbm.query('SELECT * FROM table');
```

## Repository

[GitHub](https://github.com/devrev/meerkat)
