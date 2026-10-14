import { DBM, InstanceManagerType } from '@devrev/meerkat-dbm';

/**
 * One memory snapshot of the DuckDB WASM engine + the browser process. Because
 * the benchmark app is cross-origin isolated (COOP/COEP set in vite.config), we
 * can call measureUserAgentSpecificMemory() — which reports the TRUE per-context
 * heap including the DuckDB WASM worker, the number performance.memory (main
 * thread only) misses.
 */
export interface MemorySnapshot {
  iteration: number;
  atMs: number;
  label: string;
  /** DuckDB buffer-manager total, bytes — sum over duckdb_memory() tags. */
  duckDbUsedBytes: number | null;
  /** Per-tag DuckDB memory (PARQUET_READER, HASH_TABLE, ORDER_BY, WINDOW, …). */
  duckDbByTag: Record<string, number>;
  /** Attached file bytes from DuckDB's WASM FS (globFiles). The resident parquet
   * footprint the recycle/shutdown reclaims. */
  duckDbFileBytes: number;
  duckDbFileCount: number;
  /** measureUserAgentSpecificMemory() total across all contexts (incl. WASM
   * worker) — the real browser-tab JS+WASM memory. null if unavailable. */
  uaTotalBytes: number | null;
  /** Per-context breakdown from measureUserAgentSpecificMemory (type → bytes). */
  uaByType: Record<string, number>;
  /** performance.memory main-thread JS heap (Chromium) — for comparison. */
  jsHeapUsedBytes: number | null;
}

interface GlobbableDuckDb {
  globFiles?: (path: string) => Promise<Array<{ fileName: string; fileSize?: number }>>;
  dropFiles?: (names?: string[]) => Promise<unknown>;
  reset?: () => Promise<unknown>;
}

const toBytes = (value: unknown): number | null => {
  if (value === undefined || value === null) return null;
  if (typeof value === 'number' || typeof value === 'bigint') return Number(value);
  const n = Number(String(value));
  return Number.isFinite(n) ? n : null;
};

/** Cross-origin-isolated memory measurement — the true per-context heap. */
const measureUaMemory = async (): Promise<{ total: number | null; byType: Record<string, number> }> => {
  const measure = (performance as unknown as {
    measureUserAgentSpecificMemory?: () => Promise<{
      bytes: number;
      breakdown: Array<{ bytes: number; types: string[] }>;
    }>;
  }).measureUserAgentSpecificMemory;

  if (!measure || !(globalThis as unknown as { crossOriginIsolated?: boolean }).crossOriginIsolated) {
    return { total: null, byType: {} };
  }
  try {
    const result = await measure();
    const byType: Record<string, number> = {};
    for (const b of result.breakdown) {
      const key = b.types.join('+') || 'unknown';
      byType[key] = (byType[key] ?? 0) + b.bytes;
    }
    return { total: result.bytes, byType };
  } catch {
    return { total: null, byType: {} };
  }
};

export const takeMemorySnapshot = async (
  iteration: number,
  label: string,
  dbm: DBM,
  instanceManager: InstanceManagerType,
): Promise<MemorySnapshot> => {
  const perfMemory = (performance as unknown as { memory?: { usedJSHeapSize: number } }).memory;

  const snapshot: MemorySnapshot = {
    iteration,
    atMs: Date.now(),
    label,
    duckDbUsedBytes: null,
    duckDbByTag: {},
    duckDbFileBytes: 0,
    duckDbFileCount: 0,
    uaTotalBytes: null,
    uaByType: {},
    jsHeapUsedBytes: perfMemory?.usedJSHeapSize ?? null,
  };

  // duckdb_memory() per-tag breakdown via the DBM raw query.
  try {
    const res = await dbm.query(
      'SELECT tag, (memory_usage_bytes + temporary_storage_bytes)::BIGINT AS used FROM duckdb_memory()',
    );
    const rows = res.toArray().map((r: { toJSON: () => Record<string, unknown> }) => r.toJSON());
    let total = 0;
    for (const row of rows) {
      const tag = String(row['tag'] ?? '');
      const used = Number(row['used'] ?? 0);
      if (Number.isFinite(used) && used > 0) {
        snapshot.duckDbByTag[tag] = used;
        total += used;
      }
    }
    snapshot.duckDbUsedBytes = total;
  } catch {
    // duckdb_memory() unsupported — leave null
  }

  // Attached files from the WASM FS (authoritative resident buffer set).
  try {
    const db = (await instanceManager.getDB()) as unknown as GlobbableDuckDb;
    if (db.globFiles) {
      const files = await db.globFiles('*');
      snapshot.duckDbFileCount = files.length;
      snapshot.duckDbFileBytes = files.reduce((sum, f) => sum + (f.fileSize ?? 0), 0);
    }
  } catch {
    // leave 0
  }

  const ua = await measureUaMemory();
  snapshot.uaTotalBytes = ua.total;
  snapshot.uaByType = ua.byType;

  return snapshot;
};

/** dropFiles() — free registered parquet buffers, keep engine warm. */
export const dropAllBuffers = async (instanceManager: InstanceManagerType): Promise<void> => {
  const db = (await instanceManager.getDB()) as unknown as GlobbableDuckDb;
  await db.dropFiles?.();
};

/** reset() — reset the DB (drop catalog + buffers + cache), keep worker warm. */
export const resetEngine = async (instanceManager: InstanceManagerType): Promise<void> => {
  const db = (await instanceManager.getDB()) as unknown as GlobbableDuckDb;
  await db.reset?.();
};

/** terminate — kill the worker (full shutdown; next query pays cold boot). */
export const terminateEngine = async (instanceManager: InstanceManagerType): Promise<void> => {
  await instanceManager.terminateDB();
};

export const formatBytes = (bytes: number | null): string => {
  if (bytes === null) return '—';
  if (bytes < 1024) return `${bytes} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let value = bytes / 1024;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  return `${value.toFixed(1)} ${units[unitIndex]}`;
};

/** Query variants over the taxi view — each stresses a different memory tag. */
export const POC_QUERIES: Array<{ id: string; label: string; sql: string }> = [
  { id: 'scan', label: 'scan + limit', sql: 'SELECT * FROM taxi LIMIT 1000' },
  { id: 'count', label: 'count(*)', sql: 'SELECT count(*) AS n FROM taxi' },
  {
    id: 'groupby',
    label: 'group by license',
    sql: 'SELECT hvfhs_license_num, count(*) AS trips, avg(base_passenger_fare) AS avg_fare FROM taxi GROUP BY hvfhs_license_num ORDER BY trips DESC',
  },
  {
    id: 'groupby-hi',
    label: 'group by PU+DO (high card)',
    sql: 'SELECT PULocationID, DOLocationID, count(*) AS n FROM taxi GROUP BY PULocationID, DOLocationID ORDER BY n DESC LIMIT 500',
  },
  {
    id: 'orderby',
    label: 'order by fare (full sort)',
    sql: 'SELECT hvfhs_license_num, base_passenger_fare FROM taxi ORDER BY base_passenger_fare DESC LIMIT 1000',
  },
  {
    id: 'window',
    label: 'window: rank per license',
    sql: 'SELECT hvfhs_license_num, base_passenger_fare, row_number() OVER (PARTITION BY hvfhs_license_num ORDER BY base_passenger_fare DESC) AS rnk FROM taxi QUALIFY rnk <= 100',
  },
];
