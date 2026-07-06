import { cubeQueryToSQL } from '@devrev/meerkat-browser';
import {
  astDeserializerQuery,
  cubeToDuckdbAST,
  deserializeQuery,
  getCombinedTableSchema,
} from '@devrev/meerkat-core';
import * as duckdb from '@duckdb/duckdb-wasm';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { useState } from 'react';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { buildDimIssueSchemaAndQuery } from './dim-issue-fixture';

const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

/**
 * Browser generation benchmark.
 *
 * Runs the REAL `cubeQueryToSQL` (browser) against a real duckdb-wasm Web
 * Worker connection. Each internal `connection.query(...)` is a postMessage hop
 * to the worker, so this measures the true cost of the AST deserialize
 * round-trip vs the AST-free fast path — and worker contention when many
 * generations fire in parallel.
 *
 * Emits results into `#gen_results` (JSON) for puppeteer to read.
 */
const ITER = 30;
const PARALLEL = 25;

const avg = async (fn: () => Promise<void>, iterations: number) => {
  await fn(); // warmup
  const start = performance.now();
  for (let i = 0; i < iterations; i += 1) await fn();
  return (performance.now() - start) / iterations;
};

const connectWasm = async (): Promise<AsyncDuckDBConnection> => {
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker!}");`], {
      type: 'text/javascript',
    })
  );
  const worker = new Worker(worker_url);
  const logger = new duckdb.VoidLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(worker_url);
  return db.connect();
};

export const GenBenchmarking = () => {
  const [results, setResults] = useState<any>(null);

  useClassicEffect(() => {
    (async () => {
      const connection = await connectWasm();
      // Base table so generated SQL is valid if executed; generation itself
      // does not need rows.
      await connection.query(
        `CREATE TABLE dim_issue (custom_fields VARCHAR, links_json VARCHAR, priority_uenum_json VARCHAR, stage_json VARCHAR, owned_by_ids VARCHAR[], custom_schema_fragment_ids VARCHAR[], created_date TIMESTAMP, id VARCHAR, space_id VARCHAR, subtype VARCHAR, title VARCHAR, display_id VARCHAR, target_close_date TIMESTAMP, sla_summary VARCHAR, actual_close_date TIMESTAMP, __fdl_row_order__ INTEGER);`
      );

      const { schema, query } = buildDimIssueSchemaAndQuery();
      const contextParams = { current_dev_user_id: 'devu/1' };

      const combined = getCombinedTableSchema([schema], query);

      // Count worker hits (postMessage round-trips) per full generation, both
      // paths, by wrapping connection.query.
      const countHits = async (fn: () => Promise<unknown>) => {
        const orig = connection.query.bind(connection);
        let hits = 0;
        (connection as any).query = (...a: any[]) => {
          hits += 1;
          return (orig as any)(...a);
        };
        await fn();
        (connection as any).query = orig;
        return hits;
      };

      // FAST PATH full generation (empty filters → AST-free preBaseQuery).
      const fastGen = () =>
        cubeQueryToSQL({ connection, query, tableSchemas: [schema], contextParams });

      // BASELINE full generation: same pipeline but forced through the AST
      // deserialize round-trip for preBaseQuery (what shipped before this PR).
      const baselineGen = async () => {
        const ast = cubeToDuckdbAST(query, combined, { filterType: 'PROJECTION_FILTER' });
        const arrow = await connection.query(astDeserializerQuery(ast as any));
        // deserialize + the same downstream string work the real pipeline does
        deserializeQuery(arrow.toArray().map((r) => r.toJSON()));
      };

      const fastHits = await countHits(fastGen);
      const baselineHits = await countHits(baselineGen);

      const fastSeqMs = await avg(fastGen, ITER);
      const baselineSeqMs = await avg(baselineGen, ITER);

      const fireBatch = async (fn: () => Promise<unknown>) => {
        const start = performance.now();
        await Promise.all(Array.from({ length: PARALLEL }, fn));
        return performance.now() - start;
      };
      await fireBatch(fastGen);
      const batchFastMs = await fireBatch(fastGen);
      await fireBatch(baselineGen);
      const batchBaselineMs = await fireBatch(baselineGen);

      // Head-of-line blocking: the wasm worker is single-threaded. Kick off a
      // heavy data query (occupies the worker), then immediately time a
      // generation. Baseline generation must queue behind the heavy query on
      // the worker; the fast path runs on the main thread, unaffected.
      await connection.query(
        `INSERT INTO dim_issue (id, __fdl_row_order__) SELECT CAST(i AS VARCHAR), i FROM range(200000) t(i);`
      );
      const heavyQuery = `SELECT COUNT(*) FROM (SELECT id FROM dim_issue ORDER BY __fdl_row_order__ DESC LIMIT 100000) a JOIN dim_issue b ON a.id = b.id;`;

      const measureUnderLoad = async (gen: () => Promise<unknown>) => {
        const heavy = connection.query(heavyQuery); // occupy the worker
        const s = performance.now();
        await gen(); // how long until generation completes?
        const genLatency = performance.now() - s;
        await heavy;
        return genLatency;
      };
      const fastUnderLoadMs = await measureUnderLoad(fastGen);
      const baselineUnderLoadMs = await measureUnderLoad(baselineGen);

      setResults({
        fastPathWorkerHits: fastHits,
        baselineWorkerHits: baselineHits,
        fastSeqMs: Number(fastSeqMs.toFixed(4)),
        baselineSeqMs: Number(baselineSeqMs.toFixed(4)),
        seqSpeedup: Number((baselineSeqMs / fastSeqMs).toFixed(2)),
        parallel: PARALLEL,
        batchFastMs: Number(batchFastMs.toFixed(4)),
        batchBaselineMs: Number(batchBaselineMs.toFixed(4)),
        batchSpeedup: Number((batchBaselineMs / batchFastMs).toFixed(2)),
        genLatencyUnderWorkerLoad: {
          fastMs: Number(fastUnderLoadMs.toFixed(4)),
          baselineMs: Number(baselineUnderLoadMs.toFixed(4)),
        },
      });
    })();
  }, []);

  return (
    <div>
      <h1>Generation Benchmark</h1>
      {results ? (
        <pre id="gen_results">{JSON.stringify(results)}</pre>
      ) : (
        <div>running…</div>
      )}
    </div>
  );
};
