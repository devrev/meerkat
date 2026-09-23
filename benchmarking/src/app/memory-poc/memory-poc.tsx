import { DBM, FileManagerType, MemoryDBFileManager } from '@devrev/meerkat-dbm';
import axios from 'axios';
import log from 'loglevel';
import React, { useCallback, useRef, useState } from 'react';
import { TAXI_FILE_URL } from '../file-loader/constants';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { InstanceManager } from '../dbm-context/instance-manager';
import { useAsyncDuckDB } from '../dbm-context/use-async-duckdb';
import { generateViewQuery } from '../utils';
import {
  dropAllBuffers,
  formatBytes,
  MemorySnapshot,
  POC_QUERIES,
  resetEngine,
  takeMemorySnapshot,
  terminateEngine,
} from './memory-probe';

const TABLE_NAME = 'taxi';
const FILE_NAME = 'taxi.parquet';

export const MemoryPoc = () => {
  const instanceManagerRef = useRef<InstanceManager>(new InstanceManager());
  const fileManagerRef = useRef<FileManagerType | null>(null);
  const dbmRef = useRef<DBM | null>(null);

  const [status, setStatus] = useState('booting…');
  const [loaded, setLoaded] = useState(false);
  const [busy, setBusy] = useState(false);
  const [snapshots, setSnapshots] = useState<MemorySnapshot[]>([]);
  const [queryMs, setQueryMs] = useState<Record<string, number>>({});
  const iterationRef = useRef(0);
  const startMsRef = useRef<number>(Date.now());
  const loadStartedRef = useRef(false);

  const dbState = useAsyncDuckDB();

  const snap = useCallback(async (label: string) => {
    if (!dbmRef.current) return;
    iterationRef.current += 1;
    const s = await takeMemorySnapshot(iterationRef.current, label, dbmRef.current, instanceManagerRef.current);
    s.atMs = s.atMs - startMsRef.current;
    setSnapshots((prev) => [...prev, s]);
  }, []);

  // Build the DBM once DuckDB is ready, then auto-load the parquet + view.
  useClassicEffect(() => {
    if (!dbState || loadStartedRef.current) {
      return;
    }
    loadStartedRef.current = true;
    fileManagerRef.current = new MemoryDBFileManager({
      instanceManager: instanceManagerRef.current,
      fetchTableFileBuffers: async () => [],
    });
    const dbm = new DBM({
      instanceManager: instanceManagerRef.current,
      fileManager: fileManagerRef.current,
      logger: log,
      onEvent: (event) => log.info(event),
    });
    dbmRef.current = dbm;

    void (async () => {
      try {
        await snap('engine only (no data)');
        setStatus('downloading 452 MB parquet…');
        const res = await axios.get(TAXI_FILE_URL, { responseType: 'arraybuffer' });
        const buffer = new Uint8Array(res.data);
        setStatus(`registering ${formatBytes(buffer.byteLength)}…`);
        await fileManagerRef.current!.registerFileBuffer({
          tableName: TABLE_NAME,
          fileName: FILE_NAME,
          buffer,
        });
        await dbm.query(generateViewQuery(TABLE_NAME, [FILE_NAME]));
        const countRes = await dbm.query(`SELECT count(*)::BIGINT AS n FROM ${TABLE_NAME}`);
        const n = countRes.toArray()[0]?.toJSON()?.['n'];
        setStatus(`loaded — ${formatBytes(buffer.byteLength)}, ${n ?? '?'} rows`);
        setLoaded(true);
        await snap('after load');
      } catch (e) {
        setStatus(`load failed: ${e instanceof Error ? e.message : String(e)}`);
      }
    })();
  }, [dbState]);

  const runQuery = useCallback(
    async (q: { id: string; label: string; sql: string }) => {
      if (!dbmRef.current) return;
      setBusy(true);
      try {
        const start = performance.now();
        await dbmRef.current.query(q.sql);
        setQueryMs((prev) => ({ ...prev, [q.id]: performance.now() - start }));
        await snap(q.label);
      } catch (e) {
        setStatus(`query "${q.label}" failed: ${e instanceof Error ? e.message : String(e)}`);
      } finally {
        setBusy(false);
      }
    },
    [snap],
  );

  const runAll = useCallback(async () => {
    setBusy(true);
    for (const q of POC_QUERIES) {
      if (!dbmRef.current) break;
      try {
        const start = performance.now();
        await dbmRef.current.query(q.sql);
        setQueryMs((prev) => ({ ...prev, [q.id]: performance.now() - start }));
        await snap(q.label);
      } catch {
        // keep going
      }
    }
    setBusy(false);
  }, [snap]);

  const reclaim = useCallback(
    async (mode: 'dropFiles' | 'reset' | 'terminate') => {
      setBusy(true);
      try {
        if (mode === 'dropFiles') await dropAllBuffers(instanceManagerRef.current);
        else if (mode === 'reset') await resetEngine(instanceManagerRef.current);
        else await terminateEngine(instanceManagerRef.current);
        setStatus(`${mode}() done`);
        await snap(`after ${mode}()`);
      } catch (e) {
        setStatus(`${mode}() failed: ${e instanceof Error ? e.message : String(e)}`);
      } finally {
        setBusy(false);
      }
    },
    [snap],
  );

  const last = snapshots[snapshots.length - 1];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, padding: 12 }}>
      <div>
        Loads the 452 MB NYC-taxi parquet via <code>registerFileBuffer</code>, creates a view, runs varied SQL, and
        measures DuckDB memory + <b>measureUserAgentSpecificMemory()</b> (true per-context incl. WASM worker).
      </div>
      <div>
        <b>status:</b> {status} {busy ? '(busy…)' : ''}
      </div>

      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
        <button disabled={!loaded || busy} onClick={() => void runAll()}>
          Run all queries
        </button>
        {POC_QUERIES.map((q) => (
          <button key={q.id} disabled={!loaded || busy} onClick={() => void runQuery(q)}>
            {q.label}
            {queryMs[q.id] != null ? ` — ${queryMs[q.id].toFixed(0)}ms` : ''}
          </button>
        ))}
      </div>

      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
        <button disabled={busy} onClick={() => void reclaim('dropFiles')}>
          dropFiles()
        </button>
        <button disabled={busy} onClick={() => void reclaim('reset')}>
          reset()
        </button>
        <button disabled={busy} onClick={() => void reclaim('terminate')}>
          terminate()
        </button>
        <button disabled={busy} onClick={() => void snap('manual')}>
          Snapshot now
        </button>
      </div>

      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 24 }}>
        <span>samples: {snapshots.length}</span>
        <span>DuckDB: {formatBytes(last?.duckDbUsedBytes ?? null)}</span>
        <span>
          globFiles: {formatBytes(last?.duckDbFileBytes ?? null)} ({last?.duckDbFileCount ?? 0})
        </span>
        <span>
          <b>UA total (incl. WASM): {formatBytes(last?.uaTotalBytes ?? null)}</b>
        </span>
        <span>JS heap: {formatBytes(last?.jsHeapUsedBytes ?? null)}</span>
      </div>

      {last && Object.keys(last.uaByType).length > 0 ? (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16, fontSize: 12 }}>
          <b>UA by type:</b>
          {Object.entries(last.uaByType)
            .sort((a, b) => b[1] - a[1])
            .map(([t, b]) => (
              <span key={t}>
                {t}: {formatBytes(b)}
              </span>
            ))}
        </div>
      ) : null}

      <table style={{ width: '100%', fontSize: 12, borderCollapse: 'collapse' }}>
        <thead>
          <tr>
            <th style={{ textAlign: 'left' }}>#</th>
            <th style={{ textAlign: 'left' }}>t(s)</th>
            <th style={{ textAlign: 'left' }}>event</th>
            <th style={{ textAlign: 'left' }}>DuckDB</th>
            <th style={{ textAlign: 'left' }}>globFiles</th>
            <th style={{ textAlign: 'left' }}>UA total</th>
            <th style={{ textAlign: 'left' }}>JS heap</th>
          </tr>
        </thead>
        <tbody>
          {snapshots.map((s) => (
            <tr key={s.iteration}>
              <td>{s.iteration}</td>
              <td>{(s.atMs / 1000).toFixed(1)}</td>
              <td>{s.label}</td>
              <td>{formatBytes(s.duckDbUsedBytes)}</td>
              <td>{formatBytes(s.duckDbFileBytes)}</td>
              <td>{formatBytes(s.uaTotalBytes)}</td>
              <td>{formatBytes(s.jsHeapUsedBytes)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};
