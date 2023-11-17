import { DBM, FileManagerType, MemoryDBFileManager } from '@devrev/meerkat-dbm';
import * as duckdb from '@duckdb/duckdb-wasm';
import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import React, { useState } from 'react';
const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

export const DBMContext = React.createContext<{
  dbm: DBM;
  fileManager: FileManagerType;
}>(null as any);

export const useClassicEffect = createClassicEffectHook();

function createClassicEffectHook() {
  return (effect: React.EffectCallback, deps?: React.DependencyList) => {
    React.useEffect(() => {
      let isMounted = true;
      let unmount: void | (() => void);

      queueMicrotask(() => {
        if (isMounted) unmount = effect();
      });

      return () => {
        isMounted = false;
        unmount?.();
      };
    }, deps);
  };
}

export const DBMProvider = ({ children }: { children: JSX.Element }) => {
  const [dbState, setdbState] = useState<AsyncDuckDB | null>(null);
  const fileManagerRef = React.useRef<FileManagerType | null>(null);
  const [dbm, setdbm] = useState<DBM | null>(null);

  useClassicEffect(() => {
    (async () => {
      const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

      const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker!}");`], {
          type: 'text/javascript',
        })
      );

      // Instantiate the asynchronus version of DuckDB-wasm
      const worker = new Worker(worker_url);
      const logger = new duckdb.ConsoleLogger();
      const db = new duckdb.AsyncDuckDB(logger, worker);
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      URL.revokeObjectURL(worker_url);
      setdbState(db);
    })();
  }, []);

  useClassicEffect(() => {
    if (!dbState) {
      return;
    }
    fileManagerRef.current = new MemoryDBFileManager({
      db: dbState,
      fetchTableFileBuffers: async (table) => {
        return [];
      },
    });
    const dbm = new DBM({
      db: dbState,
      fileManager: fileManagerRef.current,
    });
    setdbm(dbm);
  }, [dbState]);

  if (!dbm || !fileManagerRef.current) {
    return <div>Loading...</div>;
  }

  return (
    <DBMContext.Provider
      value={{
        dbm,
        fileManager: fileManagerRef.current,
      }}
    >
      {children}
    </DBMContext.Provider>
  );
};

export const useDBM = () => {
  const context = React.useContext(DBMContext);
  if (context === undefined) {
    throw new Error('useDBM must be used within a DBMProvider');
  }
  return context;
};
