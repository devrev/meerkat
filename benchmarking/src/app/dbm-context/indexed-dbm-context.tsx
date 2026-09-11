import { DBM, IndexedDBFileManager } from '@devrev/meerkat-dbm';
import log from 'loglevel';
import { useState } from 'react';
import { DBMContext } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { InstanceManager } from './instance-manager';
import { useAsyncDuckDB } from './use-async-duckdb';

export const IndexedDBMProvider = ({ children }: { children: JSX.Element }) => {
  const [instanceManager] = useState(() => new InstanceManager());
  const [runtime, setRuntime] = useState<{
    dbm: DBM;
    fileManager: IndexedDBFileManager;
  } | null>(null);

  const dbState = useAsyncDuckDB();

  useClassicEffect(() => {
    if (!dbState) {
      return;
    }
    const fileManager = new IndexedDBFileManager({
      instanceManager,
      fetchTableFileBuffers: async (table) => {
        return [];
      },
    });

    fileManager.initializeDB();

    const dbm = new DBM({
      instanceManager,
      fileManager,
      onEvent: (event) => {
        console.info(event);
      },
      logger: log,
      options: {
        shutdownInactiveTime: 1000,
      },
    });

    setRuntime({ dbm, fileManager });
  }, [dbState]);

  if (!runtime) {
    return <div>Loading...</div>;
  }

  return (
    <DBMContext.Provider
      value={{
        dbm: runtime.dbm,
        fileManager: runtime.fileManager,
        fileManagerType: 'indexdb',
      }}
    >
      {children}
    </DBMContext.Provider>
  );
};
