import { DBM, MemoryDBFileManager } from '@devrev/meerkat-dbm';
import log from 'loglevel';
import { useState } from 'react';
import { DBMContext } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { InstanceManager } from './instance-manager';
import { useAsyncDuckDB } from './use-async-duckdb';

export const MemoryDBMProvider = ({ children }: { children: JSX.Element }) => {
  const [instanceManager] = useState(() => new InstanceManager());
  const [runtime, setRuntime] = useState<{
    dbm: DBM;
    fileManager: MemoryDBFileManager;
  } | null>(null);

  const dbState = useAsyncDuckDB();

  useClassicEffect(() => {
    if (!dbState) {
      return;
    }
    const fileManager = new MemoryDBFileManager({
      instanceManager,
      fetchTableFileBuffers: async (table) => {
        return [];
      },
      logger: log,
      onEvent: (event) => {
        console.info(event);
      },
    });
    log.setLevel('DEBUG');
    const dbm = new DBM({
      instanceManager,
      fileManager,
      logger: log,
      onEvent: (event) => {
        console.info(event);
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
        fileManagerType: 'memory',
      }}
    >
      {children}
    </DBMContext.Provider>
  );
};
