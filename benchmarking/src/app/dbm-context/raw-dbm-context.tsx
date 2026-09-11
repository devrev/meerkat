import { DBM, MemoryDBFileManager } from '@devrev/meerkat-dbm';
import log from 'loglevel';
import { useState } from 'react';
import { DBMContext } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { InstanceManager } from './instance-manager';
import { useAsyncDuckDB } from './use-async-duckdb';

export const RawDBMProvider = ({ children }: { children: JSX.Element }) => {
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
    });
    const dbm = new DBM({
      instanceManager,
      fileManager,
      logger: log,
      onEvent: (event) => {
        log.info(event);
      },
    });
    /**
     * Making the queryWithTables simply run the queries without sequence which is the default behavior
     */
    dbm.queryWithTables = async ({ query, tables }) => {
      return dbm.query(query);
    };
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
        fileManagerType: 'raw',
      }}
    >
      {children}
    </DBMContext.Provider>
  );
};
