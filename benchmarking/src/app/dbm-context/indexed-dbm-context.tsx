import { DBM, IndexedDBFileManager } from '@devrev/meerkat-dbm';
import log from 'loglevel';
import React, { useState } from 'react';
import { DBMContext } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { InstanceManager } from './instance-manager';
import { useAsyncDuckDB } from './use-async-duckdb';

export const IndexedDBMProvider = ({ children }: { children: JSX.Element }) => {
  const fileManagerRef = React.useRef<IndexedDBFileManager | null>(null);
  const [dbm, setdbm] = useState<DBM | null>(null);
  const instanceManagerRef = React.useRef(new InstanceManager());

  const dbState = useAsyncDuckDB();

  useClassicEffect(() => {
    if (!dbState) {
      return;
    }
    fileManagerRef.current = new IndexedDBFileManager({
      instanceManager: instanceManagerRef.current,
      fetchTableFileBuffers: async (table) => {
        return [];
      },
    });

    fileManagerRef.current.initializeDB();

    const dbm = new DBM({
      instanceManager: instanceManagerRef.current,
      fileManager: fileManagerRef.current,
      onEvent: (event) => {
        console.info(event);
      },
      logger: log,
      options: {
        shutdownInactiveTime: 1000,
      },
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
        fileManagerType: 'indexdb',
      }}
    >
      {children}
    </DBMContext.Provider>
  );
};
