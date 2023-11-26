import { DBM, IndexedDBFileManager } from '@devrev/meerkat-dbm';
import log from 'loglevel';
import React, { useState } from 'react';
import { DBMContext } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { useAsyncDuckDB } from './use-async-duckdb';

export const IndexedDBMProvider = ({ children }: { children: JSX.Element }) => {
  const fileManagerRef = React.useRef<IndexedDBFileManager | null>(null);
  const [dbm, setdbm] = useState<DBM | null>(null);

  const dbState = useAsyncDuckDB();

  useClassicEffect(() => {
    if (!dbState) {
      return;
    }
    fileManagerRef.current = new IndexedDBFileManager({
      db: dbState,
      fetchTableFileBuffers: async (table) => {
        return [];
      },
    });

    fileManagerRef.current.initializeDB();

    const dbm = new DBM({
      db: dbState,
      fileManager: fileManagerRef.current,
      onEvent: (event) => {
        console.info(event);
      },
      logger: log,
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
