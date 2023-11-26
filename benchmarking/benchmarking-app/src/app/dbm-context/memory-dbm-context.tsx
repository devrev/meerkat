import { DBM, FileManagerType, MemoryDBFileManager } from '@devrev/meerkat-dbm';
import log from 'loglevel';
import React, { useState } from 'react';
import { DBMContext } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { useAsyncDuckDB } from './use-async-duckdb';

export const MemoryDBMProvider = ({ children }: { children: JSX.Element }) => {
  const fileManagerRef = React.useRef<FileManagerType | null>(null);
  const [dbm, setdbm] = useState<DBM | null>(null);

  const dbState = useAsyncDuckDB();

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
    log.setLevel('DEBUG');
    const dbm = new DBM({
      db: dbState,
      fileManager: fileManagerRef.current,
      logger: log,
      onEvent: (event) => {
        console.info(event);
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
      }}
    >
      {children}
    </DBMContext.Provider>
  );
};
