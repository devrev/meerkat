import {
  DBM,
  FileManagerType,
  IndexedDBFileManager,
} from '@devrev/meerkat-dbm';
import React, { useState } from 'react';
import { DBMContext } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { useAsyncDuckDB } from './use-async-duckdb';

export const IndexedDBMProvider = ({ children }: { children: JSX.Element }) => {
  const fileManagerRef = React.useRef<FileManagerType | null>(null);
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
    const dbm = new DBM({
      db: dbState,
      fileManager: fileManagerRef.current,
    });
    /**
     * Making the queryWithTableNames simply run the queries without sequence which is the default behavior
     */

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
