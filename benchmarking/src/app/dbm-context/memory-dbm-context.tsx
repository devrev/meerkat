import { DBM, FileManagerType, MemoryDBFileManager } from '@devrev/meerkat-dbm';
import log from 'loglevel';
import React, { useState } from 'react';
import { DBMContext } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { InstanceManager } from './instance-manager';

export const MemoryDBMProvider = ({ children }: { children: JSX.Element }) => {
  const fileManagerRef = React.useRef<FileManagerType | null>(null);
  const [dbm, setdbm] = useState<DBM | null>(null);
  const instanceManagerRef = React.useRef<InstanceManager>(
    new InstanceManager()
  );

  useClassicEffect(() => {
    fileManagerRef.current = new MemoryDBFileManager({
      instanceManager: instanceManagerRef.current,
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
      instanceManager: instanceManagerRef.current,
      fileManager: fileManagerRef.current,
      logger: log,
      onEvent: (event) => {
        console.info(event);
      },
    });
    setdbm(dbm);
  }, []);

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
