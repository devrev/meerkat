import {
  DBMNative,
  FileManagerType,
  NativeFileManager,
} from '@devrev/meerkat-dbm';
import log from 'loglevel';
import {
  NativeBridge,
  QueryResult,
} from 'meerkat-dbm/src/dbm/dbm-native/native-bridge';
import { useMemo, useRef, useState } from 'react';
import { DBMContext } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { InstanceManager } from './instance-manager';
import { useAsyncDuckDB } from './use-async-duckdb';

export const NativeDBMProvider = ({ children }: { children: JSX.Element }) => {
  const fileManagerRef = useRef<FileManagerType | null>(null);
  const [dbm, setdbm] = useState<DBMNative | null>(null);
  const instanceManagerRef = useRef<InstanceManager>(new InstanceManager());

  const dbState = useAsyncDuckDB();

  const nativeManager: NativeBridge = useMemo(() => {
    return {
      registerFiles: async ({ files }) => {
        window.electron?.registerFiles({ files });
      },
      downloadFiles: async ({ files }) => {
        window.electron?.downloadFiles({ files });
      },
      query: async (query) => {
        const result = await window.electron?.query(query);

        return result as QueryResult;
      },
      dropFilesByTable: async ({ tableName, fileNames }) => {
        window.electron?.dropFilesByTable({
          tableName,
          fileNames,
        });
      },
    };
  }, []);

  useClassicEffect(() => {
    if (!dbState) {
      return;
    }

    fileManagerRef.current = new NativeFileManager({
      fetchTableFileBuffers: async (table) => {
        return [];
      },
      nativeManager: nativeManager,
      logger: log,
      onEvent: (event) => {
        console.info(event);
      },
      instanceManager: instanceManagerRef.current,
    });

    const dbm = new DBMNative({
      instanceManager: instanceManagerRef.current,
      fileManager: fileManagerRef.current,
      logger: log,
      onEvent: (event) => {
        console.info(event);
      },
      nativeManager: nativeManager,
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
