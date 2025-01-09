import {
  DBMNative,
  FileManagerType,
  NativeFileManager,
} from '@devrev/meerkat-dbm';
import log from 'loglevel';
import { NativeBridge } from 'meerkat-dbm/src/dbm/dbm-native/native-bridge';
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
        console.log('registerFiles in native manager', files);
        const filePaths = await window.electron?.registerFiles({ files });
        console.log('nativeManager filePaths', filePaths);
        return filePaths;
      },

      query: async (query) => {
        console.log('query in native manager', query, window.electron);

        try {
          await window.electron?.query(query);
        } catch (error) {
          console.log('Query error:', error);
        }
      },
      dropFilesByTableName: async ({ tableName, fileNames }) => {
        window.electron?.dropFilesByTableName({
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
      onEvent: (event) => {},
      instanceManager: instanceManagerRef.current,
    });

    const dbm = new DBMNative({
      instanceManager: instanceManagerRef.current,
      fileManager: fileManagerRef.current,
      logger: log,
      onEvent: (event) => {},
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
