import { DBMNative, FileStore, NativeFileManager } from '@devrev/meerkat-dbm';
import log from 'loglevel';
import { NativeBridge } from 'meerkat-dbm/src/dbm/dbm-native/native-bridge';
import { useMemo, useState } from 'react';
import { DBMContext } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { InstanceManager } from './instance-manager';
import { useAsyncDuckDB } from './use-async-duckdb';

export const NativeDBMProvider = ({ children }: { children: JSX.Element }) => {
  const [instanceManager] = useState(() => new InstanceManager());
  const [runtime, setRuntime] = useState<{
    dbm: DBMNative;
    fileManager: NativeFileManager;
  } | null>(null);

  const dbState = useAsyncDuckDB();

  const nativeBridge: NativeBridge = useMemo(() => {
    return {
      registerFiles: async (files: FileStore[]): Promise<void> => {
        await window.api?.registerFiles(files);
      },

      query: async (query): Promise<Record<string, unknown>> => {
        const result = await window.api?.query(query);

        return result ?? {};
      },
      dropFilesByTableName: async ({ tableName, fileNames }): Promise<void> => {
        await window.api?.dropFilesByTableName({
          tableName,
          fileNames,
        });
      },
      getFilePathsForTable: async (tableName): Promise<string[]> => {
        return (await window.api?.getFilePathsForTable(tableName)) ?? [];
      },
    };
  }, []);

  useClassicEffect(() => {
    if (!dbState) {
      return;
    }

    const fileManager = new NativeFileManager({
      fetchTableFileBuffers: async (table) => {
        return [];
      },
      nativeBridge: nativeBridge,
      logger: log,
      onEvent: (event) => {
        console.log('event', event);
      },
      instanceManager,
    });

    const dbm = new DBMNative({
      instanceManager,
      fileManager,
      logger: log,
      onEvent: (event) => {
        console.log('event', event);
      },
      nativeBridge: nativeBridge,
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
        fileManagerType: 'native',
      }}
    >
      {children}
    </DBMContext.Provider>
  );
};
