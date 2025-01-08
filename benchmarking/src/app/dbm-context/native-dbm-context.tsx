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

export enum DBMEvent {
  REGISTER_FILE_BUFFER = 'register-file-buffer',
  QUERY_FILE_BUFFER = 'query-file-buffer',
  DROP_FILE_BUFFER = 'drop-file-buffer',
}

export type Channels = DBMEvent;

export type Electron = {
  ipcRenderer: {
    invoke: (channel: Channels, ...args: unknown[]) => void;
    send(channel: Channels, ...args: unknown[]): void;
    sendMessage(channel: Channels, ...args: unknown[]): void;
    on(channel: Channels, func: (...args: unknown[]) => void): () => void;
    once(channel: Channels, func: (...args: unknown[]) => void): void;
  };
};

declare global {
  interface Window {
    electron?: Electron;
  }
}

export const NativeDBMProvider = ({ children }: { children: JSX.Element }) => {
  const fileManagerRef = useRef<FileManagerType | null>(null);
  const [dbm, setdbm] = useState<DBMNative | null>(null);
  const instanceManagerRef = useRef<InstanceManager>(new InstanceManager());

  const dbState = useAsyncDuckDB();

  const nativeManager: NativeBridge = useMemo(() => {
    return {
      registerFiles: async ({ files }) => {
        window.electron?.ipcRenderer.invoke(DBMEvent.REGISTER_FILE_BUFFER, {
          files,
        });
      },
      query: async (query) => {
        console.log('Executing query:', query);
        const result = await window.electron?.ipcRenderer.invoke(
          DBMEvent.QUERY_FILE_BUFFER,
          {
            query,
          }
        );
        return result;
      },
      dropFilesByTableName: async ({ table, fileNames }) => {
        window.electron?.ipcRenderer.invoke(DBMEvent.DROP_FILE_BUFFER, {
          table,
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
      windowApi: window.electron,
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
