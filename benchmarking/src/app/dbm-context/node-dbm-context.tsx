import { DBMApp, FileManagerType, NodeFileManager } from '@devrev/meerkat-dbm';
import log from 'loglevel';
import { useRef, useState } from 'react';
import { DBMContext } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { InstanceManager } from './instance-manager';
import { useAsyncDuckDB } from './use-async-duckdb';

export enum DBMEvent {
  REGISTER_FILE_BUFFER = 'register-file-buffer',
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

export const NodeDBMProvider = ({ children }: { children: JSX.Element }) => {
  const fileManagerRef = useRef<FileManagerType | null>(null);
  const [dbm, setdbm] = useState<DBMApp | null>(null);
  const instanceManagerRef = useRef<InstanceManager>(new InstanceManager());

  const dbState = useAsyncDuckDB();

  useClassicEffect(() => {
    if (!dbState) {
      return;
    }
    fileManagerRef.current = new NodeFileManager({
      fetchTableFileBuffers: async (table) => {
        return [];
      },
      windowApi: window.electron,
      logger: log,
      onEvent: (event) => {
        console.info(event);
      },
      instanceManager: instanceManagerRef.current,
    });
    log.setLevel('DEBUG');
    const dbm = new DBMApp({
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
