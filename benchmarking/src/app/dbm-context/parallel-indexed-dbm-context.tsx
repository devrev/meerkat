import {
  DBMParallel,
  IFrameRunnerManager,
  ParallelIndexedDBFileManager,
} from '@devrev/meerkat-dbm';
import log from 'loglevel';
import { useRef, useState } from 'react';
import { DBMContext } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { InstanceManager } from './instance-manager';
import { useAsyncDuckDB } from './use-async-duckdb';

export const ParallelIndexedDBMProvider = ({
  children,
}: {
  children: JSX.Element;
}) => {
  const [dbm, setdbm] = useState<DBMParallel | null>(null);
  const instanceManagerRef = useRef<InstanceManager>(new InstanceManager());
  const fileManagerRef = useRef<ParallelIndexedDBFileManager>(
    new ParallelIndexedDBFileManager({
      instanceManager: instanceManagerRef.current,
      fetchTableFileBuffers: async (table) => {
        return [];
      },
      logger: log,
      onEvent: (event) => {
        console.info(event);
      },
    })
  );

  const dbState = useAsyncDuckDB();

  useClassicEffect(() => {
    if (!dbState) {
      return;
    }
    const iframeManager = new IFrameRunnerManager({
      runnerURL: 'http://localhost:4200/runner/indexeddb-runner.html',
      origin: 'http://localhost:4200',
      totalRunners: 2,
      fetchTableFileBuffers: async (table) => {
        return [];
      },
      fetchPreQuery: () => {
        return [];
      },
      onEvent: (event) => {
        console.info(event);
      },
      logger: log,
    });

    const dbm = new DBMParallel({
      instanceManager: instanceManagerRef.current,
      fileManager: fileManagerRef.current,
      onEvent: (event) => {
        console.info(event);
      },
      logger: log,
      options: {
        shutdownInactiveTime: 1000,
      },
      iFrameRunnerManager: iframeManager,
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
        fileManager: fileManagerRef.current as any,
      }}
    >
      {children}
    </DBMContext.Provider>
  );
};
