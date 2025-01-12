import {
  DBMParallel,
  IFrameRunnerManager,
  ParallelMemoryFileManager,
} from '@devrev/meerkat-dbm';
import log from 'loglevel';
import { useRef, useState } from 'react';
import { DBMContext } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { InstanceManager } from './instance-manager';
import { useAsyncDuckDB } from './use-async-duckdb';

export const ParallelMemoryDBMProvider = ({
  children,
}: {
  children: JSX.Element;
}) => {
  const [dbm, setdbm] = useState<DBMParallel | null>(null);
  const instanceManagerRef = useRef<InstanceManager>(new InstanceManager());
  const fileManagerRef = useRef<ParallelMemoryFileManager>(
    new ParallelMemoryFileManager({
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
      runnerURL: 'http://localhost:4204/runner/memory-runner.html',
      origin: 'http://localhost:4204',
      totalRunners: 4,
      fetchTableFileBuffers: async (table) => {
        return fileManagerRef.current.getTableBufferData(table);
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
