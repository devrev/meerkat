import {
  DBMParallel,
  IFrameRunnerManager,
  ParallelIndexedDBFileManager,
  Table,
} from '@devrev/meerkat-dbm';
import log from 'loglevel';
import { useState } from 'react';
import { DBMContext } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { generateViewQuery } from '../utils';
import { InstanceManager } from './instance-manager';
import { useAsyncDuckDB } from './use-async-duckdb';

export const ParallelIndexedDBMProvider = ({
  children,
}: {
  children: JSX.Element;
}) => {
  const [dbm, setdbm] = useState<DBMParallel | null>(null);
  const [instanceManager] = useState(() => new InstanceManager());
  const [fileManager] = useState(
    () =>
      new ParallelIndexedDBFileManager({
        instanceManager,
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
      runnerURL: 'http://localhost:4204/runner/indexeddb-runner.html',
      origin: 'http://localhost:4204',
      totalRunners: 4,
      fetchTableFileBuffers: async (table) => {
        return [];
      },
      fetchPreQuery: (runnerId: string, tables: Table[]) => {
        const preQueries: string[] = [];

        for (const tableData of tables) {
          preQueries.push(
            generateViewQuery(
              tableData.tableName,
              tableData.files.map((file) => file.fileName)
            )
          );
        }
        return preQueries;
      },
      onEvent: (event) => {
        console.info(event);
      },
      logger: log,
    });

    const dbm = new DBMParallel({
      instanceManager,
      fileManager,
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

  if (!dbm) {
    return <div>Loading...</div>;
  }

  return (
    <DBMContext.Provider
      value={{
        dbm,
        fileManager: fileManager as any,
        fileManagerType: 'parallel-indexdb',
      }}
    >
      {children}
    </DBMContext.Provider>
  );
};
