import {
  DBMParallel,
  IFrameRunnerManager,
  ParallelIndexedDBFileManager,
} from '@devrev/meerkat-dbm';
import log from 'loglevel';
import { TableWiseFiles } from 'meerkat-dbm/src/types/common-types';
import { useRef, useState } from 'react';
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
      runnerURL: 'http://localhost:4204/runner/index.html',
      origin: 'http://localhost:4204',
      totalRunners: 4,
      fetchTableFileBuffers: async (table) => {
        return [];
      },
      fetchPreQuery: (runnerId: string, tableWiseFiles: TableWiseFiles[]) => {
        const preQueries: string[] = [];

        console.info('tableWiseFiles', tableWiseFiles);
        for (const tableWiseFile of tableWiseFiles) {
          preQueries.push(
            generateViewQuery(
              tableWiseFile.tableName,
              tableWiseFile.files.map((file) => file.fileName)
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
        fileManagerType: 'parallel-indexdb',
      }}
    >
      {children}
    </DBMContext.Provider>
  );
};
