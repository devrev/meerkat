import { DBMParallel, ParallelMemoryFileManager } from '@devrev/meerkat-dbm';
import log from 'loglevel';
import { useRef, useState } from 'react';
import { DBMContext } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';
import { InstanceManager } from './instance-manager';
import { useAsyncDuckDB } from './use-async-duckdb';

export const ParallelDBMProvider = ({
  children,
}: {
  children: JSX.Element;
}) => {
  // const communicationRef = useRef<CommunicationInterface<BrowserRunnerMessage>>(
  //   new WindowCommunication<BrowserRunnerMessage>({
  //     app_name: 'PARENT',
  //     origin: 'localhost:4205',
  //     targetApp: 'PARENT',
  //     targetWindow: ,
  //   })
  // );

  const fileManagerRef = useRef<ParallelMemoryFileManager | null>(null);
  const [dbm, setdbm] = useState<DBMParallel | null>(null);
  const instanceManagerRef = useRef<InstanceManager>(new InstanceManager());

  const dbState = useAsyncDuckDB();

  useClassicEffect(() => {
    if (!dbState) {
      return;
    }
    fileManagerRef.current = new ParallelMemoryFileManager({
      instanceManager: instanceManagerRef.current,
      fetchTableFileBuffers: async (table) => {
        return [];
      },
      logger: log,
      onEvent: (event) => {
        console.info(event);
      },
      communication: communicationRef.current,
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
