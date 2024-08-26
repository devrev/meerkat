import {
  BROWSER_RUNNER_TYPE,
  BrowserRunnerMessage,
  convertArrowTableToJSON,
  DBM,
  FileManagerType,
  RunnerMemoryDBFileManager,
  WindowCommunication,
} from '@devrev/meerkat-dbm';

import log from 'loglevel';
import React, { useEffect, useRef, useState } from 'react';
import { InstanceManager } from './instance-manager';

type EffectCallback = () => void | (() => void | undefined);

function getAppName(appId: string, uuid: string) {
  return appId + '__' + uuid;
}

function useEffectOnce(effect: EffectCallback): void {
  const destroyFunc = useRef<void | (() => void | undefined)>();
  const effectCalled = useRef(false);
  const renderAfterCalled = useRef(false);
  const [, setVal] = useState<number>(0);

  if (effectCalled.current) {
    renderAfterCalled.current = true;
  }

  useEffect(() => {
    if (!effectCalled.current) {
      destroyFunc.current = effect();
      effectCalled.current = true;
    }

    setVal((val) => val + 1);

    return () => {
      if (!renderAfterCalled.current) {
        return;
      }
      if (destroyFunc.current) {
        destroyFunc.current();
      }
    };
  }, []);
}

export function App() {
  const messageRefSet = React.useRef<boolean>(false);
  const urlParams = new URLSearchParams(window.location.search);
  const uuid = urlParams.get('uuid') ?? '';
  const origin = urlParams.get('origin');

  const communicationRef = React.useRef<
    WindowCommunication<BrowserRunnerMessage>
  >(
    new WindowCommunication<BrowserRunnerMessage>({
      app_name: getAppName('RUNNER', uuid),
      origin: origin as string,
      targetApp: getAppName('DBM', uuid),
      targetWindow: window.parent,
    })
  );
  const instanceManagerRef = React.useRef<InstanceManager>(
    new InstanceManager()
  );
  const fileManagerRef = React.useRef<FileManagerType>(
    new RunnerMemoryDBFileManager({
      instanceManager: instanceManagerRef.current,
      fetchTableFileBuffers: async (table) => {
        return [];
      },
      logger: log,
      onEvent: (event) => {
        communicationRef.current.sendRequestWithoutResponse({
          type: BROWSER_RUNNER_TYPE.RUNNER_ON_EVENT,
          payload: event,
        });
      },
      communication: communicationRef.current,
    })
  );

  const dbmRef = React.useRef<DBM>(
    new DBM({
      instanceManager: instanceManagerRef.current,
      fileManager: fileManagerRef.current,
      logger: log,
      onEvent: (event) => {
        communicationRef.current.sendRequestWithoutResponse({
          type: BROWSER_RUNNER_TYPE.RUNNER_ON_EVENT,
          payload: event,
        });
      },
    })
  );

  useEffectOnce(() => {
    if (!messageRefSet.current) {
      communicationRef.current.onMessage((message) => {
        switch (message.message.type) {
          case BROWSER_RUNNER_TYPE.EXEC_QUERY:
            dbmRef.current
              .queryWithTables(message.message.payload)
              .then((result: any) => {
                communicationRef.current.sendResponse(message.uuid, {
                  data: convertArrowTableToJSON(result),
                  isError: false,
                  error: null,
                });
              })
              .catch((error) => {
                communicationRef.current.sendResponse(message.uuid, {
                  data: null,
                  isError: true,
                  error: error,
                });
              });

            break;
          default:
            break;
        }
      });
      messageRefSet.current = true;
    }
  });

  useEffectOnce(() => {
    (async () => {
      //Execute dummy query to check if the DB is ready
      await dbmRef.current.query('SELECT 1');
      communicationRef.current.sendRequestWithoutResponse({
        type: BROWSER_RUNNER_TYPE.RUNNER_ON_READY,
      });
    })();
  });

  return <div>Runners </div>;
}

export default App;
