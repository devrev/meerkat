import {
  BROWSER_RUNNER_TYPE,
  BrowserRunnerMessage,
  convertArrowTableToJSON,
  DBM,
  FileManagerType,
  getMainAppName,
  getRunnerAppName,
  RunnerIndexedDBFileManager,
  WindowCommunication,
} from '@devrev/meerkat-dbm';

import log from 'loglevel';
import { Table } from 'meerkat-dbm/src/types';
import { useEffect, useRef, useState } from 'react';
import { InstanceManager } from './duck-db/instance-manager';

type EffectCallback = () => void | (() => void | undefined);

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
  const messageRefSet = useRef<boolean>(false);

  const communicationRef = useRef<WindowCommunication<BrowserRunnerMessage>>();
  const instanceManagerRef = useRef<InstanceManager>();
  const fileManagerRef = useRef<FileManagerType>();
  const dbmRef = useRef<DBM>();
  const activeQueriesRef = useRef<Map<string, AbortController>>(new Map());

  const urlParams = new URLSearchParams(window.location.search);
  const uuid = urlParams.get('uuid') ?? '';
  const origin = urlParams.get('origin');

  if (!communicationRef.current) {
    communicationRef.current = new WindowCommunication<BrowserRunnerMessage>({
      app_name: getRunnerAppName(uuid),
      origin: origin as string,
      targetApp: getMainAppName(uuid),
      targetWindow: window.parent,
    });
  }

  if (!instanceManagerRef.current) {
    instanceManagerRef.current = new InstanceManager();
  }

  if (!fileManagerRef.current) {
    fileManagerRef.current = new RunnerIndexedDBFileManager({
      instanceManager: instanceManagerRef.current,
      fetchTableFileBuffers: async () => [],
      logger: log,
      onEvent: (event) => {
        communicationRef.current?.sendRequestWithoutResponse({
          type: BROWSER_RUNNER_TYPE.RUNNER_ON_EVENT,
          payload: event,
        });
      },
    });
  }

  if (!dbmRef.current) {
    dbmRef.current = new DBM({
      instanceManager: instanceManagerRef.current,
      fileManager: fileManagerRef.current,
      logger: log,
      onEvent: (event) => {
        communicationRef.current?.sendRequestWithoutResponse({
          type: BROWSER_RUNNER_TYPE.RUNNER_ON_EVENT,
          payload: event,
        });
      },
    });
  }

  useEffectOnce(() => {
    if (!messageRefSet.current) {
      communicationRef.current?.onMessage((message) => {
        switch (message.message.type) {
          case BROWSER_RUNNER_TYPE.EXEC_QUERY: {
            // Create an AbortController for this query
            const abortController = new AbortController();
            const queryId = message.message.payload.queryId;
            activeQueriesRef.current?.set(queryId, abortController);

            dbmRef.current
              ?.queryWithTables({
                query: message.message.payload.query,
                tables: message.message.payload.tables,
                options: {
                  ...message.message.payload.options,
                  signal: abortController.signal,
                  preQuery: async (tables: Table[]) => {
                    const preQueryMessage =
                      await communicationRef.current?.sendRequest<string[]>({
                        type: BROWSER_RUNNER_TYPE.RUNNER_PRE_QUERY,
                        payload: {
                          runnerId: uuid,
                          tables: tables,
                        },
                      });

                    const preQueries: string[] = preQueryMessage?.message ?? [];

                    for (const preQuery of preQueries) {
                      await dbmRef.current?.query(preQuery);
                    }
                  },
                },
              })
              .then((result: any) => {
                communicationRef.current?.sendResponse(message.uuid, {
                  data: convertArrowTableToJSON(result),
                  isError: false,
                  error: null,
                });
              })
              .catch((error) => {
                communicationRef.current?.sendResponse(message.uuid, {
                  data: null,
                  isError: true,
                  error: error,
                });
              })
              .finally(() => {
                // Clean up the abort controller after query completes
                activeQueriesRef.current?.delete(queryId);
              });
            break;
          }
          case BROWSER_RUNNER_TYPE.CANCEL_QUERY: {
            const queryId = message.message.payload.queryId;
            const abortController = activeQueriesRef.current?.get(queryId);

            if (abortController) {
              // Abort the query
              abortController.abort();
              activeQueriesRef.current?.delete(queryId);
            }
            break;
          }
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
      await dbmRef.current?.query('SELECT 1');
      communicationRef.current?.sendRequestWithoutResponse({
        type: BROWSER_RUNNER_TYPE.RUNNER_ON_READY,
      });
    })();
  });

  return <div>Runners </div>;
}

export default App;
