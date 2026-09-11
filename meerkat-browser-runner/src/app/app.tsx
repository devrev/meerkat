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
import { useEffect } from 'react';
import { InstanceManager } from './duck-db/instance-manager';

export function App() {
  const urlParams = new URLSearchParams(window.location.search);
  const uuid = urlParams.get('uuid') ?? '';
  const origin = urlParams.get('origin');

  useEffect(() => {
    const communication = new WindowCommunication<BrowserRunnerMessage>({
      app_name: getRunnerAppName(uuid),
      origin: origin as string,
      targetApp: getMainAppName(uuid),
      targetWindow: window.parent,
    });
    const instanceManager = new InstanceManager();
    const activeQueries = new Map<string, AbortController>();

    const fileManager: FileManagerType = new RunnerIndexedDBFileManager({
      instanceManager,
      fetchTableFileBuffers: async () => [],
      logger: log,
      onEvent: (event) => {
        communication.sendRequestWithoutResponse({
          type: BROWSER_RUNNER_TYPE.RUNNER_ON_EVENT,
          payload: event,
        });
      },
    });

    const dbm = new DBM({
      instanceManager,
      fileManager,
      logger: log,
      onEvent: (event) => {
        communication.sendRequestWithoutResponse({
          type: BROWSER_RUNNER_TYPE.RUNNER_ON_EVENT,
          payload: event,
        });
      },
    });

    communication.onMessage((message) => {
      switch (message.message.type) {
        case BROWSER_RUNNER_TYPE.EXEC_QUERY: {
          const abortController = new AbortController();
          const queryId = message.message.payload.queryId;

          activeQueries.set(queryId, abortController);

          dbm
            .queryWithTables({
              query: message.message.payload.query,
              tables: message.message.payload.tables,
              options: {
                ...message.message.payload.options,
                signal: abortController.signal,
                preQuery: async (tables: Table[]) => {
                  const preQueryMessage = await communication.sendRequest<
                    string[]
                  >({
                    type: BROWSER_RUNNER_TYPE.RUNNER_PRE_QUERY,
                    payload: {
                      runnerId: uuid,
                      tables: tables,
                    },
                  });

                  const preQueries: string[] = preQueryMessage.message ?? [];

                  for (const preQuery of preQueries) {
                    await dbm.query(preQuery);
                  }
                },
              },
            })
            .then((result: any) => {
              communication.sendResponse(message.uuid, {
                data: convertArrowTableToJSON(result),
                isError: false,
                error: null,
              });
            })
            .catch((error) => {
              communication.sendResponse(message.uuid, {
                data: null,
                isError: true,
                error: error,
              });
            })
            .finally(() => {
              activeQueries.delete(queryId);
            });
          break;
        }
        case BROWSER_RUNNER_TYPE.CANCEL_QUERY: {
          const queryId = message.message.payload.queryId;
          const abortController = activeQueries.get(queryId);

          if (abortController) {
            abortController.abort();
            activeQueries.delete(queryId);
          }
          break;
        }
        default:
          break;
      }
    });

    void dbm.query('SELECT 1').then(() => {
      communication.sendRequestWithoutResponse({
        type: BROWSER_RUNNER_TYPE.RUNNER_ON_READY,
      });
    });

    return () => {
      activeQueries.forEach((controller) => controller.abort());
      activeQueries.clear();
      communication.destroy();
    };
  }, [origin, uuid]);

  return <div>Runners </div>;
}

export default App;
