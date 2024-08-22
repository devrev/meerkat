import {
  BROWSER_RUNNER_TYPE,
  BrowserRunnerMessage,
  DBM,
  FileManagerType,
  RunnerMemoryDBFileManager,
  WindowCommunication,
} from '@devrev/meerkat-dbm';
import log from 'loglevel';
import React, { useEffect } from 'react';
import { InstanceManager } from './instance-manager';

export function App() {
  const messageRefSet = React.useRef<boolean>(false);
  const communicationRef = React.useRef<
    WindowCommunication<BrowserRunnerMessage>
  >(
    new WindowCommunication<BrowserRunnerMessage>({
      app_name: 'RUNNER',
      origin: 'http://localhost:4200',
      targetApp: 'dbm',
      // eslint-disable-next-line no-restricted-globals
      targetWindow: parent,
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
        console.info(event);
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
        console.info(event);
      },
    })
  );

  useEffect(() => {
    if (!messageRefSet.current) {
      communicationRef.current.onMessage((message) => {
        switch (message.message.type) {
          case BROWSER_RUNNER_TYPE.EXEC_QUERY:
            {
              //Read ?uuid= from the URL
              const urlParams = new URLSearchParams(window.location.search);
              const uuid = urlParams.get('uuid');
              console.info('EXEC_QUERY', uuid, message.message.payload.query);
              dbmRef.current
                .queryWithTables(message.message.payload)
                .then((result) => {
                  console.log('result', result);
                  communicationRef.current.sendResponse(message.uuid, result);
                });
            }

            break;
          default:
            break;
        }
      });
      messageRefSet.current = true;
    }
  }, []);

  log.setLevel('DEBUG');

  useEffect(() => {
    communicationRef.current.sendRequestWithoutResponse({
      type: BROWSER_RUNNER_TYPE.RUNNER_ON_READY,
    });
  }, []);

  return <div>FK </div>;
}

export default App;
