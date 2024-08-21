import {
  BROWSER_RUNNER_TYPE,
  BrowserRunnerMessage,
  DBM,
  FileManagerType,
  RunnerMemoryDBFileManager,
  WindowCommunication,
} from '@devrev/meerkat-dbm';
import log from 'loglevel';
import React from 'react';
import { InstanceManager } from './instance-manager';

export function App() {
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

  communicationRef.current.onMessage((message) => {
    console.log(message, 'message');
    switch (message.message.type) {
      case BROWSER_RUNNER_TYPE.EXEC_QUERY:
        // eslint-disable-next-line no-lone-blocks
        {
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

  log.setLevel('DEBUG');

  return <div>FK </div>;
}

export default App;
