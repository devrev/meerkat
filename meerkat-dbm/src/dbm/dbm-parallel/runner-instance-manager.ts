// import { InstanceManagerType } from '../instance-manager';
// import { IFrameManager } from './iframe-manager';
// import { BROWSER_RUNNER_TYPE, BrowserRunnerExecQueryMessage, BrowserRunnerMessage } from '../../window-communication/runner-types';

// class Connection {
//   query(query: string) {
//     return query;
//   }
// }

// class RunnerInstanceManager implements InstanceManagerType {
//   iFrameManagers: IFrameManager[];
//   connection: Connection;

//   constructor() {
//     this.iFrameManagers = [];
//     this.iFrameManagers.push(new IFrameManager('1'));
//     this.connection = new Connection();
//   }

//   private query (query: string) {
//     this.iFrameManagers[0].communication.sendRequest<BrowserRunnerExecQueryMessage>({
//         type: BROWSER_RUNNER_TYPE.EXEC_QUERY,
//         payload: {
//           query,
//         }
//     })
//   }

//   async getDB() {
//     //Wait for 1s
//     await new Promise((resolve) => setTimeout(resolve, 1000));

//     return {
//       connect: async () => this.connection,
//     };
//   }
// }
