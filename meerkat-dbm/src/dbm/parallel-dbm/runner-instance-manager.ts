import { InstanceManagerType } from '../instance-manager';
import { IFrameManager } from './iframe-manager';

class Connection {
  query(query: string) {
    return query;
  }
}

class RunnerInstanceManager implements InstanceManagerType {
  iFrameManagers: IFrameManager[];
  connection: Connection;

  constructor() {
    this.iFrameManagers = [];
    this.iFrameManagers.push(new IFrameManager('1'));
    this.connection = new Connection();
  }

  async getDB() {
    //Wait for 1s
    await new Promise((resolve) => setTimeout(resolve, 1000));

    return {
      connect: async () => this.connection,
    };
  }
}
