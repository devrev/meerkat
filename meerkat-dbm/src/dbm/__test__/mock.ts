import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import { InstanceManagerType } from '../instance-manager';

export const mockDB = {
  registerFileBuffer: async (name: string, buffer: Uint8Array) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve(buffer);
      }, 200);
    });
  },
  connect: async () => {
    return {
      query: async (query: string) => {
        return new Promise((resolve) => {
          setTimeout(() => {
            resolve([query]);
          }, 200);
        });
      },
      cancelSent: async () => {
        return new Promise((resolve) => {
          setTimeout(() => {
            resolve(true);
          }, 100);
        });
      },
      close: async () => {
        // do nothing
      },
    };
  },
};

export class InstanceManager implements InstanceManagerType {
  async getDB() {
    return mockDB as AsyncDuckDB;
  }

  async terminateDB() {
    // do nothing
  }
}
