import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import {
  FileBufferStore,
  FileManagerType,
} from '../file-manager/file-manager-type';
import { DBM, DBMConstructorOptions } from './dbm';

export class MockFileManager implements FileManagerType {
  private fileBufferStore: Record<string, FileBufferStore> = {};

  async bulkRegisterFileBuffer(props: FileBufferStore[]): Promise<void> {
    for (const prop of props) {
      this.fileBufferStore[prop.fileName] = prop;
    }
  }

  async registerFileBuffer(prop: FileBufferStore): Promise<void> {
    this.fileBufferStore[prop.fileName] = prop;
  }

  async getFileBuffer(name: string): Promise<Uint8Array> {
    const fileBuffer = this.fileBufferStore[name];
    if (!fileBuffer) {
      throw new Error(`File buffer for ${name} not found`);
    }
    return fileBuffer.buffer;
  }

  async mountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    for (const tableName of tableNames) {
      for (const key in this.fileBufferStore) {
        if (this.fileBufferStore[key].tableName === tableName) {
          // mount operation here
          console.log(`Mounted file buffer for ${key}`);
        }
      }
    }
  }

  async unmountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    for (const tableName of tableNames) {
      for (const key in this.fileBufferStore) {
        if (this.fileBufferStore[key].tableName === tableName) {
          // unmount operation here
          console.log(`Unmounted file buffer for ${key}`);
        }
      }
    }
  }
}

const mockDB = {
  connect: async () => {
    return {
      query: async (query: string) => {
        return new Promise((resolve) => {
          setTimeout(() => {
            resolve([query]);
          }, 200);
        });
      },
    };
  },
};

describe('DBM', () => {
  let db: AsyncDuckDB;
  let fileManager: FileManagerType;
  let dbm: DBM;

  beforeAll(async () => {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    db = mockDB;
    fileManager = new MockFileManager();
  });

  beforeEach(() => {
    const options: DBMConstructorOptions = {
      db,
      fileManager,
    };
    dbm = new DBM(options);
  });

  describe('query', () => {
    it('should execute a query', async () => {
      const result = await dbm.query('SELECT 1');
      expect(result).toEqual(['SELECT 1']);
    });
  });

  describe('queryWithTableNames', () => {
    it('should execute a query with table names', async () => {
      const result = await dbm.queryWithTableNames('SELECT * FROM table1', [
        'table1',
      ]);
      expect(result).toEqual(['SELECT * FROM table1']);
    });

    it('should execute multiple queries with table names', async () => {
      const promise1 = dbm.queryWithTableNames('SELECT * FROM table1', [
        'table1',
      ]);
      const promise2 = dbm.queryWithTableNames('SELECT * FROM table2', [
        'table1',
      ]);
      /**
       * Number of queries in the queue should be 1 as the first query is running
       */
      expect(dbm.getQueueLength()).toBe(1);

      /**
       * The queue should be running
       */
      expect(dbm.isQueryRunning()).toBe(true);
      const [result, result1] = await Promise.all([promise1, promise2]);
      expect(result).toEqual(['SELECT * FROM table1']);
      expect(result1).toEqual(['SELECT * FROM table2']);

      /**
       * Number of queries in the queue should be 0 as all the queries are executed
       */
      expect(dbm.getQueueLength()).toBe(0);
      /**
       * The queue should not be running
       */
      expect(dbm.isQueryRunning()).toBe(false);

      /**
       * Execute another query
       */
      const promise3 = dbm.queryWithTableNames('SELECT * FROM table3', [
        'table1',
      ]);

      /**
       * Now the queue should be running
       */
      expect(dbm.isQueryRunning()).toBe(true);

      const result3 = await promise3;

      expect(result3).toEqual(['SELECT * FROM table3']);
    });
  });
});
