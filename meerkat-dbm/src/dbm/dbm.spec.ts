import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import log from 'loglevel';
import {
  FileBufferStore,
  FileJsonStore,
  FileManagerType,
} from '../file-manager/file-manager-type';
import { FileData, Table, TableWiseFiles } from '../types';
import { DBM } from './dbm';
import { InstanceManagerType } from './instance-manager';
import { DBMConstructorOptions, TableConfig } from './types';

export class MockFileManager implements FileManagerType {
  private fileBufferStore: Record<string, FileBufferStore> = {};
  private tables: Record<string, Table> = {};

  async bulkRegisterFileBuffer(props: FileBufferStore[]): Promise<void> {
    for (const prop of props) {
      this.fileBufferStore[prop.fileName] = prop;
      this.tables[prop.tableName] = this.tables[prop.tableName] || {
        files: [],
      };
      this.tables[prop.tableName].files.push(...props);
    }
  }

  async registerFileBuffer(prop: FileBufferStore): Promise<void> {
    this.fileBufferStore[prop.fileName] = prop;
    this.tables[prop.tableName] = this.tables[prop.tableName] || { files: [] };
    this.tables[prop.tableName].files.push(prop);
  }

  async bulkRegisterJSON(props: FileJsonStore[]): Promise<void> {
    for (const prop of props) {
      await this.registerJSON(prop);
    }
  }

  async registerJSON(prop: FileJsonStore): Promise<void> {
    const { json, ...fileData } = prop;

    this.registerFileBuffer({
      ...fileData,
      buffer: new Uint8Array(),
    });
  }

  async getFileBuffer(name: string): Promise<Uint8Array> {
    const fileBuffer = this.fileBufferStore[name];
    if (!fileBuffer) {
      throw new Error(`File buffer for ${name} not found`);
    }
    return fileBuffer.buffer;
  }

  async mountFileBufferByTables(tables: TableConfig[]): Promise<void> {
    const tableNames = tables.map((table) => table.name);
    for (const tableName of tableNames) {
      for (const key in this.fileBufferStore) {
        if (this.fileBufferStore[key].tableName === tableName) {
          // mount operation here
          console.log(`Mounted file buffer for ${key}`);
        }
      }
    }
  }

  async getFilesByTableName(tableName: string): Promise<FileData[]> {
    const files: FileData[] = [];

    for (const key in this.fileBufferStore) {
      if (this.fileBufferStore[key].tableName === tableName) {
        files.push({ fileName: key });
      }
    }

    return files;
  }

  async dropFilesByTableName(
    tableName: string,
    fileNames: string[]
  ): Promise<void> {
    for (const fileName of fileNames) {
      delete this.fileBufferStore[fileName];
    }
  }

  async getFilesNameForTables(
    tableNames: TableConfig[]
  ): Promise<TableWiseFiles[]> {
    const data: TableWiseFiles[] = [];

    for (const { name: tableName } of tableNames) {
      const files: FileData[] = [];

      for (const key in this.fileBufferStore) {
        if (this.fileBufferStore[key].tableName === tableName) {
          files.push({ fileName: key });
        }
      }

      data.push({
        tableName,
        files,
      });
    }

    return data;
  }

  async getTableData(table: TableConfig): Promise<Table | undefined> {
    return this.tables[table.name];
  }

  async setTableMetadata(table: string, metadata: object): Promise<void> {
    this.tables[table].metadata = metadata;
  }

  onDBShutdownHandler = jest.fn(async () => {
    // do nothing
  });
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

describe('DBM', () => {
  let db: AsyncDuckDB;
  let fileManager: FileManagerType;
  let dbm: DBM;
  let instanceManager: InstanceManager;

  const tables = [{ name: 'table1' }];

  beforeAll(async () => {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    db = mockDB;
    fileManager = new MockFileManager();
    instanceManager = new InstanceManager();
  });

  beforeEach(() => {
    const instanceManager = new InstanceManager();
    const options: DBMConstructorOptions = {
      instanceManager: instanceManager,
      fileManager,
      logger: log,
      onEvent: (event) => {
        console.log(event);
      },
      options: {
        shutdownInactiveTime: 100,
      },
    };
    dbm = new DBM(options);
  });

  describe('query', () => {
    it('should execute a query', async () => {
      const result = await dbm.query('SELECT 1');
      expect(result).toEqual(['SELECT 1']);
    });
  });

  describe('queryWithTables', () => {
    it('should call the preQuery hook', async () => {
      const preQuery = jest.fn();

      await fileManager.registerFileBuffer({
        fileName: 'file1',
        tableName: 'table1',
        buffer: new Uint8Array(),
      });

      const result = await dbm.queryWithTables({
        query: 'SELECT * FROM table1',
        tables: tables,
        options: {
          preQuery,
        },
      });

      expect(preQuery).toBeCalledTimes(1);

      expect(preQuery).toBeCalledWith([
        {
          tableName: 'table1',
          files: [{ fileName: 'file1' }],
        },
      ]);
    });

    it('should execute a query with table names', async () => {
      const result = await dbm.queryWithTables({
        query: 'SELECT * FROM table1',
        tables: tables,
      });
      expect(result).toEqual(['SELECT * FROM table1']);
    });

    it('should execute multiple queries with table names', async () => {
      const promise1 = dbm.queryWithTables({
        query: 'SELECT * FROM table1',
        tables: tables,
      });
      const promise2 = dbm.queryWithTables({
        query: 'SELECT * FROM table2',
        tables: tables,
      });
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
      const promise3 = dbm.queryWithTables({
        query: 'SELECT * FROM table3',
        tables: tables,
      });

      /**
       * Now the queue should be running
       */
      expect(dbm.isQueryRunning()).toBe(true);

      const result3 = await promise3;

      expect(result3).toEqual(['SELECT * FROM table3']);
    });
  });

  describe('shutdown the db test', () => {
    it('should shutdown the db if there are no queries in the queue', async () => {
      const instanceManager = new InstanceManager();
      // If instanceManager.terminateDB is a method
      jest.spyOn(instanceManager, 'terminateDB');

      const onDuckDBShutdown = jest.fn();

      // If instanceManager.terminateDB is a function
      instanceManager.terminateDB = jest.fn();
      const options: DBMConstructorOptions = {
        instanceManager: instanceManager,
        fileManager,
        logger: log,
        onEvent: (event) => {
          console.log(event);
        },
        onDuckDBShutdown: onDuckDBShutdown,
        options: {
          shutdownInactiveTime: 100,
        },
      };
      const dbm = new DBM(options);

      /**
       * Execute a query
       */
      const promise1 = dbm.queryWithTables({
        query: 'SELECT * FROM table1',
        tables: tables,
      });

      /**
       * Execute another query
       */
      const promise2 = dbm.queryWithTables({
        query: 'SELECT * FROM table2',
        tables: tables,
      });

      /**
       * Wait for the queries to complete
       */
      await Promise.all([promise1, promise2]);

      await new Promise((resolve) => setTimeout(resolve, 10));
      /**
       * Expect instanceManager.terminateDB to not be called
       */
      expect(instanceManager.terminateDB).not.toBeCalled();

      /**
       * wait for 200ms
       */
      await new Promise((resolve) => setTimeout(resolve, 200));

      /**
       * Expect onDuckDBShutdown to be called
       */
      expect(onDuckDBShutdown).toBeCalled();

      /**
       * Expect instanceManager.terminateDB to be called
       */
      expect(fileManager.onDBShutdownHandler).toBeCalled();
      expect(instanceManager.terminateDB).toBeCalled();
    });

    it('should not shutdown the db if option is not set', async () => {
      const instanceManager = new InstanceManager();
      // If instanceManager.terminateDB is a method
      jest.spyOn(instanceManager, 'terminateDB');

      // If instanceManager.terminateDB is a function
      instanceManager.terminateDB = jest.fn();
      const options: DBMConstructorOptions = {
        instanceManager: instanceManager,
        fileManager,
        logger: log,
        onEvent: (event) => {
          console.log(event);
        },
      };
      const dbm = new DBM(options);

      /**
       * Execute a query
       */
      const promise1 = dbm.queryWithTables({
        query: 'SELECT * FROM table1',
        tables: tables,
      });

      /**
       * Execute another query
       */
      const promise2 = dbm.queryWithTables({
        query: 'SELECT * FROM table2',
        tables: tables,
      });

      /**
       * Wait for the queries to complete
       */
      await Promise.all([promise1, promise2]);

      await new Promise((resolve) => setTimeout(resolve, 10));
      /**
       * Expect instanceManager.terminateDB to not be called
       */
      expect(instanceManager.terminateDB).not.toBeCalled();

      /**
       * wait for 200ms
       */
      await new Promise((resolve) => setTimeout(resolve, 200));
      /**
       * Expect instanceManager.terminateDB to be called
       */
      expect(instanceManager.terminateDB).not.toBeCalled();
    });

    it('should not shutdown the db if the shutdown lock is true', async () => {
      jest.spyOn(instanceManager, 'terminateDB');

      /**
       * Set the shutdown lock to true
       */
      dbm.setShutdownLock(true);

      /**
       * Execute a query
       */
      await dbm.queryWithTables({
        query: 'SELECT * FROM table1',
        tables: tables,
      });

      /**
       * wait for 200ms
       */
      await new Promise((resolve) => setTimeout(resolve, 200));

      /**
       * Expect instanceManager.terminateDB to not be called
       */
      expect(instanceManager.terminateDB).not.toBeCalled();
    });
  });

  describe('cancel query execution', () => {
    it('should cancel the current executing query when abort is emitted', async () => {
      const abortController1 = new AbortController();

      // check the current query throws error abort is emitted
      try {
        const promise = dbm.queryWithTables({
          query: 'SELECT * FROM table1',
          tables: tables,
          options: {
            signal: abortController1.signal,
          },
        });

        abortController1.abort();

        await promise;

        expect(promise).not.toBeDefined();
      } catch (e) {
        expect(e).toBeDefined();
      }
    });

    it('should cancel the query in the queue when abort is emitted', async () => {
      const abortController1 = new AbortController();
      const abortController2 = new AbortController();

      const mockDBMQuery = jest.spyOn(dbm, 'query');

      const promise1 = dbm.queryWithTables({
        query: 'SELECT * FROM table1',
        tables: tables,
        options: {
          signal: abortController1.signal,
        },
      });

      const promise2 = dbm.queryWithTables({
        query: 'SELECT * FROM table2',
        tables: [{ name: 'table2' }],
        options: {
          signal: abortController2.signal,
        },
      });

      abortController2.abort();

      const promises = await Promise.allSettled([promise1, promise2]);

      // the first query should be fulfilled
      expect(mockDBMQuery).toBeCalledWith('SELECT * FROM table1');
      expect(promises[0].status).toBe('fulfilled');

      // the second query should be rejected as it was aborted
      expect(mockDBMQuery).not.toBeCalledWith('SELECT * FROM table2');
      expect(promises[1].status).toBe('rejected');
    });
  });
});
