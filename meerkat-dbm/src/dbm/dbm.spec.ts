import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import log from 'loglevel';
import {
  FileBufferStore,
  FileJsonStore,
  FileManagerType
} from '../file-manager/file-manager-type';
import { FileData, Table, TableWiseFiles } from '../types';
import { DBM } from './dbm';
import { InstanceManagerType } from './instance-manager';
import { DBMConstructorOptions } from './types';

export class MockFileManager implements FileManagerType {
  private fileBufferStore: Record<string, FileBufferStore> = {};
  private tables: Record<string, Table> = {};

  async bulkRegisterFileBuffer(props: FileBufferStore[]): Promise<void> {
    for (const prop of props) {
      this.fileBufferStore[prop.fileName] = prop;
      this.tables[prop.tableName] = this.tables[prop.tableName] || {
        files: []
      };
      this.tables[prop.tableName].files.push(...props);
    }
  }

  async registerFileBuffer(prop: FileBufferStore): Promise<void> {
    this.fileBufferStore[prop.fileName] = prop;
    this.tables[prop.tableName] = this.tables[prop.tableName] || { files: [] };
    this.tables[prop.tableName].files.push(prop);
  }

  async registerJSON(prop: FileJsonStore): Promise<void> {
    const { json, ...fileData } = prop;

    this.registerFileBuffer({
      ...fileData,
      buffer: new Uint8Array()
    });
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

  async getFilesNameForTables(tableNames: string[]): Promise<TableWiseFiles[]> {
    const data: TableWiseFiles[] = [];

    for (const tableName of tableNames) {
      const files: string[] = [];

      for (const key in this.fileBufferStore) {
        if (this.fileBufferStore[key].tableName === tableName) {
          files.push(key);
        }
      }

      data.push({
        tableName,
        files
      });
    }

    return data;
  }

  async getTableData(tableName: string): Promise<Table | undefined> {
    return this.tables[tableName];
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
      close: async () => {
        // do nothing
      }
    };
  }
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
  let instanceManager;

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
      }
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
    it('should call the preQuery hook', async () => {
      const preQuery = jest.fn();

      await fileManager.registerFileBuffer({
        fileName: 'file1',
        tableName: 'table1',
        buffer: new Uint8Array()
      });

      const result = await dbm.queryWithTableNames(
        'SELECT * FROM table1',
        ['table1'],
        {
          preQuery
        }
      );

      expect(preQuery).toBeCalledTimes(1);

      expect(preQuery).toBeCalledWith([
        {
          tableName: 'table1',
          files: ['file1']
        }
      ]);
    });

    it('should execute a query with table names', async () => {
      const result = await dbm.queryWithTableNames('SELECT * FROM table1', [
        'table1'
      ]);
      expect(result).toEqual(['SELECT * FROM table1']);
    });

    it('should execute multiple queries with table names', async () => {
      const promise1 = dbm.queryWithTableNames('SELECT * FROM table1', [
        'table1'
      ]);
      const promise2 = dbm.queryWithTableNames('SELECT * FROM table2', [
        'table1'
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
        'table1'
      ]);

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
          shutdownInactiveTime: 100
        }
      };
      const dbm = new DBM(options);

      /**
       * Execute a query
       */
      const promise1 = dbm.queryWithTableNames('SELECT * FROM table1', [
        'table1'
      ]);

      /**
       * Execute another query
       */
      const promise2 = dbm.queryWithTableNames('SELECT * FROM table2', [
        'table1'
      ]);

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
        }
      };
      const dbm = new DBM(options);

      /**
       * Execute a query
       */
      const promise1 = dbm.queryWithTableNames('SELECT * FROM table1', [
        'table1'
      ]);

      /**
       * Execute another query
       */
      const promise2 = dbm.queryWithTableNames('SELECT * FROM table2', [
        'table1'
      ]);

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
  });
});
