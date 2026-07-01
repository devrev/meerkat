import log from 'loglevel';
import {
  FileBufferStore,
  FileJsonStore,
  FileManagerType,
} from '../../file-manager/file-manager-type';
import { FileData, Table } from '../../types';
import { DBM } from '../dbm';
import { DBMConstructorOptions, TableConfig } from '../types';
import { InstanceManager, mockDB } from './mock';

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

  async getFilesNameForTables(tableNames: TableConfig[]): Promise<Table[]> {
    const data: Table[] = [];

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

describe('DBM', () => {
  let fileManager: FileManagerType;
  let dbm: DBM;
  let instanceManager: InstanceManager;

  const tables = [{ name: 'table1' }];
  const onCreateConnection = jest
    .fn()
    .mockImplementation(async (connection) => {
      await connection.query('SELECT 2');
    });

  beforeAll(async () => {
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
      onCreateConnection,
    };
    dbm = new DBM(options);
  });

  afterEach(() => {
    onCreateConnection.mockClear();
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

    it('should deduplicate tables by name before executing query', async () => {
      const duplicateTables = [
        { name: 'table1' },
        { name: 'table2' },
        { name: 'table1' },
        { name: 'table3' },
        { name: 'table2' },
      ];

      const mockMountFileBufferByTables = jest.spyOn(
        fileManager,
        'mountFileBufferByTables'
      );

      await dbm.queryWithTables({
        query: 'SELECT * FROM table1 JOIN table2 JOIN table3',
        tables: duplicateTables,
      });

      // Verify that only unique tables were passed to mountFileBufferByTables
      expect(mockMountFileBufferByTables).toHaveBeenCalledWith([
        { name: 'table1' },
        { name: 'table2' },
        { name: 'table3' },
      ]);
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

  describe('recycle the db test', () => {
    const buildDbm = (
      recycleOpts: {
        recycleInactiveTime?: number;
        shutdownInactiveTime?: number;
        shouldRecycle?: () => boolean | Promise<boolean>;
      },
      onShutdown?: () => void
    ) => {
      const instanceManager = new InstanceManager();
      instanceManager.terminateDB = jest.fn();
      const getDBSpy = jest.spyOn(instanceManager, 'getDB');
      const dbm = new DBM({
        instanceManager,
        fileManager,
        logger: log,
        onEvent: () => undefined,
        onDuckDBShutdown: onShutdown,
        options: recycleOpts,
      });
      return { dbm, instanceManager, getDBSpy };
    };

    it('recycles (terminate + restart) at recycleInactiveTime, then shuts down at shutdownInactiveTime', async () => {
      const onDuckDBShutdown = jest.fn();
      const { dbm, instanceManager, getDBSpy } = buildDbm(
        { recycleInactiveTime: 100, shutdownInactiveTime: 300 },
        onDuckDBShutdown
      );

      await dbm.queryWithTables({ query: 'SELECT 1', tables });
      getDBSpy.mockClear();

      // Before recycle window: nothing fired.
      await new Promise((r) => setTimeout(r, 50));
      expect(instanceManager.terminateDB).not.toBeCalled();

      // After recycle window: terminate + restart (getDB) once.
      await new Promise((r) => setTimeout(r, 100));
      expect(instanceManager.terminateDB).toBeCalledTimes(1);
      expect(getDBSpy).toBeCalledTimes(1); // eager restart

      // After shutdown window: final shutdown terminates again, no restart.
      await new Promise((r) => setTimeout(r, 200));
      expect(instanceManager.terminateDB).toBeCalledTimes(2);
      expect(getDBSpy).toBeCalledTimes(1);
    });

    it('skips the recycle when shouldRecycle returns false', async () => {
      const { dbm, instanceManager, getDBSpy } = buildDbm({
        recycleInactiveTime: 100,
        shutdownInactiveTime: 300,
        shouldRecycle: () => false,
      });

      await dbm.queryWithTables({ query: 'SELECT 1', tables });
      getDBSpy.mockClear();

      // Past the recycle window — gate said no, so no terminate yet.
      await new Promise((r) => setTimeout(r, 180));
      expect(instanceManager.terminateDB).not.toBeCalled();
      expect(getDBSpy).not.toBeCalled();

      // Final shutdown still fires.
      await new Promise((r) => setTimeout(r, 180));
      expect(instanceManager.terminateDB).toBeCalledTimes(1);
    });

    it('recycles at most once per idle period', async () => {
      const { dbm, instanceManager } = buildDbm({
        recycleInactiveTime: 100,
        // No shutdown — isolate recycle behavior.
      });

      await dbm.queryWithTables({ query: 'SELECT 1', tables });

      // Wait well past multiple recycle windows.
      await new Promise((r) => setTimeout(r, 400));
      expect(instanceManager.terminateDB).toBeCalledTimes(1);
    });

    it('re-arms the recycle after new activity', async () => {
      const { dbm, instanceManager } = buildDbm({ recycleInactiveTime: 100 });

      await dbm.queryWithTables({ query: 'SELECT 1', tables });
      await new Promise((r) => setTimeout(r, 150));
      expect(instanceManager.terminateDB).toBeCalledTimes(1);

      // New query → idle period resets → recycle can fire again.
      await dbm.queryWithTables({ query: 'SELECT 2', tables });
      await new Promise((r) => setTimeout(r, 150));
      expect(instanceManager.terminateDB).toBeCalledTimes(2);
    });

    it('does not recycle if a query starts executing while shouldRecycle awaits', async () => {
      // A slow async gate widens the window between the idle re-check and the
      // recycle. A query that arrives in that window must suppress the recycle
      // so _recycle() never tears down a live connection mid-query.
      let resolveGate: (value: boolean) => void = () => undefined;
      const shouldRecycle = () =>
        new Promise<boolean>((resolve) => {
          resolveGate = resolve;
        });

      const { dbm, instanceManager } = buildDbm({
        recycleInactiveTime: 100,
        shouldRecycle,
      });

      await dbm.queryWithTables({ query: 'SELECT 1', tables });

      // Let the recycle timer fire and enter the (pending) gate.
      await new Promise((r) => setTimeout(r, 150));
      expect(instanceManager.terminateDB).not.toBeCalled();

      // A new query arrives and begins executing while the gate is pending.
      const queryPromise = dbm.queryWithTables({ query: 'SELECT 2', tables });

      // Gate now resolves true — the post-await idle re-check must see the
      // busy queue and skip the recycle.
      resolveGate(true);
      await queryPromise;

      expect(instanceManager.terminateDB).not.toBeCalled();
    });

    it('waits for an in-flight recycle before binding a connection (no dead connection)', async () => {
      // Reproduces the teardown/getConnection race: a query that arrives while
      // _recycle() is terminating the instance must NOT bind to the instance
      // being destroyed. With the barrier it waits until the recycle finishes
      // (terminate + warm re-instantiate), then connects to the fresh instance.
      const events: string[] = [];

      const instanceManager = new InstanceManager();
      instanceManager.getDB = jest.fn(async () => {
        events.push('getDB');
        return mockDB as unknown as Awaited<
          ReturnType<typeof instanceManager.getDB>
        >;
      });
      // Slow terminate so the recycle is still mid-flight when the next query
      // arrives — this is the window the barrier must cover.
      instanceManager.terminateDB = jest.fn(
        () =>
          new Promise<void>((resolve) =>
            setTimeout(() => {
              events.push('terminate-done');
              resolve();
            }, 150)
          )
      );
      const connectSpy = jest.spyOn(mockDB, 'connect');

      const dbm = new DBM({
        instanceManager,
        fileManager,
        logger: log,
        onEvent: () => undefined,
        options: { recycleInactiveTime: 100 },
      });

      await dbm.queryWithTables({ query: 'SELECT 1', tables });
      connectSpy.mockClear();
      connectSpy.mockImplementation(async () => {
        events.push('connect');
        return {
          query: async (q: string) => [q],
          cancelSent: async () => true,
          close: async () => undefined,
        } as unknown as Awaited<ReturnType<typeof mockDB.connect>>;
      });

      // Let the recycle fire; terminate is now in flight.
      await new Promise((r) => setTimeout(r, 120));
      expect(instanceManager.terminateDB).toBeCalledTimes(1);

      // Query arrives mid-recycle. It must block on the barrier, not connect now.
      const result = await dbm.queryWithTables({ query: 'SELECT 2', tables });

      // Let the recycle's slow terminate + re-instantiate fully settle so the
      // event ordering is complete before we assert on it.
      await new Promise((r) => setTimeout(r, 250));

      // The query succeeded against the warm instance...
      expect(result).toEqual(['SELECT 2']);
      // ...the teardown actually finished its terminate...
      expect(events).toContain('terminate-done');
      // ...and crucially the query connected only AFTER that terminate, i.e. it
      // bound to the fresh instance, never the one being torn down.
      expect(events.indexOf('connect')).toBeGreaterThan(
        events.indexOf('terminate-done')
      );

      connectSpy.mockRestore();
    });

    it('throws when recycleInactiveTime is not less than shutdownInactiveTime', () => {
      expect(() =>
        buildDbm({ recycleInactiveTime: 300, shutdownInactiveTime: 300 })
      ).toThrow(/recycleInactiveTime/);
      expect(() =>
        buildDbm({ recycleInactiveTime: 400, shutdownInactiveTime: 300 })
      ).toThrow(/recycleInactiveTime/);
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

  describe('create connection callback', () => {
    it('should call the create connection callback', async () => {
      await dbm.query('SELECT 1');
      expect(onCreateConnection).toHaveBeenCalled();

      expect(onCreateConnection).toHaveBeenCalledWith(
        // arguments of the mockdb connection return object
        expect.objectContaining({
          query: expect.any(Function),
          cancelSent: expect.any(Function),
          close: expect.any(Function),
        })
      );

      /**
       * wait for 200ms
       */
      await new Promise((resolve) => setTimeout(resolve, 200));

      /**
       * after shutdown on new connection creation
       * also call the create connection callback
       */

      await dbm.query('SELECT 2');
      expect(onCreateConnection).toHaveBeenCalled();
      expect(onCreateConnection).toHaveBeenCalledWith(
        expect.objectContaining({
          query: expect.any(Function),
          cancelSent: expect.any(Function),
          close: expect.any(Function),
        })
      );
    });
  });

  describe('shutdown does not fire while a query is in flight', () => {
    it('does not terminate the worker when the shutdown timer elapses mid-query', async () => {
      const instanceManager = new InstanceManager();
      instanceManager.terminateDB = jest.fn();
      const onDuckDBShutdown = jest.fn();

      const dbm = new DBM({
        instanceManager,
        fileManager,
        logger: log,
        onEvent: () => undefined,
        onDuckDBShutdown,
        options: {
          shutdownInactiveTime: 100,
        },
      });

      // preQuery holds the query in flight (currentQueryItem set, queue empty)
      // well past shutdownInactiveTime, reproducing the mid-query window where
      // the shutdown timer previously fired and killed the worker.
      const result = await dbm.queryWithTables({
        query: 'SELECT * FROM table1',
        tables,
        options: {
          preQuery: async () => {
            await new Promise((resolve) => setTimeout(resolve, 250));
            // The shutdown timer has elapsed by now; it must have seen the
            // in-flight query and skipped terminateDB.
            expect(instanceManager.terminateDB).not.toBeCalled();
          },
        },
      });

      expect(result).toEqual(['SELECT * FROM table1']);
      // Still not terminated immediately after the query resolves.
      expect(instanceManager.terminateDB).not.toBeCalled();

      // Once idle, the re-armed timer shuts down normally.
      await new Promise((resolve) => setTimeout(resolve, 200));
      expect(instanceManager.terminateDB).toBeCalled();
      expect(onDuckDBShutdown).toBeCalled();
    });

    it('cancels the pending shutdown timer when a new query starts', async () => {
      const instanceManager = new InstanceManager();
      instanceManager.terminateDB = jest.fn();

      const dbm = new DBM({
        instanceManager,
        fileManager,
        logger: log,
        onEvent: () => undefined,
        options: {
          shutdownInactiveTime: 100,
        },
      });

      // First query drains the queue and arms the shutdown timer.
      await dbm.queryWithTables({ query: 'SELECT * FROM table1', tables });

      // Start a second query before the timer elapses. Starting it must cancel
      // the pending timer so it never fires against this query.
      await new Promise((resolve) => setTimeout(resolve, 50));
      await dbm.queryWithTables({ query: 'SELECT * FROM table2', tables });

      // The original timer's window has now fully passed; it must not have
      // fired because starting the second query cleared it.
      await new Promise((resolve) => setTimeout(resolve, 70));
      expect(instanceManager.terminateDB).not.toBeCalled();
    });
  });
});
