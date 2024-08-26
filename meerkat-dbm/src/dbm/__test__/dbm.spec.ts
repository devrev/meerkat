import log from 'loglevel';
import { FileManagerType } from '../../file-manager/file-manager-type';
import { DBM } from '../dbm';
import { DBMConstructorOptions } from '../types';
import { InstanceManager, MockFileManager } from './mock';

describe('DBM', () => {
  let fileManager: FileManagerType;
  let dbm: DBM;
  let instanceManager: InstanceManager;

  const tables = [{ name: 'table1' }];

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

  describe('table locks', () => {
    it('should lock the table and release it', async () => {
      const tableName = 'exampleTable';

      // Request the lock for the table and then release it
      await dbm.lockTables([tableName]);

      expect(dbm.isTableLocked(tableName)).toBe(true);

      await dbm.unlockTables([tableName]);

      expect(dbm.isTableLocked(tableName)).toBe(false);

      // Again request the lock for the table
      await dbm.lockTables([tableName]);

      await dbm.unlockTables([tableName]);
    });

    it('two consumers requesting lock for the same table', async () => {
      const tableName = 'exampleTable';

      // Set up promises for the two consumers
      const consumer1Promise = dbm.lockTables([tableName]);
      const consumer2Promise = dbm.lockTables([tableName]);

      // Wait for the first consumer to get the lock
      await expect(consumer1Promise).resolves.toBeUndefined();

      expect(dbm.isTableLocked(tableName)).toBe(true);

      const timeout1 = new Promise((resolve) => {
        setTimeout(resolve, 1000, 'TIMEOUT');
      });

      // Promise.race will wait for either the promises be resolved
      // consumer2 will not be able to get the lock as it is already locked by consumer1
      await expect(Promise.race([consumer2Promise, timeout1])).resolves.toBe(
        'TIMEOUT'
      );

      // Release the lock for the first consumer
      await dbm.unlockTables([tableName]);

      // Check if the table is still locked as the consumer2 will get the lock
      expect(dbm.isTableLocked(tableName)).toBe(true);

      const timeout2 = new Promise((resolve) => {
        setTimeout(resolve, 1000, 'TIMEOUT');
      });

      // This time the consumer2 will get the lock
      await expect(
        Promise.race([consumer2Promise, timeout2])
      ).resolves.toBeUndefined();

      // Release the lock
      await dbm.unlockTables([tableName]);

      expect(dbm.isTableLocked(tableName)).toBe(false);
    });
  });
});
