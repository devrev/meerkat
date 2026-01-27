import { FileManagerType } from '../../file-manager/file-manager-type';
import { DBMLogger } from '../../logger/logger-types';
import { BROWSER_RUNNER_TYPE } from '../../window-communication/runner-types';
import { DBMParallel } from '../dbm-parallel/dbm-parallel';
import { IFrameRunnerManager } from '../dbm-parallel/runner-manager';
import { MockFileManager } from './dbm.spec';
import { InstanceManager } from './mock';

const loggerMock = {
  debug: jest.fn(),
  error: jest.fn(),
  info: jest.fn(),
} as unknown as DBMLogger;

const iFrameRunnerManager = {
  startRunners: jest.fn(),
  stopRunners: jest.fn(),
  getRunnerIds: jest.fn().mockReturnValue(['1', '2']),
  isFrameRunnerReady: jest.fn().mockResolvedValue(true),
  fetchTableFileBuffers: jest.fn().mockResolvedValue([]),
  totalRunners: 2,
  iFrameManagers: new Map(),
  iFrameReadyMap: new Map(),
  logger: loggerMock, // Adjusted to use the loggerMock
  addIFrameManager: jest.fn(),
  areRunnersRunning: jest.fn(),
  messageListener: jest.fn(),
} as unknown as jest.Mocked<IFrameRunnerManager>;

const runnerMock = {
  communication: {
    sendRequest: jest.fn(),
    sendRequestWithoutResponse: jest.fn(),
  },
};

iFrameRunnerManager.iFrameManagers.set('1', runnerMock as any);
iFrameRunnerManager.iFrameManagers.set('2', runnerMock as any);

describe('DBMParallel', () => {
  let dbmParallel: DBMParallel;
  let fileManager: FileManagerType;
  let instanceManager: InstanceManager;

  beforeAll(async () => {
    fileManager = new MockFileManager();
    instanceManager = new InstanceManager();
  });

  beforeEach(() => {
    dbmParallel = new DBMParallel({
      fileManager,
      logger: loggerMock,
      options: { shutdownInactiveTime: 1000 },
      iFrameRunnerManager,
      instanceManager: instanceManager,
    });
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should execute query with tables successfully', async () => {
    runnerMock.communication.sendRequest.mockResolvedValue({
      message: { isError: false, data: [{ data: 1 }] },
    });

    const result = await dbmParallel.queryWithTables({
      query: 'SELECT * FROM table',
      tables: [],
    });

    expect(result).toEqual([{ data: 1 }]);
    expect(runnerMock.communication.sendRequest).toHaveBeenCalledWith(
      expect.objectContaining({
        type: BROWSER_RUNNER_TYPE.EXEC_QUERY,
        payload: expect.objectContaining({
          query: 'SELECT * FROM table',
          tables: [],
          queryId: expect.any(String),
          options: {
            signal: undefined,
          },
        }),
      })
    );
  });

  it('should handle query execution errors', async () => {
    const response = {
      message: { isError: true, error: 'Query failed', data: [] },
    };

    runnerMock.communication.sendRequest.mockResolvedValue(response);

    await expect(
      dbmParallel.queryWithTables({
        query: 'SELECT * FROM table',
        tables: [],
      })
    ).rejects.toThrow('Query failed');
  });

  it('should shut down after all queries are complete and timeout elapses', async () => {
    jest.useFakeTimers();

    runnerMock.communication.sendRequest.mockResolvedValue({
      message: { isError: false, data: [{ data: 1 }] },
    });

    // Execute a query
    await dbmParallel.queryWithTables({
      query: 'SELECT * FROM table1',
      tables: [],
    });

    // Advance timer to time less than the shutdown time
    jest.advanceTimersByTime(500);

    // Check if the shutdown is not triggered
    expect(fileManager.onDBShutdownHandler).not.toHaveBeenCalled();
    expect(iFrameRunnerManager.stopRunners).not.toHaveBeenCalled();

    // Advance timer to time more than the shutdown time
    jest.advanceTimersByTime(600);

    expect(fileManager.onDBShutdownHandler).toHaveBeenCalled();
    expect(iFrameRunnerManager.stopRunners).toHaveBeenCalled();

    jest.useRealTimers();
  });

  it('should clear previous shutdown timer if a new query arrives before shutdown', async () => {
    jest.useFakeTimers();

    // Simulate a query execution
    runnerMock.communication.sendRequest.mockResolvedValue({
      message: { isError: false, data: [{ data: 1 }] },
    });

    await dbmParallel.queryWithTables({
      query: 'SELECT * FROM table1',
      tables: [],
    });

    // Advance timer to time 100 less than the shutdown time
    jest.advanceTimersByTime(900);

    // Execute another query
    await dbmParallel.queryWithTables({
      query: 'SELECT * FROM table2',
      tables: [],
    });

    // Shutdown timer should be reset due to the new query
    jest.advanceTimersByTime(900);

    // Shutdown should not have been triggered
    expect(fileManager.onDBShutdownHandler).not.toHaveBeenCalled();
    expect(iFrameRunnerManager.stopRunners).not.toHaveBeenCalled();

    jest.useRealTimers();
  });

  it('should wait for a write lock to be released before executing a query', async () => {
    const tableName = 'lockedTable';
    let queryExecuted = false;

    // Mock sendRequest to track execution
    runnerMock.communication.sendRequest.mockImplementation(() => {
      queryExecuted = true;
      return Promise.resolve({
        message: { isError: false, data: [{ data: 1 }] },
      });
    });

    // Acquire a write lock
    await dbmParallel.lockTables([tableName], 'write');

    // Execute a query that requires a read lock on the same table
    const queryPromise = dbmParallel.queryWithTables({
      query: `SELECT * FROM ${tableName}`,
      tables: [{ name: tableName }],
    });

    // The query should not have been executed yet because of the write lock
    expect(queryExecuted).toBe(false);

    // Release the write lock
    dbmParallel.unlockTables([tableName], 'write');

    // Now the query should be able to execute
    await queryPromise;
    expect(queryExecuted).toBe(true);
  });

  it('should deduplicate tables by name before executing query', async () => {
    runnerMock.communication.sendRequest.mockResolvedValue({
      message: { isError: false, data: [{ data: 1 }] },
    });

    const duplicateTables = [
      { name: 'table1' },
      { name: 'table2' },
      { name: 'table1' },
      { name: 'table3' },
      { name: 'table2' },
    ];

    await dbmParallel.queryWithTables({
      query: 'SELECT * FROM table1 JOIN table2 JOIN table3',
      tables: duplicateTables,
    });

    // Verify that only unique tables were sent to the runner
    expect(runnerMock.communication.sendRequest).toHaveBeenCalledWith(
      expect.objectContaining({
        type: BROWSER_RUNNER_TYPE.EXEC_QUERY,
        payload: expect.objectContaining({
          tables: [{ name: 'table1' }, { name: 'table2' }, { name: 'table3' }],
        }),
      })
    );
  });

  describe('Abort Signal', () => {
    it('should abort a query when abort signal is triggered', async () => {
      const abortController = new AbortController();

      // Mock a long-running query that never resolves
      runnerMock.communication.sendRequest.mockImplementation(
        () =>
          new Promise(() => {
            // Never resolves - simulates a long-running query
          })
      );

      const queryPromise = dbmParallel.queryWithTables({
        query: 'SELECT * FROM table',
        tables: [],
        options: { signal: abortController.signal },
      });

      // Give a small delay to ensure the query has started
      await new Promise((resolve) => setTimeout(resolve, 10));

      // Abort the query
      abortController.abort();

      await expect(queryPromise).rejects.toThrow('Query aborted by user');

      // Verify that cancel message was sent to the runner
      expect(
        runnerMock.communication.sendRequestWithoutResponse
      ).toHaveBeenCalledWith(
        expect.objectContaining({
          type: BROWSER_RUNNER_TYPE.CANCEL_QUERY,
          payload: expect.objectContaining({
            queryId: expect.any(String),
          }),
        })
      );
    });

    it('should not abort a query if signal is triggered after query completes', async () => {
      const abortController = new AbortController();

      runnerMock.communication.sendRequest.mockResolvedValue({
        message: { isError: false, data: [{ data: 1 }] },
      });

      const result = await dbmParallel.queryWithTables({
        query: 'SELECT * FROM table',
        tables: [],
        options: { signal: abortController.signal },
      });

      // Abort after query completes
      abortController.abort();

      expect(result).toEqual([{ data: 1 }]);
      // Cancel message should not be sent since query already completed
      expect(
        runnerMock.communication.sendRequestWithoutResponse
      ).not.toHaveBeenCalled();
    });

    it('should clean up abort signal listener after query completes', async () => {
      const abortController = new AbortController();
      const removeEventListenerSpy = jest.spyOn(
        abortController.signal,
        'removeEventListener'
      );

      runnerMock.communication.sendRequest.mockResolvedValue({
        message: { isError: false, data: [{ data: 1 }] },
      });

      await dbmParallel.queryWithTables({
        query: 'SELECT * FROM table',
        tables: [],
        options: { signal: abortController.signal },
      });

      expect(removeEventListenerSpy).toHaveBeenCalledWith(
        'abort',
        expect.any(Function)
      );
    });

    it('should clean up abort signal listener after query is aborted', async () => {
      const abortController = new AbortController();
      const removeEventListenerSpy = jest.spyOn(
        abortController.signal,
        'removeEventListener'
      );

      // Mock a long-running query that never resolves
      runnerMock.communication.sendRequest.mockImplementation(
        () =>
          new Promise(() => {
            // Never resolves
          })
      );

      const queryPromise = dbmParallel.queryWithTables({
        query: 'SELECT * FROM table',
        tables: [],
        options: { signal: abortController.signal },
      });

      // Give a small delay to ensure the query has started
      await new Promise((resolve) => setTimeout(resolve, 10));

      abortController.abort();

      await expect(queryPromise).rejects.toThrow('Query aborted by user');

      expect(removeEventListenerSpy).toHaveBeenCalledWith(
        'abort',
        expect.any(Function)
      );
    });

    it('should handle multiple queries with independent abort signals', async () => {
      const abortController1 = new AbortController();
      const abortController2 = new AbortController();

      // Mock queries that never resolve
      runnerMock.communication.sendRequest.mockImplementation(
        () =>
          new Promise(() => {
            // Never resolves
          })
      );

      const query1Promise = dbmParallel.queryWithTables({
        query: 'SELECT * FROM table1',
        tables: [],
        options: { signal: abortController1.signal },
      });

      const query2Promise = dbmParallel.queryWithTables({
        query: 'SELECT * FROM table2',
        tables: [],
        options: { signal: abortController2.signal },
      });

      // Give a small delay to ensure both queries have started
      await new Promise((resolve) => setTimeout(resolve, 10));

      // Abort only the first query
      abortController1.abort();

      await expect(query1Promise).rejects.toThrow('Query aborted by user');

      // Second query should still be running
      // Abort the second query as well to clean up
      abortController2.abort();

      await expect(query2Promise).rejects.toThrow('Query aborted by user');

      // Verify that cancel messages were sent for both queries
      expect(
        runnerMock.communication.sendRequestWithoutResponse
      ).toHaveBeenCalledTimes(2);
    });

    it('should execute query successfully without abort signal', async () => {
      runnerMock.communication.sendRequest.mockResolvedValue({
        message: { isError: false, data: [{ data: 1 }] },
      });

      const result = await dbmParallel.queryWithTables({
        query: 'SELECT * FROM table',
        tables: [],
        options: {},
      });

      expect(result).toEqual([{ data: 1 }]);
      expect(
        runnerMock.communication.sendRequestWithoutResponse
      ).not.toHaveBeenCalled();
    });

    it('should abort query during execution', async () => {
      const abortController = new AbortController();
      let resolveQuery: ((value: any) => void) | undefined;

      // Mock a query that we can control when it resolves
      runnerMock.communication.sendRequest.mockImplementation(
        () =>
          new Promise((resolve) => {
            resolveQuery = resolve;
          })
      );

      const queryPromise = dbmParallel.queryWithTables({
        query: 'SELECT * FROM table',
        tables: [],
        options: { signal: abortController.signal },
      });

      // Wait for query to start
      await new Promise((resolve) => setTimeout(resolve, 10));

      // Abort the query before it completes
      abortController.abort();

      // The query should be aborted
      await expect(queryPromise).rejects.toThrow('Query aborted by user');

      // Clean up - resolve the mock query if it's still pending
      if (resolveQuery) {
        resolveQuery({
          message: { isError: false, data: [{ data: 1 }] },
        });
      }
    });

    it('should strip signal from options when passing to iframe', async () => {
      const abortController = new AbortController();

      runnerMock.communication.sendRequest.mockResolvedValue({
        message: { isError: false, data: [{ data: 1 }] },
      });

      await dbmParallel.queryWithTables({
        query: 'SELECT * FROM table',
        tables: [],
        options: { signal: abortController.signal },
      });

      // Verify that signal is undefined in the options passed to iframe
      expect(runnerMock.communication.sendRequest).toHaveBeenCalledWith(
        expect.objectContaining({
          type: BROWSER_RUNNER_TYPE.EXEC_QUERY,
          payload: expect.objectContaining({
            options: expect.objectContaining({
              signal: undefined,
            }),
          }),
        })
      );
    });

    it('should not log error when query is aborted by user', async () => {
      const abortController = new AbortController();

      // Mock a long-running query that never resolves
      runnerMock.communication.sendRequest.mockImplementation(
        () =>
          new Promise(() => {
            // Never resolves
          })
      );

      const queryPromise = dbmParallel.queryWithTables({
        query: 'SELECT * FROM table',
        tables: [],
        options: { signal: abortController.signal },
      });

      // Give a small delay to ensure the query has started
      await new Promise((resolve) => setTimeout(resolve, 10));

      // Clear any previous error logs
      (loggerMock.error as jest.Mock).mockClear();

      // Abort the query
      abortController.abort();

      await expect(queryPromise).rejects.toThrow('Query aborted by user');

      // Logger should not have been called since it was a user-initiated abort
      expect(loggerMock.error).not.toHaveBeenCalled();
    });
  });
});
