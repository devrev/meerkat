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
    expect(runnerMock.communication.sendRequest).toHaveBeenCalledWith({
      type: BROWSER_RUNNER_TYPE.EXEC_QUERY,
      payload: {
        query: 'SELECT * FROM table',
        tables: [],
        options: undefined,
      },
    });
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
});
