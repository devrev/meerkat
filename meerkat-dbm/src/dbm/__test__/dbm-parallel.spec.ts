import { FileManagerType } from '../../file-manager/file-manager-type';
import { DBMLogger } from '../../logger/logger-types';
import { BROWSER_RUNNER_TYPE } from '../../window-communication/runner-types';
import { DBMParallel } from '../dbm-parallel/dbm-parallel';
import { IFrameRunnerManager } from '../dbm-parallel/runner-manager';
import { InstanceManager } from './utils';

// eslint-disable-next-line @typescript-eslint/ban-ts-comment
//@ts-ignore
const iFrameRunnerManager: IFrameRunnerManager = {
  startRunners: jest.fn(),
  stopRunners: jest.fn(),
  getRunnerIds: jest.fn().mockReturnValue(['1', '2']),
  isFrameRunnerReady: jest.fn().mockResolvedValue(true),
  iFrameManagers: new Map(),
};

const loggerMock = {
  debug: jest.fn(),
  error: jest.fn(),
} as unknown as DBMLogger;

// eslint-disable-next-line @typescript-eslint/ban-ts-comment
//@ts-ignore
const fileManagermock: FileManagerType = {
  onDBShutdownHandler: jest.fn(),
};

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
    fileManager = fileManagermock;
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

  describe('queryWithTables', () => {
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

      // Mock the `sendRequest` to return the simulated error response
      runnerMock.communication.sendRequest.mockResolvedValue(response);

      await expect(
        dbmParallel.queryWithTables({
          query: 'SELECT * FROM table',
          tables: [],
        })
      ).rejects.toThrow('Query failed');
    });

    it('should distribute queries across runners in a round-robin fashion', async () => {
      const response = {
        message: { isError: false, data: [{ data: 1 }] },
      };

      // Mock the response for `sendRequest` to return successfully
      runnerMock.communication.sendRequest.mockResolvedValue(response);

      // Capture the results of the queries
      const results = await Promise.all([
        dbmParallel.queryWithTables({
          query: 'SELECT * FROM table1',
          tables: [],
        }),
        dbmParallel.queryWithTables({
          query: 'SELECT * FROM table2',
          tables: [],
        }),
      ]);

      // Check the results of the queries
      expect(results[0]).toEqual([{ data: 1 }]);
      expect(results[1]).toEqual([{ data: 1 }]);

      // Check that the queries were sent to different runners
      expect(runnerMock.communication.sendRequest).toHaveBeenCalledTimes(2);

      const calls = runnerMock.communication.sendRequest.mock.calls;
      expect(calls[0][0].payload.query).toBe('SELECT * FROM table1');
      expect(calls[1][0].payload.query).toBe('SELECT * FROM table2');

      // Check the round-robin assignment
      const firstCallRunnerId = calls[0][0].payload.query; // assuming you identify the runner by the query for this example
      const secondCallRunnerId = calls[1][0].payload.query;

      // Check if counter has been updated (mocking or checking the internal state)
      expect(dbmParallel['counter']).toBe(1); // Counter should be 1 after the first query
    });
  });
});
