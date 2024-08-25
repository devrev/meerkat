import { FileManagerType } from '../../file-manager/file-manager-type';
import { DBMLogger } from '../../logger/logger-types';
import { BROWSER_RUNNER_TYPE } from '../../window-communication/runner-types';
import { DBMParallel } from '../dbm-parallel/dbm-parallel';
import { IFrameRunnerManager } from '../dbm-parallel/runner-manager';
import { InstanceManager } from './mock';

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
const fileManagerMock: FileManagerType = {
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
    fileManager = fileManagerMock;
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

    it('should shut down after all queries are complete and timeout elapses', async () => {
      jest.useFakeTimers();

      runnerMock.communication.sendRequest.mockResolvedValue({
        message: { isError: false, data: [{ data: 1 }] },
      });

      await dbmParallel.queryWithTables({
        query: 'SELECT * FROM table1',
        tables: [],
      });

      // Advance timer to time less than the shutdown time
      jest.advanceTimersByTime(500);

      // Check if the shutdown is not triggered
      expect(fileManagerMock.onDBShutdownHandler).not.toHaveBeenCalled();
      expect(iFrameRunnerManager.stopRunners).not.toHaveBeenCalled();

      // Advance timer to time more than the shutdown time
      jest.advanceTimersByTime(600);

      expect(fileManagerMock.onDBShutdownHandler).toHaveBeenCalled();
      expect(iFrameRunnerManager.stopRunners).toHaveBeenCalled();

      jest.useRealTimers();
    });
  });
});
