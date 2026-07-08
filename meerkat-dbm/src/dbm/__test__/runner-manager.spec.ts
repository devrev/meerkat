import log from 'loglevel';
import { BROWSER_RUNNER_TYPE } from '../../window-communication/runner-types';
import { IFrameRunnerManager } from '../dbm-parallel/runner-manager';

const mockSendResponse = jest.fn();

jest.mock('../dbm-parallel/iframe-manager', () => {
  return {
    IFrameManager: jest.fn().mockImplementation(() => ({
      communication: {
        sendResponse: mockSendResponse,
      },
      destroy: jest.fn(),
    })),
  };
});

describe('IFrameRunnerManager', () => {
  let manager: IFrameRunnerManager;
  const fetchTableFileBuffersMock = jest.fn();

  beforeEach(() => {
    fetchTableFileBuffersMock.mockResolvedValue([
      {
        tableName: 'table1',
        fileName: 'file1.parquet',
        buffer: new Uint8Array([1, 2, 3]),
      },
    ]);
    manager = new IFrameRunnerManager({
      fetchTableFileBuffers: fetchTableFileBuffersMock,
      totalRunners: 1,
      logger: log,
      runnerURL: 'http://localhost:3000',
      origin: 'http://localhost:3001',
      fetchPreQuery: jest.fn(),
    });
  });

  it('should start runners and create IFrameManager instances', () => {
    expect(manager.getRunnerIds()).toHaveLength(0);

    // Start runners and check if they are started
    manager.startRunners();
    expect(manager.getRunnerIds()).toHaveLength(1);
  });

  it('should stop all runners and clear the map', () => {
    manager.startRunners();
    expect(manager.getRunnerIds()).toHaveLength(1);

    // Stop runners and check if they are stopped
    manager.stopRunners();
    expect(manager.getRunnerIds()).toHaveLength(0);
  });

  it('should add IFrameManager instances', () => {
    // Add IFrameManager instances
    manager['addIFrameManager']('0');
    manager['addIFrameManager']('1');

    // Check if IFrameManager instances are added
    expect(manager['iFrameManagers'].size).toBe(2);
  });

  it('should handle RUNNER_ON_READY message', async () => {
    const message = {
      uuid: 'mock-uuid',
      message: {
        type: BROWSER_RUNNER_TYPE.RUNNER_ON_READY,
        payload: { tables: [] },
      },
      target_app: 'runner',
      timestamp: Date.now(),
    };

    // Start runners
    manager.startRunners();

    // Initially iframe is not ready
    expect(manager['iFrameReadyMap'].get('0')).toBe(false);

    // Set iframe as ready
    manager['messageListener']('0', message);

    // Verify if iframe is set ready
    const iframeStatus = await manager.isFrameRunnerReady();
    await expect(iframeStatus).toBe(true);
  });

  it('should handle RUNNER_GET_FILE_BUFFERS message', async () => {
    const message = {
      uuid: 'mock-uuid',
      message: {
        type: BROWSER_RUNNER_TYPE.RUNNER_GET_FILE_BUFFERS,
        payload: { tables: [{ name: 'table1' }] },
      },
      target_app: 'runner',
      timestamp: Date.now(),
    };

    // Start runners
    manager.startRunners();

    // Send message to get file buffers
    manager['messageListener']('0', message);

    // Verify if fetchTableFileBuffers is called
    expect(fetchTableFileBuffersMock).toHaveBeenCalledWith(
      message.message.payload.tables
    );
  });

  const onEventMessage = (scope?: 'query' | 'instance') => ({
    uuid: 'mock-uuid',
    message: {
      type: BROWSER_RUNNER_TYPE.RUNNER_ON_EVENT,
      payload: { event_name: 'query_execution_duration', duration: 5 },
      scope,
    },
    target_app: 'runner',
    timestamp: 0,
  });

  it('dispatches a query-scoped RUNNER_ON_EVENT to the callback registered for the emitting runner', () => {
    const onEvent = jest.fn();
    manager.registerQueryEventCallback('0', onEvent);

    manager['messageListener']('0', onEventMessage('query') as never);

    expect(onEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        event_name: 'query_execution_duration',
        duration: 5,
      })
    );
  });

  it('stops dispatching after the per-query callback is unregistered', () => {
    const onEvent = jest.fn();
    manager.registerQueryEventCallback('0', onEvent);
    manager.unregisterQueryEventCallback('0');

    manager['messageListener']('0', onEventMessage('query') as never);

    expect(onEvent).not.toHaveBeenCalled();
  });

  it('does not dispatch to a callback registered for a different runner', () => {
    const onEvent = jest.fn();
    manager.registerQueryEventCallback('0', onEvent);

    manager['messageListener']('1', onEventMessage('query') as never);

    expect(onEvent).not.toHaveBeenCalled();
  });

  it('routes exclusively: the instance onEvent does not fire for a query-scoped event when a per-runner callback is registered', () => {
    const instanceOnEvent = jest.fn();
    manager['onEvent'] = instanceOnEvent;
    const perQueryOnEvent = jest.fn();
    manager.registerQueryEventCallback('0', perQueryOnEvent);

    manager['messageListener']('0', onEventMessage('query') as never);

    expect(perQueryOnEvent).toHaveBeenCalledTimes(1);
    expect(instanceOnEvent).not.toHaveBeenCalled();
  });

  it('falls back to the instance onEvent when no per-runner callback is registered', () => {
    const instanceOnEvent = jest.fn();
    manager['onEvent'] = instanceOnEvent;

    manager['messageListener']('0', onEventMessage('query') as never);

    expect(instanceOnEvent).toHaveBeenCalledWith(
      expect.objectContaining({ event_name: 'query_execution_duration' })
    );
  });

  it('routes an instance-scoped event to the instance sink even while a per-runner callback is registered', () => {
    const instanceOnEvent = jest.fn();
    manager['onEvent'] = instanceOnEvent;
    const perQueryOnEvent = jest.fn();
    manager.registerQueryEventCallback('0', perQueryOnEvent);

    // The runner tags clone_buffer_duration (buffer setup) as instance scope.
    manager['messageListener'](
      '0',
      {
        uuid: 'mock-uuid',
        message: {
          type: BROWSER_RUNNER_TYPE.RUNNER_ON_EVENT,
          payload: { event_name: 'clone_buffer_duration', duration: 9 },
          scope: 'instance',
        },
        target_app: 'runner',
        timestamp: 0,
      } as never
    );

    expect(instanceOnEvent).toHaveBeenCalledWith(
      expect.objectContaining({ event_name: 'clone_buffer_duration' })
    );
    expect(perQueryOnEvent).not.toHaveBeenCalled();
  });

  it('falls back to the instance sink when a RUNNER_ON_EVENT carries no scope (older runner bundle)', () => {
    const instanceOnEvent = jest.fn();
    manager['onEvent'] = instanceOnEvent;
    const perQueryOnEvent = jest.fn();
    manager.registerQueryEventCallback('0', perQueryOnEvent);

    manager['messageListener']('0', onEventMessage(undefined) as never);

    expect(instanceOnEvent).toHaveBeenCalledTimes(1);
    expect(perQueryOnEvent).not.toHaveBeenCalled();
  });
});
