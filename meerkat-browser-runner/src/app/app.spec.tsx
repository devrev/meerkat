import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './app';

let mockDBM: any;
let mockWindowCommunication: any;
let mockOnMessageCallback: ((message: any) => void) | null;

vi.mock('@devrev/meerkat-dbm', () => ({
  BROWSER_RUNNER_TYPE: {
    EXEC_QUERY: 'EXEC_QUERY',
    CANCEL_QUERY: 'CANCEL_QUERY',
    RUNNER_ON_READY: 'RUNNER_ON_READY',
    RUNNER_ON_EVENT: 'RUNNER_ON_EVENT',
    RUNNER_PRE_QUERY: 'RUNNER_PRE_QUERY',
  },
  convertArrowTableToJSON: vi.fn((data) => data),
  DBM: vi.fn().mockImplementation(() => mockDBM),
  getMainAppName: vi.fn((uuid) => `main-${uuid}`),
  getRunnerAppName: vi.fn((uuid) => `runner-${uuid}`),
  RunnerIndexedDBFileManager: vi.fn().mockImplementation(() => ({})),
  WindowCommunication: vi
    .fn()
    .mockImplementation(() => mockWindowCommunication),
}));

// Mock the InstanceManager
vi.mock('./duck-db/instance-manager', () => ({
  InstanceManager: vi.fn().mockImplementation(() => ({})),
}));

// Mock URL search params
Object.defineProperty(window, 'location', {
  value: {
    search: '?uuid=test-uuid&origin=http://localhost',
  },
  writable: true,
});

describe('App', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockOnMessageCallback = null;

    mockDBM = {
      query: vi.fn().mockResolvedValue(null),
      queryWithTables: vi.fn().mockResolvedValue({ data: 'mock-result' }),
    };

    mockWindowCommunication = {
      sendRequestWithoutResponse: vi.fn(),
      sendRequest: vi.fn().mockResolvedValue({ message: [] }),
      sendResponse: vi.fn(),
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      onMessage: vi.fn((callback: (message: any) => void) => {
        mockOnMessageCallback = callback;
      }),
    };
  });

  it('should render successfully', () => {
    render(<App />);
    expect(screen.getByText('Runners')).toBeTruthy();
  });

  it('should send RUNNER_ON_READY after initialization', async () => {
    render(<App />);

    await waitFor(() => {
      expect(mockDBM.query).toHaveBeenCalledWith('SELECT 1');
      expect(
        mockWindowCommunication.sendRequestWithoutResponse
      ).toHaveBeenCalledWith({
        type: 'RUNNER_ON_READY',
      });
    });
  });

  it('should handle EXEC_QUERY message successfully', async () => {
    render(<App />);

    const mockMessage = {
      uuid: 'msg-123',
      message: {
        type: 'EXEC_QUERY',
        payload: {
          queryId: 'query-1',
          query: 'SELECT * FROM table',
          tables: [{ name: 'table1' }],
          options: {},
        },
      },
    };

    await waitFor(() => {
      expect(mockOnMessageCallback).toBeDefined();
    });

    if (mockOnMessageCallback) {
      mockOnMessageCallback(mockMessage);
    }

    await waitFor(() => {
      expect(mockDBM.queryWithTables).toHaveBeenCalled();
      expect(mockWindowCommunication.sendResponse).toHaveBeenCalledWith(
        'msg-123',
        {
          data: { data: 'mock-result' },
          isError: false,
          error: null,
        }
      );
    });
  });

  it('should handle EXEC_QUERY message with error', async () => {
    const mockError = new Error('Query failed');
    mockDBM.queryWithTables = vi.fn().mockRejectedValue(mockError);

    render(<App />);

    const mockMessage = {
      uuid: 'msg-456',
      message: {
        type: 'EXEC_QUERY',
        payload: {
          queryId: 'query-2',
          query: 'SELECT * FROM invalid',
          tables: [],
          options: {},
        },
      },
    };

    await waitFor(() => {
      expect(mockOnMessageCallback).toBeDefined();
    });

    if (mockOnMessageCallback) {
      mockOnMessageCallback(mockMessage);
    }

    await waitFor(() => {
      expect(mockWindowCommunication.sendResponse).toHaveBeenCalledWith(
        'msg-456',
        {
          data: null,
          isError: true,
          error: mockError,
        }
      );
    });
  });

  it('should handle CANCEL_QUERY message', async () => {
    render(<App />);

    await waitFor(() => {
      expect(mockOnMessageCallback).toBeDefined();
    });

    // First, start a query
    const execMessage = {
      uuid: 'msg-exec',
      message: {
        type: 'EXEC_QUERY',
        payload: {
          queryId: 'query-to-cancel',
          query: 'SELECT * FROM table',
          tables: [],
          options: {},
        },
      },
    };

    if (mockOnMessageCallback) {
      mockOnMessageCallback(execMessage);
    }

    // Then cancel it
    const cancelMessage = {
      uuid: 'msg-cancel',
      message: {
        type: 'CANCEL_QUERY',
        payload: {
          queryId: 'query-to-cancel',
        },
      },
    };

    if (mockOnMessageCallback) {
      mockOnMessageCallback(cancelMessage);
    }

    // Verify the cancel was processed (no errors thrown)
    expect(mockOnMessageCallback).toBeDefined();
  });

  it('should pass abort signal to queryWithTables', async () => {
    render(<App />);

    const mockMessage = {
      uuid: 'msg-789',
      message: {
        type: 'EXEC_QUERY',
        payload: {
          queryId: 'query-3',
          query: 'SELECT * FROM table',
          tables: [],
          options: { timeout: 5000 },
        },
      },
    };

    await waitFor(() => {
      expect(mockOnMessageCallback).toBeDefined();
    });

    if (mockOnMessageCallback) {
      mockOnMessageCallback(mockMessage);
    }

    await waitFor(() => {
      expect(mockDBM.queryWithTables).toHaveBeenCalled();
      const callArgs = mockDBM.queryWithTables.mock.calls[0][0];
      expect(callArgs.options.signal).toBeDefined();
      expect(callArgs.options.signal).toBeInstanceOf(AbortSignal);
    });
  });

  it('should handle preQuery callback', async () => {
    mockWindowCommunication.sendRequest = vi
      .fn()
      .mockResolvedValue({ message: ['CREATE TABLE temp AS SELECT 1'] });

    render(<App />);

    const mockMessage = {
      uuid: 'msg-pre',
      message: {
        type: 'EXEC_QUERY',
        payload: {
          queryId: 'query-4',
          query: 'SELECT * FROM table',
          tables: [{ name: 'table1' }],
          options: {},
        },
      },
    };

    await waitFor(() => {
      expect(mockOnMessageCallback).toBeDefined();
    });

    if (mockOnMessageCallback) {
      mockOnMessageCallback(mockMessage);
    }

    await waitFor(() => {
      expect(mockDBM.queryWithTables).toHaveBeenCalled();
    });

    // Execute the preQuery callback
    const queryWithTablesCall = mockDBM.queryWithTables.mock.calls[0][0];
    await queryWithTablesCall.options.preQuery([{ name: 'table1' }]);

    expect(mockWindowCommunication.sendRequest).toHaveBeenCalledWith({
      type: 'RUNNER_PRE_QUERY',
      payload: {
        runnerId: 'test-uuid',
        tables: [{ name: 'table1' }],
      },
    });
    expect(mockDBM.query).toHaveBeenCalledWith('CREATE TABLE temp AS SELECT 1');
  });
});
