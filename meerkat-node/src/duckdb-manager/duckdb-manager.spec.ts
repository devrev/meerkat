import { vi } from 'vitest';
import { DuckDBSingleton } from '../duckdb-singleton';
import { DuckDBManager } from './duckdb-manager';

vi.mock('../duckdb-singleton', () => ({
  DuckDBSingleton: {
    getInstance: vi.fn(),
  },
}));

describe('DuckDBManager', () => {
  let mockDb: any;
  let mockConnection: {
    close: any;
    prepare: any;
    run: any;
  };
  let mockStatement: {
    columns: any;
    all: any;
  };

  beforeEach(() => {
    vi.clearAllMocks();

    // Setup all mocks
    mockConnection = {
      close: vi.fn(),
      prepare: vi.fn(),
      run: vi.fn(),
    };

    mockStatement = {
      columns: vi.fn().mockReturnValue([
        { name: 'col1', type: { sql_type: 'text', name: 'text' } },
        { name: 'col2', type: { sql_type: 'int', name: 'int' } },
      ]),
      all: vi.fn(),
    };

    mockDb = {
      connect: vi.fn().mockResolvedValue(mockConnection),
    } as any;

    (DuckDBSingleton.getInstance as any).mockReturnValue(mockDb);
  });

  describe('initialization', () => {
    it('should initialize with onInitialize callback', async () => {
      const onInitialize = vi.fn();
      const manager = new DuckDBManager({ onInitialize });

      await manager['initPromise'];

      expect(DuckDBSingleton.getInstance).toHaveBeenCalled();

      // check if onInitialize is called with db instance
      expect(onInitialize).toHaveBeenCalledWith(mockDb);
    });
  });

  describe('getConnection', () => {
    it('should return a database connection', async () => {
      const manager = new DuckDBManager({});
      const connection = await manager.getConnection();

      expect(connection).toBe(mockConnection);
      expect(mockDb.connect).toHaveBeenCalled();
    });
  });

  describe('query', () => {
    it('should execute a query and return results', async () => {
      const manager = new DuckDBManager({});
      const testData = [
        { col1: 'value1', col2: 2 },
        { col1: 'value2', col2: 3 },
      ];

      mockConnection.prepare.mockImplementation(
        (
          query: string,
          callback: (error: Error | null, statement?: any) => void
        ) => {
          callback(null, mockStatement);
        }
      );

      mockStatement.all.mockImplementation((callback: any) => {
        callback(null, testData);
      });

      const result = await manager.query('SELECT * FROM test');

      expect(mockConnection.prepare).toHaveBeenCalledWith(
        'SELECT * FROM test',
        expect.any(Function)
      );
      expect(mockStatement.columns).toHaveBeenCalled();
      expect(mockStatement.all).toHaveBeenCalled();
      expect(result).toEqual({
        columns: [
          { name: 'col1', type: { sql_type: 'text', name: 'text' } },
          { name: 'col2', type: { sql_type: 'int', name: 'int' } },
        ],
        data: [
          { col1: 'value1', col2: 2 },
          { col1: 'value2', col2: 3 },
        ],
      });
    });

    it('should handle prepare errors', async () => {
      const manager = new DuckDBManager({});
      const error = new Error('Prepare error');

      mockConnection.prepare.mockImplementation((query: string, callback: any) => {
        callback(error);
      });

      await expect(manager.query('SELECT * FROM test')).rejects.toThrow(
        'Query preparation failed: Prepare error'
      );
    });

    it('should handle execution errors', async () => {
      const manager = new DuckDBManager({});
      const error = new Error('Execution error');

      mockConnection.prepare.mockImplementation((query: string, callback: any) => {
        callback(null, mockStatement);
      });

      mockStatement.all.mockImplementation((callback: any) => {
        callback(error);
      });

      await expect(manager.query('SELECT * FROM test')).rejects.toThrow(
        'Query execution failed: Execution error'
      );
    });
  });

  describe('close', () => {
    it('should close the connection', async () => {
      const manager = new DuckDBManager({});

      // Set the connection manually since we're testing private property
      (manager as any).connection = mockConnection;

      await manager.close();

      expect(mockConnection.close).toHaveBeenCalled();
      expect((manager as any).connection).toBeNull();
    });
  });
});
