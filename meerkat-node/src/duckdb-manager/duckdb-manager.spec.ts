import { Database, TableData } from 'duckdb';
import { DuckDBSingleton } from '../duckdb-singleton';
import { DuckDBManager } from './duckdb-manager';

jest.mock('../duckdb-singleton', () => ({
  DuckDBSingleton: {
    getInstance: jest.fn(),
  },
}));

describe('DuckDBManager', () => {
  let mockDb: jest.Mocked<Database>;
  let mockConnection: {
    close: jest.Mock;
    prepare: jest.Mock;
    run: jest.Mock;
  };
  let mockStatement: {
    columns: jest.Mock;
    all: jest.Mock;
  };

  beforeEach(() => {
    // Reset all mocks
    jest.clearAllMocks();

    // Setup mock connection
    mockConnection = {
      close: jest.fn(),
      prepare: jest.fn(),
      run: jest.fn(),
    };

    // Setup mock statement
    mockStatement = {
      columns: jest.fn().mockReturnValue(['col1', 'col2']),
      all: jest.fn(),
    };

    // Setup mock database
    mockDb = {
      connect: jest.fn().mockResolvedValue(mockConnection),
    } as any;

    // Mock getInstance to return our mockDb
    (DuckDBSingleton.getInstance as jest.Mock).mockReturnValue(mockDb);
  });

  describe('initialization', () => {
    it('should initialize with onInitialize callback', async () => {
      const onInitialize = jest.fn();
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
      const testData: TableData = [{ col1: 'value1', col2: 'value2' }];

      mockConnection.prepare.mockImplementation(
        (
          query: string,
          callback: (error: Error | null, statement?: any) => void
        ) => {
          callback(null, mockStatement);
        }
      );

      mockStatement.all.mockImplementation((callback) => {
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
        columns: ['col1', 'col2'],
        data: testData,
      });
    });

    it('should handle prepare errors', async () => {
      const manager = new DuckDBManager({});
      const error = new Error('Prepare error');

      mockConnection.prepare.mockImplementation((query: string, callback) => {
        callback(error);
      });

      await expect(manager.query('SELECT * FROM test')).rejects.toThrow(
        'Query preparation failed: Prepare error'
      );
    });

    it('should handle execution errors', async () => {
      const manager = new DuckDBManager({});
      const error = new Error('Execution error');

      mockConnection.prepare.mockImplementation((query: string, callback) => {
        callback(null, mockStatement);
      });

      mockStatement.all.mockImplementation((callback) => {
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
