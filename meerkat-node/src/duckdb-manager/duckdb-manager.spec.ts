import { DuckDBInstance } from '@duckdb/node-api';
import { DuckDBSingleton } from '../duckdb-singleton';
import { DuckDBManager } from './duckdb-manager';

// Mock DuckDB related modules
jest.mock('../duckdb-singleton', () => ({
  DuckDBSingleton: {
    getInstance: jest.fn(),
  },
}));
jest.mock('duckdb');

describe('DuckDBManager', () => {
  let dbmNode: DuckDBManager;
  let mockDb: jest.Mocked<DuckDBInstance>;
  let mockStatement: any;

  beforeEach(() => {
    // Setup mocks
    mockStatement = {
      columns: jest.fn().mockReturnValue([
        { name: 'id', type: { id: 'INTEGER' } },
        { name: 'name', type: { id: 'VARCHAR' } },
      ]),
      all: jest.fn(),
    };

    mockDb = {
      prepare: jest.fn(),
    } as unknown as jest.Mocked<DuckDBInstance>;

    (DuckDBSingleton.getInstance as jest.Mock).mockResolvedValue(mockDb);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('constructor', () => {
    it('should initialize with custom database initializer', async () => {
      const initializeDatabase = jest.fn();
      dbmNode = new DuckDBManager({ onInitialize: initializeDatabase });

      // Wait for initialization to complete
      await new Promise(process.nextTick);

      expect(DuckDBSingleton.getInstance).toHaveBeenCalled();
      expect(initializeDatabase).toHaveBeenCalledWith(mockDb);
    });

    it('should initialize without database initializer', async () => {
      dbmNode = new DuckDBManager({});

      // Wait for initialization to complete
      await new Promise(process.nextTick);

      expect(DuckDBSingleton.getInstance).toHaveBeenCalled();
    });
  });

  describe('query', () => {
    beforeEach(() => {
      dbmNode = new DuckDBManager({});
    });

    it('should execute a query and return results successfully', async () => {
      const mockData = [
        { id: 1, name: 'test' },
        { id: 2, name: 'test2' },
      ];

      mockDb.prepare.mockImplementation((query, callback) => {
        callback(null, mockStatement);
        mockStatement.all.mockImplementation((callback) => {
          callback(null, mockData);
        });
      });

      const result = await dbmNode.query('SELECT * FROM test');

      expect(mockDb.prepare).toHaveBeenCalledWith(
        'SELECT * FROM test',
        expect.any(Function)
      );
      expect(result).toEqual({
        columns: [
          { name: 'id', type: { id: 'INTEGER' } },
          { name: 'name', type: { id: 'VARCHAR' } },
        ],
        data: mockData,
      });
    });

    it('should throw error if database is not initialized', async () => {
      (DuckDBSingleton.getInstance as jest.Mock).mockResolvedValue(null);
      const dbManager = new DuckDBManager({});

      await expect(dbManager.query('SELECT * FROM test')).rejects.toThrow(
        'Database not initialized'
      );
    });

    it('should throw error if prepare fails', async () => {
      mockDb.prepare.mockImplementation((query, callback) => {
        callback(new Error('Prepare failed'), null);
      });

      await expect(dbmNode.query('SELECT * FROM test')).rejects.toThrow(
        'Query preparation failed: Prepare failed'
      );
    });

    it('should throw error if execution fails', async () => {
      mockDb.prepare.mockImplementation((query, callback) => {
        callback(null, mockStatement);
        mockStatement.all.mockImplementation((callback) => {
          callback(new Error('Execution failed'), null);
        });
      });

      await expect(dbmNode.query('SELECT * FROM test')).rejects.toThrow(
        'Query execution failed: Execution failed'
      );
    });
  });
});
