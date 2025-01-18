import {
  DuckDBConnection,
  DuckDBInstance,
  DuckDBResult,
} from '@duckdb/node-api';
import { DuckDBSingleton } from '../duckdb-singleton';
import { DuckDBManager } from './duckdb-manager';

// Mock DuckDB related modules
jest.mock('../duckdb-singleton', () => ({
  DuckDBSingleton: {
    getInstance: jest.fn(),
  },
}));
jest.mock('@duckdb/node-api');

describe('DuckDBManager', () => {
  let dbmNode: DuckDBManager;
  let mockDb: jest.Mocked<DuckDBInstance>;
  let mockConnection: jest.Mocked<DuckDBConnection>;

  beforeEach(() => {
    // Setup mocks
    mockConnection = {
      run: jest.fn(),
      close: jest.fn(),
    } as unknown as jest.Mocked<DuckDBConnection>;

    mockDb = {
      connect: jest.fn().mockResolvedValue(mockConnection),
    } as unknown as jest.Mocked<DuckDBInstance>;

    (DuckDBSingleton.getInstance as jest.Mock).mockResolvedValue(mockDb);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  const mockResult = {
    columnNames: jest.fn().mockReturnValue(['id', 'name']),
    columnTypes: jest.fn().mockReturnValue(['INTEGER', 'VARCHAR']),
    getRows: jest.fn().mockResolvedValue([
      [1, 'test'],
      [2, 'test2'],
    ]),
  };

  describe('constructor', () => {
    it('should initialize with custom database initializer', async () => {
      const initializeDatabase = jest.fn();
      dbmNode = new DuckDBManager({ initializeDatabase });

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

    it('should execute a query and transform the result successfully', async () => {
      mockConnection.run.mockResolvedValue(
        mockResult as unknown as DuckDBResult
      );

      const result = await dbmNode.query('SELECT * FROM test');

      expect(mockConnection.run).toHaveBeenCalledWith('SELECT * FROM test');
      expect(result).toEqual({
        data: [
          { id: 1, name: 'test' },
          { id: 2, name: 'test2' },
        ],
        schema: [
          { name: 'id', type: 'INTEGER' },
          { name: 'name', type: 'VARCHAR' },
        ],
      });
    });

    it('should throw error if connection is not initialized', async () => {
      // Force connection to be null
      mockDb.connect.mockResolvedValue(null);

      await expect(dbmNode.query('SELECT * FROM test')).rejects.toThrow(
        'DuckDB connection not initialized'
      );
    });
  });
});
