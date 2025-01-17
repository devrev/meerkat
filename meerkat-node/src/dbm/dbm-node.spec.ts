import { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api';
import { DuckDBSingleton } from '../../duckdb-singleton';
import { DBMNode } from '../dbm-node';

// Mock DuckDB related modules
jest.mock('../../duckdb-singleton');
jest.mock('@duckdb/node-api');

describe('DBMNode', () => {
  let dbmNode: DBMNode;
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

  describe('constructor', () => {
    it('should initialize with custom database initializer', async () => {
      const initializeDatabase = jest.fn();
      dbmNode = new DBMNode({ initializeDatabase });

      // Wait for initialization to complete
      await new Promise(process.nextTick);

      expect(DuckDBSingleton.getInstance).toHaveBeenCalled();
      expect(initializeDatabase).toHaveBeenCalledWith(mockDb);
    });

    it('should initialize without database initializer', async () => {
      dbmNode = new DBMNode({});

      // Wait for initialization to complete
      await new Promise(process.nextTick);

      expect(DuckDBSingleton.getInstance).toHaveBeenCalled();
    });
  });

  describe('query', () => {
    beforeEach(() => {
      dbmNode = new DBMNode({});
    });

    it('should execute a query successfully', async () => {
      const mockResult = {
        columnTypes: jest.fn().mockReturnValue(['INTEGER', 'VARCHAR']),
      };
      mockConnection.run.mockResolvedValue(mockResult);

      const result = await dbmNode.query('SELECT * FROM test');

      expect(mockConnection.run).toHaveBeenCalledWith('SELECT * FROM test');
      expect(result).toBe(mockResult);
    });

    it('should throw error if database is not initialized', async () => {
      // Force connection to be null
      mockDb.connect.mockResolvedValue(null);

      await expect(dbmNode.query('SELECT * FROM test')).rejects.toThrow(
        'Database not initialized'
      );
    });
  });

  describe('close', () => {
    beforeEach(() => {
      dbmNode = new DBMNode({});
    });

    it('should close the connection', async () => {
      // First make a query to establish connection
      await dbmNode.query('SELECT 1');

      await dbmNode.close();
      expect(mockConnection.close).toHaveBeenCalled();
    });

    it('should handle closing when no connection exists', async () => {
      await dbmNode.close();
      expect(mockConnection.close).not.toHaveBeenCalled();
    });
  });
});
