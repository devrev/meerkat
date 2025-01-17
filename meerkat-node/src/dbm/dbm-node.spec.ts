import { DuckDBSingleton } from '../duckdb-singleton';
import { DBMNode } from './dbm-node';
jest.mock('../../duckdb-singleton');

export const mockDB = {
  registerFileBuffer: async (name: string, buffer: Uint8Array) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve(buffer);
      }, 200);
    });
  },
  connect: async () => {
    return;
  },
};

describe('DBMNode', () => {
  let dbmNode: DBMNode;

  beforeEach(() => {
    jest.clearAllMocks();
    dbmNode = new DBMNode();
  });

  afterEach(async () => {
    await dbmNode.close();
  });

  describe('initialization', () => {
    it('should initialize successfully', async () => {
      const mockDb = {
        connect: jest.fn(),
        prepare: jest.fn(),
      };

      (DuckDBSingleton.getInstance as jest.Mock).mockResolvedValue(mockDb);

      const instance = new DBMNode();
      // Wait for initialization to complete
      await instance.query('SELECT 1');

      expect(DuckDBSingleton.getInstance).toHaveBeenCalled();
      expect(mockDb.connect).toHaveBeenCalled();
    });
  });

  describe('query', () => {
    it('should execute a query successfully', async () => {
      const mockColumns = [{ name: 'id', type: 'INTEGER' }];
      const mockData = [{ id: 1 }];
      const mockStatement = {
        columns: jest.fn().mockReturnValue(mockColumns),
        all: jest
          .fn()
          .mockImplementation((callback) => callback(null, mockData)),
        finalize: jest.fn().mockImplementation((callback) => callback(null)),
      };

      const mockDb = {
        connect: jest.fn(),
        prepare: jest
          .fn()
          .mockImplementation((query, callback) =>
            callback(null, mockStatement)
          ),
      };

      (DuckDBSingleton.getInstance as jest.Mock).mockResolvedValue(mockDb);

      const result = await dbmNode.query('SELECT * FROM test');

      expect(result).toEqual({
        columns: mockColumns,
        data: mockData,
      });
      expect(mockDb.prepare).toHaveBeenCalledWith(
        'SELECT * FROM test',
        expect.any(Function)
      );
    });

    it('should handle query preparation errors', async () => {
      const mockDb = {
        connect: jest.fn(),
        prepare: jest
          .fn()
          .mockImplementation((query, callback) =>
            callback(new Error('Preparation failed'))
          ),
      };

      (DuckDBSingleton.getInstance as jest.Mock).mockResolvedValue(mockDb);

      await expect(dbmNode.query('INVALID SQL')).rejects.toThrow(
        'Query preparation failed: Preparation failed'
      );
    });
  });

  describe('close', () => {
    it('should close connections properly', async () => {
      const mockConnection = {
        close: jest.fn(),
      };

      const mockDb = {
        connect: jest.fn().mockReturnValue(mockConnection),
        close: jest.fn(),
      };

      (DuckDBSingleton.getInstance as jest.Mock).mockResolvedValue(mockDb);

      const instance = new DBMNode();
      await instance.close();

      expect(mockConnection.close).toHaveBeenCalled();
      expect(mockDb.close).toHaveBeenCalled();
    });
  });
});
