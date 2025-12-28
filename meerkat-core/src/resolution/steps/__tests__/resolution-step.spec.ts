import { TableSchema } from '../../../types/cube-types';
import { BASE_DATA_SOURCE_NAME, ResolutionConfig } from '../../types';
import { getResolvedTableSchema } from '../resolution-step';

describe('resolution-step', () => {
  describe('getResolvedTableSchema', () => {
    const createMockTableSchema = (
      name: string,
      dimensions: {
        name: string;
        sql?: string;
        alias?: string;
        type?: string;
      }[] = []
    ): TableSchema => ({
      name,
      sql: `SELECT * FROM ${name}`,
      dimensions: dimensions.map((d) => ({
        name: d.name,
        sql: d.sql || `${name}.${d.name}`,
        type: d.type || 'string',
        alias: d.alias,
      })),
      measures: [],
    });

    it('should call cubeQueryToSQL with query containing all dimensions namespaced', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM resolved');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders__customer_id', alias: 'Customer ID' },
        { name: 'orders__status', alias: 'Status' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };
      const contextParams = { userId: '123' };

      await getResolvedTableSchema({
        baseTableSchema,
        resolutionConfig,
        columnProjections: ['orders.customer_id', 'orders.status'],
        cubeQueryToSQL: mockCubeQueryToSQL,
        contextParams,
      });

      expect(mockCubeQueryToSQL).toHaveBeenCalledTimes(1);
      const calledParams = mockCubeQueryToSQL.mock.calls[0][0];

      expect(calledParams.contextParams).toEqual({ userId: '123' });
      expect(calledParams.query.measures).toEqual([]);
      expect(calledParams.query.dimensions).toEqual([
        `${BASE_DATA_SOURCE_NAME}.orders__customer_id`,
        `${BASE_DATA_SOURCE_NAME}.orders__status`,
      ]);
      expect(calledParams.tableSchemas).toHaveLength(1);
      expect(calledParams.tableSchemas[0].name).toBe(BASE_DATA_SOURCE_NAME);
    });

    it('should return table schema with dimensions from column projections', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT col1, col2 FROM resolved');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders__customer_id', alias: 'Customer ID', type: 'number' },
        { name: 'orders__status', alias: 'Status', type: 'string' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = await getResolvedTableSchema({
        baseTableSchema,
        resolutionConfig,
        columnProjections: ['orders.customer_id', 'orders.status'],
        cubeQueryToSQL: mockCubeQueryToSQL,
      });

      expect(result.name).toBe(BASE_DATA_SOURCE_NAME);
      expect(result.sql).toBe('SELECT col1, col2 FROM resolved');
      expect(result.dimensions).toHaveLength(2);
      // Dimensions are returned from baseTableSchema since no resolution configs
      expect(result.dimensions[0]).toEqual({
        name: 'orders__customer_id',
        sql: `${BASE_DATA_SOURCE_NAME}.orders__customer_id`,
        type: 'number',
        alias: 'Customer ID',
      });
      expect(result.dimensions[1]).toEqual({
        name: 'orders__status',
        sql: `${BASE_DATA_SOURCE_NAME}.orders__status`,
        type: 'string',
        alias: 'Status',
      });
    });

    it('should return empty dimensions when column projections is empty', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM resolved');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders__customer_id' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = await getResolvedTableSchema({
        baseTableSchema,
        resolutionConfig,
        columnProjections: [],
        cubeQueryToSQL: mockCubeQueryToSQL,
      });

      expect(result.dimensions).toEqual([]);
    });

    it('should throw error when column projection not found in base schema', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM resolved');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders__customer_id' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      await expect(
        getResolvedTableSchema({
          baseTableSchema,
          resolutionConfig,
          columnProjections: ['orders.nonexistent'],
          cubeQueryToSQL: mockCubeQueryToSQL,
        })
      ).rejects.toThrow(
        "Column projection 'orders__nonexistent' not found in base table schema dimensions"
      );
    });

    it('should generate resolution schemas and join paths when column configs provided', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM resolved');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders__owner_id', alias: 'Owner ID', type: 'number' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.owner_id',
            type: 'number',
            source: 'users',
            joinColumn: 'id',
            resolutionColumns: ['display_name'],
          },
        ],
        tableSchemas: [
          createMockTableSchema('users', [
            { name: 'display_name', alias: 'Display Name', type: 'string' },
          ]),
        ],
      };

      await getResolvedTableSchema({
        baseTableSchema,
        resolutionConfig,
        columnProjections: ['orders.owner_id'],
        cubeQueryToSQL: mockCubeQueryToSQL,
      });

      // Should have called cubeQueryToSQL with join paths
      const calledParams = mockCubeQueryToSQL.mock.calls[0][0];
      expect(calledParams.query.joinPaths).toBeDefined();
      expect(calledParams.query.joinPaths).toHaveLength(1);
      expect(calledParams.query.joinPaths[0][0]).toEqual({
        left: BASE_DATA_SOURCE_NAME,
        right: 'orders__owner_id',
        on: 'orders__owner_id', // Uses safe key when no alias found in baseTableSchemas
      });

      // Should have resolution schema in tableSchemas
      expect(calledParams.tableSchemas).toHaveLength(2);
      expect(calledParams.tableSchemas[0].name).toBe(BASE_DATA_SOURCE_NAME);
      expect(calledParams.tableSchemas[1].name).toBe('orders__owner_id');

      // Resolution schema should have correct dimension
      const resolutionSchema = calledParams.tableSchemas[1];
      expect(resolutionSchema.dimensions).toHaveLength(1);
      expect(resolutionSchema.dimensions[0].name).toBe(
        'orders__owner_id__display_name'
      );
      expect(resolutionSchema.dimensions[0].sql).toBe(
        'orders__owner_id.display_name'
      );
    });

    it('should maintain order of dimensions matching column projections', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM resolved');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders__status', alias: 'Status' },
        { name: 'orders__customer_id', alias: 'Customer ID' },
        { name: 'orders__amount', alias: 'Amount' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = await getResolvedTableSchema({
        baseTableSchema,
        resolutionConfig,
        columnProjections: [
          'orders.amount',
          'orders.customer_id',
          'orders.status',
        ],
        cubeQueryToSQL: mockCubeQueryToSQL,
      });

      expect(result.dimensions).toHaveLength(3);
      expect(result.dimensions[0].name).toBe('orders__amount');
      expect(result.dimensions[1].name).toBe('orders__customer_id');
      expect(result.dimensions[2].name).toBe('orders__status');
    });
  });
});
