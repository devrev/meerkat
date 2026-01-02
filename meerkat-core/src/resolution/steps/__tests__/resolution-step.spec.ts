import { TableSchema } from '../../../types/cube-types';
import { BASE_DATA_SOURCE_NAME, ResolutionConfig } from '../../types';
import { getResolvedTableSchema } from '../resolution-step';

const defaultOptions = { useDotNotation: false };
const dotNotationOptions = { useDotNotation: true };

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
        config: defaultOptions,
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
        config: defaultOptions,
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
        config: defaultOptions,
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
          config: defaultOptions,
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
        config: defaultOptions,
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
        config: defaultOptions,
      });

      expect(result.dimensions).toHaveLength(3);
      expect(result.dimensions[0].name).toBe('orders__amount');
      expect(result.dimensions[1].name).toBe('orders__customer_id');
      expect(result.dimensions[2].name).toBe('orders__status');
    });

    it('should handle undefined context params', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM resolved');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders__customer_id', alias: 'Customer ID' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      await getResolvedTableSchema({
        baseTableSchema,
        resolutionConfig,
        columnProjections: ['orders.customer_id'],
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultOptions,
      });

      expect(mockCubeQueryToSQL).toHaveBeenCalledTimes(1);
      const calledParams = mockCubeQueryToSQL.mock.calls[0][0];
      expect(calledParams.contextParams).toBeUndefined();
    });

    it('should handle multiple resolution configs for different columns', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM resolved');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders__customer_id', alias: 'Customer ID', type: 'number' },
        { name: 'orders__product_id', alias: 'Product ID', type: 'number' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.customer_id',
            type: 'number',
            source: 'customers',
            joinColumn: 'id',
            resolutionColumns: ['name'],
          },
          {
            name: 'orders.product_id',
            type: 'number',
            source: 'products',
            joinColumn: 'id',
            resolutionColumns: ['title'],
          },
        ],
        tableSchemas: [
          createMockTableSchema('customers', [
            { name: 'name', alias: 'Name', type: 'string' },
          ]),
          createMockTableSchema('products', [
            { name: 'title', alias: 'Title', type: 'string' },
          ]),
        ],
      };

      await getResolvedTableSchema({
        baseTableSchema,
        resolutionConfig,
        columnProjections: ['orders.customer_id', 'orders.product_id'],
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultOptions,
      });

      const calledParams = mockCubeQueryToSQL.mock.calls[0][0];

      // Should have 3 table schemas: base + 2 resolution schemas
      expect(calledParams.tableSchemas).toHaveLength(3);
      expect(calledParams.tableSchemas[0].name).toBe(BASE_DATA_SOURCE_NAME);
      expect(calledParams.tableSchemas[1].name).toBe('orders__customer_id');
      expect(calledParams.tableSchemas[2].name).toBe('orders__product_id');

      // Should have 2 join paths
      expect(calledParams.query.joinPaths).toHaveLength(2);
    });

    it('should preserve dimension types from base schema', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM resolved');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders__amount', alias: 'Amount', type: 'number' },
        { name: 'orders__date', alias: 'Date', type: 'time' },
        { name: 'orders__tags', alias: 'Tags', type: 'string_array' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = await getResolvedTableSchema({
        baseTableSchema,
        resolutionConfig,
        columnProjections: ['orders.amount', 'orders.date', 'orders.tags'],
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultOptions,
      });

      expect(result.dimensions[0].type).toBe('number');
      expect(result.dimensions[1].type).toBe('time');
      expect(result.dimensions[2].type).toBe('string_array');
    });

    it('should use SQL from cubeQueryToSQL in result schema', async () => {
      const expectedSql = 'SELECT a, b, c FROM complex_query WHERE x > 5';
      const mockCubeQueryToSQL = jest.fn().mockResolvedValue(expectedSql);
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders__customer_id', alias: 'Customer ID' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = await getResolvedTableSchema({
        baseTableSchema,
        resolutionConfig,
        columnProjections: ['orders.customer_id'],
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultOptions,
      });

      expect(result.sql).toBe(expectedSql);
    });

    it('should generate resolved dimensions for resolution columns', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM resolved');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders__user_id', alias: 'User ID', type: 'number' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.user_id',
            type: 'number',
            source: 'users',
            joinColumn: 'id',
            resolutionColumns: ['first_name', 'last_name'],
          },
        ],
        tableSchemas: [
          createMockTableSchema('users', [
            { name: 'first_name', alias: 'First Name', type: 'string' },
            { name: 'last_name', alias: 'Last Name', type: 'string' },
          ]),
        ],
      };

      await getResolvedTableSchema({
        baseTableSchema,
        resolutionConfig,
        columnProjections: ['orders.user_id'],
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultOptions,
      });

      const calledParams = mockCubeQueryToSQL.mock.calls[0][0];
      const resolutionSchema = calledParams.tableSchemas[1];

      expect(resolutionSchema.dimensions).toHaveLength(2);
      expect(resolutionSchema.dimensions[0].name).toBe(
        'orders__user_id__first_name'
      );
      expect(resolutionSchema.dimensions[1].name).toBe(
        'orders__user_id__last_name'
      );
    });

    it('should handle nested dot notation in column projections', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM resolved');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders__customer__nested_id', alias: 'Nested ID' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = await getResolvedTableSchema({
        baseTableSchema,
        resolutionConfig,
        columnProjections: ['orders.customer.nested_id'],
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultOptions,
      });

      expect(result.dimensions).toHaveLength(1);
      expect(result.dimensions[0].name).toBe('orders__customer__nested_id');
    });

    it('should preserve measures from base table schema in result', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM resolved');
      const baseTableSchema: TableSchema = {
        name: BASE_DATA_SOURCE_NAME,
        sql: 'SELECT * FROM base',
        dimensions: [
          { name: 'orders__customer_id', sql: 'customer_id', type: 'string' },
        ],
        measures: [
          { name: 'orders__total', sql: 'SUM(total)', type: 'number' },
        ],
      };
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = await getResolvedTableSchema({
        baseTableSchema,
        resolutionConfig,
        columnProjections: ['orders.customer_id'],
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultOptions,
      });

      // Measures are preserved from the base table schema
      expect(result.measures).toHaveLength(1);
      expect(result.measures[0].name).toBe('orders__total');
    });
  });

  describe('getResolvedTableSchema (useDotNotation: true)', () => {
    // Note: The resolution pipeline internally uses underscore notation for lookups,
    // so base table dimension names must use underscore notation.
    // The config option affects the join paths 'on' field and 'right' field.
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

    it('should call cubeQueryToSQL with query containing all dimensions namespaced with dot notation config', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM resolved');
      // With useDotNotation: true, dimension names use dot notation
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders.customer_id', alias: 'Customer ID' },
        { name: 'orders.status', alias: 'Status' },
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
        config: dotNotationOptions,
      });

      expect(mockCubeQueryToSQL).toHaveBeenCalledTimes(1);
      const calledParams = mockCubeQueryToSQL.mock.calls[0][0];

      expect(calledParams.contextParams).toEqual({ userId: '123' });
      expect(calledParams.query.measures).toEqual([]);
      // With useDotNotation: true, dimensions use dot notation
      expect(calledParams.query.dimensions).toEqual([
        `${BASE_DATA_SOURCE_NAME}.orders.customer_id`,
        `${BASE_DATA_SOURCE_NAME}.orders.status`,
      ]);
      expect(calledParams.tableSchemas).toHaveLength(1);
      expect(calledParams.tableSchemas[0].name).toBe(BASE_DATA_SOURCE_NAME);
    });

    it('should return table schema with dimensions from column projections with dot notation config', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT col1, col2 FROM resolved');
      // With useDotNotation: true, dimension names use dot notation
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders.customer_id', alias: 'Customer ID', type: 'number' },
        { name: 'orders.status', alias: 'Status', type: 'string' },
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
        config: dotNotationOptions,
      });

      expect(result.name).toBe(BASE_DATA_SOURCE_NAME);
      expect(result.sql).toBe('SELECT col1, col2 FROM resolved');
      expect(result.dimensions).toHaveLength(2);
      // Dimensions are returned from baseTableSchema since no resolution configs
      expect(result.dimensions[0]).toEqual({
        name: 'orders.customer_id',
        sql: `${BASE_DATA_SOURCE_NAME}.orders.customer_id`,
        type: 'number',
        alias: 'Customer ID',
      });
      expect(result.dimensions[1]).toEqual({
        name: 'orders.status',
        sql: `${BASE_DATA_SOURCE_NAME}.orders.status`,
        type: 'string',
        alias: 'Status',
      });
    });

    it('should generate resolution schemas and join paths when column configs provided with dot notation config', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM resolved');
      // With useDotNotation: true, dimension names use dot notation
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders.owner_id', alias: 'Owner ID', type: 'number' },
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
        config: dotNotationOptions,
      });

      // Should have called cubeQueryToSQL with join paths
      const calledParams = mockCubeQueryToSQL.mock.calls[0][0];
      expect(calledParams.query.joinPaths).toBeDefined();
      expect(calledParams.query.joinPaths).toHaveLength(1);
      // With useDotNotation: true, join path fields use dot notation
      expect(calledParams.query.joinPaths[0][0]).toEqual({
        left: BASE_DATA_SOURCE_NAME,
        right: 'orders.owner_id',
        on: 'orders.owner_id',
      });

      // Should have resolution schema in tableSchemas
      expect(calledParams.tableSchemas).toHaveLength(2);
      expect(calledParams.tableSchemas[0].name).toBe(BASE_DATA_SOURCE_NAME);
      // With useDotNotation: true, resolution schema name uses dot notation
      expect(calledParams.tableSchemas[1].name).toBe('orders.owner_id');

      // Resolution schema should have correct dimension with dot notation
      const resolutionSchema = calledParams.tableSchemas[1];
      expect(resolutionSchema.dimensions).toHaveLength(1);
      expect(resolutionSchema.dimensions[0].name).toBe(
        'orders.owner_id.display_name'
      );
      expect(resolutionSchema.dimensions[0].sql).toBe(
        'orders.owner_id.display_name'
      );
    });
  });
});
