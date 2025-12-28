import { TableSchema } from '../../../types/cube-types';
import { BASE_DATA_SOURCE_NAME, ResolutionConfig } from '../../types';
import { getUnnestTableSchema } from '../unnest-step';

describe('unnest-step', () => {
  describe('getUnnestTableSchema', () => {
    const createMockTableSchema = (
      name: string,
      dimensions: {
        name: string;
        sql?: string;
        type?: string;
        alias?: string;
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

    it('should call cubeQueryToSQL with namespaced dimensions query', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM unnested');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders__customer_id', alias: 'Customer ID' },
        { name: 'orders__status', alias: 'Status' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };
      const contextParams = { userId: '123' };

      await getUnnestTableSchema({
        baseTableSchema,
        resolutionConfig,
        contextParams,
        cubeQueryToSQL: mockCubeQueryToSQL,
      });

      expect(mockCubeQueryToSQL).toHaveBeenCalledTimes(1);
      const calledParams = mockCubeQueryToSQL.mock.calls[0][0];

      expect(calledParams.contextParams).toEqual({ userId: '123' });
      expect(calledParams.query).toEqual({
        measures: [],
        dimensions: [
          `${BASE_DATA_SOURCE_NAME}.orders__customer_id`,
          `${BASE_DATA_SOURCE_NAME}.orders__status`,
        ],
      });
      expect(calledParams.tableSchemas).toHaveLength(1);
      expect(calledParams.tableSchemas[0].name).toBe(BASE_DATA_SOURCE_NAME);
    });

    it('should return wrapped table schema with correct structure', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT col1, col2 FROM unnested');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders__tags', alias: 'Tags', type: 'string_array' },
        { name: 'orders__customer_id', alias: 'Customer ID', type: 'number' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = await getUnnestTableSchema({
        baseTableSchema,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
      });

      expect(result.name).toBe(BASE_DATA_SOURCE_NAME);
      expect(result.sql).toBe('SELECT col1, col2 FROM unnested');
      expect(result.dimensions).toHaveLength(2);
      expect(result.dimensions[0]).toEqual({
        name: 'orders__tags',
        sql: `${BASE_DATA_SOURCE_NAME}."Tags"`,
        type: 'string_array',
        alias: 'Tags',
      });
      expect(result.dimensions[1]).toEqual({
        name: 'orders__customer_id',
        sql: `${BASE_DATA_SOURCE_NAME}."Customer ID"`,
        type: 'number',
        alias: 'Customer ID',
      });
      expect(result.measures).toEqual([]);
    });

    it('should add flatten modifier to array type dimensions when in column configs', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM unnested');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'orders__tags', alias: 'Tags', type: 'string_array' },
        { name: 'orders__customer_id', alias: 'Customer ID', type: 'number' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders__tags',
            type: 'string_array',
            source: 'tags_lookup',
            joinColumn: 'tag_id',
            resolutionColumns: ['tag_name'],
          },
        ],
        tableSchemas: [],
      };

      await getUnnestTableSchema({
        baseTableSchema,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
      });

      const calledParams = mockCubeQueryToSQL.mock.calls[0][0];
      const passedTableSchema = calledParams.tableSchemas[0];

      // The array dimension should have the flatten modifier
      const tagsDimension = passedTableSchema.dimensions.find(
        (d: { name: string }) => d.name === 'orders__tags'
      );
      expect(tagsDimension.modifier).toEqual({ shouldFlattenArray: true });

      // Non-array dimension should not have modifier
      const customerDimension = passedTableSchema.dimensions.find(
        (d: { name: string }) => d.name === 'orders__customer_id'
      );
      expect(customerDimension.modifier).toBeUndefined();
    });

    it('should handle empty dimensions array', async () => {
      const mockCubeQueryToSQL = jest.fn().mockResolvedValue('SELECT 1');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, []);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = await getUnnestTableSchema({
        baseTableSchema,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
      });

      expect(result.dimensions).toEqual([]);
      const calledParams = mockCubeQueryToSQL.mock.calls[0][0];
      expect(calledParams.query.dimensions).toEqual([]);
    });

    it('should preserve dimension aliases in returned schema', async () => {
      const mockCubeQueryToSQL = jest
        .fn()
        .mockResolvedValue('SELECT * FROM unnested');
      const baseTableSchema = createMockTableSchema(BASE_DATA_SOURCE_NAME, [
        { name: 'field_without_alias', type: 'string' },
        { name: 'field_with_alias', alias: 'Custom Alias', type: 'number' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = await getUnnestTableSchema({
        baseTableSchema,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
      });

      expect(result.dimensions[0]).toEqual({
        name: 'field_without_alias',
        sql: `${BASE_DATA_SOURCE_NAME}."field_without_alias"`,
        type: 'string',
        alias: undefined,
      });
      expect(result.dimensions[1]).toEqual({
        name: 'field_with_alias',
        sql: `${BASE_DATA_SOURCE_NAME}."Custom Alias"`,
        type: 'number',
        alias: 'Custom Alias',
      });
    });
  });
});
