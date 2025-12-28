import { TableSchema } from '../../../types/cube-types';
import { ResolutionConfig } from '../../types';
import { getUnnestTableSchema } from '../unnest-step';

describe('unnest-step', () => {
  describe('getUnnestTableSchema', () => {
    const mockCubeQueryToSQL = jest.fn().mockResolvedValue('SELECT * FROM unnested');

    const createMockTableSchema = (
      name: string,
      dimensions: { name: string; sql?: string; type?: string }[] = []
    ): TableSchema => ({
      name,
      sql: `SELECT * FROM ${name}`,
      dimensions: dimensions.map((d) => ({
        name: d.name,
        sql: d.sql || `${name}.${d.name}`,
        type: d.type || 'string',
      })),
      measures: [],
    });

    beforeEach(() => {
      jest.clearAllMocks();
    });

    it('should call cubeQueryToSQL with correct parameters', async () => {
      const baseTableSchema = createMockTableSchema('base', [
        { name: 'orders__customer_id' },
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

      expect(mockCubeQueryToSQL).toHaveBeenCalledWith(
        expect.objectContaining({
          contextParams,
        })
      );
    });

    it('should return wrapped table schema', async () => {
      const baseTableSchema = createMockTableSchema('base', [
        { name: 'orders__tags' },
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

      expect(result).toBeDefined();
      expect(result.name).toBeDefined();
    });

    it('should generate query with all dimensions', async () => {
      const baseTableSchema = createMockTableSchema('base', [
        { name: 'field1' },
        { name: 'field2' },
        { name: 'field3' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      await getUnnestTableSchema({
        baseTableSchema,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
      });

      const calledQuery = mockCubeQueryToSQL.mock.calls[0][0].query;
      expect(calledQuery.dimensions).toHaveLength(3);
      expect(calledQuery.measures).toEqual([]);
    });

    it('should handle empty dimensions array', async () => {
      const baseTableSchema = createMockTableSchema('base', []);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = await getUnnestTableSchema({
        baseTableSchema,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
      });

      expect(result).toBeDefined();
      const calledQuery = mockCubeQueryToSQL.mock.calls[0][0].query;
      expect(calledQuery.dimensions).toEqual([]);
    });

    it('should handle array type dimensions for unnesting', async () => {
      const baseTableSchema = createMockTableSchema('base', [
        { name: 'tags', type: 'string_array' },
        { name: 'customer_id', type: 'string' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'base.tags',
            type: 'string_array',
            source: 'tags_lookup',
            joinColumn: 'tag_id',
            resolutionColumns: ['tag_name'],
          },
        ],
        tableSchemas: [],
      };

      const result = await getUnnestTableSchema({
        baseTableSchema,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
      });

      expect(result).toBeDefined();
    });
  });
});
