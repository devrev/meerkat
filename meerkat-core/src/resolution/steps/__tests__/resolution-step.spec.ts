import { TableSchema } from '../../../types/cube-types';
import { ResolutionConfig } from '../../types';
import { getResolvedTableSchema } from '../resolution-step';

describe('resolution-step', () => {
  describe('getResolvedTableSchema', () => {
    const mockCubeQueryToSQL = jest.fn().mockResolvedValue('SELECT * FROM resolved');

    const createMockTableSchema = (
      name: string,
      dimensions: { name: string; sql?: string; alias?: string }[] = []
    ): TableSchema => ({
      name,
      sql: `SELECT * FROM ${name}`,
      dimensions: dimensions.map((d) => ({
        name: d.name,
        sql: d.sql || `${name}.${d.name}`,
        type: 'string',
        alias: d.alias,
      })),
      measures: [],
    });

    beforeEach(() => {
      jest.clearAllMocks();
    });

    it('should call cubeQueryToSQL with correct parameters', async () => {
      const baseTableSchema = createMockTableSchema('base', [
        { name: 'customer_id' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };
      const contextParams = { userId: '123' };

      await getResolvedTableSchema({
        baseTableSchema,
        resolutionConfig,
        columnProjections: [],
        cubeQueryToSQL: mockCubeQueryToSQL,
        contextParams,
      });

      expect(mockCubeQueryToSQL).toHaveBeenCalledWith(
        expect.objectContaining({
          contextParams,
        })
      );
    });

    it('should return a table schema with resolved dimensions', async () => {
      const baseTableSchema = createMockTableSchema('base', [
        { name: 'base__customer_id', alias: 'Customer ID' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'base.customer_id',
            type: 'string',
            source: 'customers',
            joinColumn: 'id',
            resolutionColumns: ['display_name'],
          },
        ],
        tableSchemas: [
          createMockTableSchema('customers', [
            { name: 'display_name', alias: 'Display Name' },
          ]),
        ],
      };

      const result = await getResolvedTableSchema({
        baseTableSchema,
        resolutionConfig,
        columnProjections: ['base.customer_id'],
        cubeQueryToSQL: mockCubeQueryToSQL,
      });

      expect(result).toBeDefined();
      expect(result.name).toBeDefined();
    });

    it('should handle empty column projections', async () => {
      const baseTableSchema = createMockTableSchema('base', [
        { name: 'customer_id' },
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

      expect(result).toBeDefined();
    });

    it('should handle resolution with no column configs', async () => {
      const baseTableSchema = createMockTableSchema('base', [
        { name: 'base__customer_id' },
        { name: 'base__status' },
      ]);
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = await getResolvedTableSchema({
        baseTableSchema,
        resolutionConfig,
        columnProjections: ['base.customer_id', 'base.status'],
        cubeQueryToSQL: mockCubeQueryToSQL,
      });

      expect(result).toBeDefined();
      expect(mockCubeQueryToSQL).toHaveBeenCalled();
    });
  });
});
