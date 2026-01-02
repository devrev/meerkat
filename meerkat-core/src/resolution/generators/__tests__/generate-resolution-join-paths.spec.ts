import { TableSchema } from '../../../types/cube-types';
import { ResolutionConfig } from '../../types';
import { generateResolutionJoinPaths } from '../generate-resolution-join-paths';

const defaultConfig = { useDotNotation: false };

describe('generate-resolution-join-paths', () => {
  describe('generateResolutionJoinPaths', () => {
    const createMockTableSchema = (
      name: string,
      dimensions: { name: string; alias?: string }[] = []
    ): TableSchema => ({
      name,
      sql: `SELECT * FROM ${name}`,
      dimensions: dimensions.map((d) => ({
        name: d.name,
        sql: `${name}.${d.name}`,
        type: 'string',
        alias: d.alias,
      })),
      measures: [],
    });

    it('should generate join paths for single column config without alias', () => {
      const baseDataSourceName = 'base';
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.customer_id',
            joinColumn: 'id',
            schema: {
              name: 'customers',
              sql: 'SELECT * FROM customers',
              dimensions: [
                { name: 'name', sql: 'customers.name', type: 'string' },
              ],
              measures: [],
            },
          },
        ],
      };
      const baseTableSchemas: TableSchema[] = [
        createMockTableSchema('orders', [{ name: 'customer_id' }]),
      ];

      const result = generateResolutionJoinPaths(
        baseDataSourceName,
        resolutionConfig,
        baseTableSchemas,
        defaultConfig
      );

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual([
        {
          left: 'base',
          right: 'orders__customer_id',
          on: 'orders__customer_id',
        },
      ]);
    });

    it('should generate join paths for single column config with custom alias', () => {
      const baseDataSourceName = 'base';
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.customer_id',
            joinColumn: 'id',
            schema: {
              name: 'customers',
              sql: 'SELECT * FROM customers',
              dimensions: [
                { name: 'name', sql: 'customers.name', type: 'string' },
              ],
              measures: [],
            },
          },
        ],
      };
      const baseTableSchemas: TableSchema[] = [
        createMockTableSchema('orders', [
          { name: 'customer_id', alias: 'Customer ID' },
        ]),
      ];

      const result = generateResolutionJoinPaths(
        baseDataSourceName,
        resolutionConfig,
        baseTableSchemas,
        defaultConfig
      );

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual([
        {
          left: 'base',
          right: 'orders__customer_id',
          on: 'Customer ID', // Uses alias without quotes (internal schema reference)
        },
      ]);
    });

    it('should generate join paths for multiple column configs', () => {
      const baseDataSourceName = 'base';
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.customer_id',
            joinColumn: 'id',
            schema: {
              name: 'customers',
              sql: 'SELECT * FROM customers',
              dimensions: [],
              measures: [],
            },
          },
          {
            name: 'orders.product_id',
            joinColumn: 'id',
            schema: {
              name: 'products',
              sql: 'SELECT * FROM products',
              dimensions: [],
              measures: [],
            },
          },
        ],
      };
      const baseTableSchemas: TableSchema[] = [
        createMockTableSchema('orders', [
          { name: 'customer_id' },
          { name: 'product_id', alias: 'Product ID' },
        ]),
      ];

      const result = generateResolutionJoinPaths(
        baseDataSourceName,
        resolutionConfig,
        baseTableSchemas,
        defaultConfig
      );

      expect(result).toHaveLength(2);
      expect(result[0][0].on).toBe('orders__customer_id');
      expect(result[1][0].on).toBe('Product ID');
    });

    it('should return empty array when no column configs provided', () => {
      const baseDataSourceName = 'base';
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
      };
      const baseTableSchemas: TableSchema[] = [];

      const result = generateResolutionJoinPaths(
        baseDataSourceName,
        resolutionConfig,
        baseTableSchemas,
        defaultConfig
      );

      expect(result).toEqual([]);
    });

    it('should use memberKeyToSafeKey for right side of join', () => {
      const baseDataSourceName = 'base';
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.nested.field',
            joinColumn: 'id',
            schema: {
              name: 'nested',
              sql: 'SELECT * FROM nested',
              dimensions: [],
              measures: [],
            },
          },
        ],
      };
      const baseTableSchemas: TableSchema[] = [];

      const result = generateResolutionJoinPaths(
        baseDataSourceName,
        resolutionConfig,
        baseTableSchemas,
        defaultConfig
      );

      expect(result[0][0].right).toBe('orders__nested__field');
    });
  });
});
