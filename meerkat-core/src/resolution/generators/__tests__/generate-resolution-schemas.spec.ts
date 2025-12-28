import { TableSchema } from '../../../types/cube-types';
import { ResolutionConfig } from '../../types';
import { generateResolutionSchemas } from '../generate-resolution-schemas';

describe('generate-resolution-schemas', () => {
  describe('generateResolutionSchemas', () => {
    const createMockTableSchema = (
      name: string,
      dimensions: { name: string; type?: string }[] = []
    ): TableSchema => ({
      name,
      sql: `SELECT * FROM ${name}`,
      dimensions: dimensions.map((d) => ({
        name: d.name,
        sql: `${name}.${d.name}`,
        type: d.type || 'string',
      })),
      measures: [],
    });

    it('should create resolution schema with converted name (dots to underscores)', () => {
      const config: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.customer_id',
            type: 'string',
            source: 'customers',
            joinColumn: 'id',
            resolutionColumns: ['display_name'],
          },
        ],
        tableSchemas: [
          createMockTableSchema('customers', [
            { name: 'display_name', type: 'string' },
          ]),
        ],
      };

      const result = generateResolutionSchemas(config);

      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('orders__customer_id');
    });

    it('should create dimensions with converted names and aliases', () => {
      const config: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.customer_id',
            type: 'string',
            source: 'customers',
            joinColumn: 'id',
            resolutionColumns: ['display_name', 'email'],
          },
        ],
        tableSchemas: [
          createMockTableSchema('customers', [
            { name: 'display_name', type: 'string' },
            { name: 'email', type: 'string' },
          ]),
        ],
      };

      const result = generateResolutionSchemas(config);

      expect(result[0].dimensions).toHaveLength(2);
      expect(result[0].dimensions[0].name).toBe(
        'orders__customer_id__display_name'
      );
      expect(result[0].dimensions[0].alias).toBe(
        'orders__customer_id__display_name'
      );
      expect(result[0].dimensions[1].name).toBe('orders__customer_id__email');
      expect(result[0].dimensions[1].alias).toBe('orders__customer_id__email');
    });

    it('should generate correct SQL references', () => {
      const config: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.customer_id',
            type: 'string',
            source: 'customers',
            joinColumn: 'id',
            resolutionColumns: ['display_name'],
          },
        ],
        tableSchemas: [
          createMockTableSchema('customers', [
            { name: 'display_name', type: 'string' },
          ]),
        ],
      };

      const result = generateResolutionSchemas(config);

      expect(result[0].dimensions[0].sql).toBe(
        'orders__customer_id.display_name'
      );
    });

    it('should handle multiple column configs', () => {
      const config: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.customer_id',
            type: 'string',
            source: 'customers',
            joinColumn: 'id',
            resolutionColumns: ['display_name'],
          },
          {
            name: 'orders.product_id',
            type: 'string',
            source: 'products',
            joinColumn: 'id',
            resolutionColumns: ['product_name'],
          },
        ],
        tableSchemas: [
          createMockTableSchema('customers', [
            { name: 'display_name', type: 'string' },
          ]),
          createMockTableSchema('products', [
            { name: 'product_name', type: 'string' },
          ]),
        ],
      };

      const result = generateResolutionSchemas(config);

      expect(result).toHaveLength(2);
      expect(result[0].name).toBe('orders__customer_id');
      expect(result[1].name).toBe('orders__product_id');
    });

    it('should handle nested dot notation in column names', () => {
      const config: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.customer.nested_id',
            type: 'string',
            source: 'customers',
            joinColumn: 'id',
            resolutionColumns: ['name'],
          },
        ],
        tableSchemas: [
          createMockTableSchema('customers', [{ name: 'name', type: 'string' }]),
        ],
      };

      const result = generateResolutionSchemas(config);

      expect(result[0].name).toBe('orders__customer__nested_id');
      expect(result[0].dimensions[0].name).toBe(
        'orders__customer__nested_id__name'
      );
    });

    describe('error handling', () => {
      it('should throw error when table schema not found', () => {
        const config: ResolutionConfig = {
          columnConfigs: [
            {
              name: 'orders.customer_id',
              type: 'string',
              source: 'nonexistent',
              joinColumn: 'id',
              resolutionColumns: ['display_name'],
            },
          ],
          tableSchemas: [],
        };

        expect(() => generateResolutionSchemas(config)).toThrow(
          'Table schema not found for nonexistent'
        );
      });

      it('should throw error when dimension not found in source schema', () => {
        const config: ResolutionConfig = {
          columnConfigs: [
            {
              name: 'orders.customer_id',
              type: 'string',
              source: 'customers',
              joinColumn: 'id',
              resolutionColumns: ['nonexistent_column'],
            },
          ],
          tableSchemas: [
            createMockTableSchema('customers', [
              { name: 'display_name', type: 'string' },
            ]),
          ],
        };

        expect(() => generateResolutionSchemas(config)).toThrow(
          'Dimension not found: nonexistent_column'
        );
      });
    });

    it('should return empty array when no column configs provided', () => {
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = generateResolutionSchemas(config);

      expect(result).toEqual([]);
    });

    it('should preserve dimension type from source schema', () => {
      const config: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.customer_id',
            type: 'string',
            source: 'customers',
            joinColumn: 'id',
            resolutionColumns: ['age'],
          },
        ],
        tableSchemas: [
          createMockTableSchema('customers', [{ name: 'age', type: 'number' }]),
        ],
      };

      const result = generateResolutionSchemas(config);

      expect(result[0].dimensions[0].type).toBe('number');
    });
  });
});
