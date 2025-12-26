import { TableSchema } from '../types/cube-types';
import {
  ExpressionClass,
  ExpressionType,
} from '../types/duckdb-serialization-types/serialization/Expression';
import { cubeDimensionToGroupByAST } from './cube-group-by-transformer';

describe('cube-group-by-transformer', () => {
  describe('cubeDimensionToGroupByAST', () => {
    const createMockTableSchema = (
      dimensions: { name: string; alias?: string }[] = []
    ): TableSchema => ({
      name: 'orders',
      sql: 'SELECT * FROM orders',
      dimensions: dimensions.map((d) => ({
        name: d.name,
        sql: `orders.${d.name}`,
        type: 'string',
        alias: d.alias,
      })),
      measures: [],
    });

    it('should generate group by AST for single dimension without alias', () => {
      const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
      const result = cubeDimensionToGroupByAST(
        ['orders.customer_id'],
        tableSchema
      );

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({
        class: ExpressionClass.COLUMN_REF,
        type: ExpressionType.COLUMN_REF,
        alias: '',
        column_names: ['orders__customer_id'],
      });
    });

    it('should generate group by AST for single dimension with custom alias', () => {
      const tableSchema = createMockTableSchema([
        { name: 'customer_id', alias: 'Customer ID' },
      ]);
      const result = cubeDimensionToGroupByAST(
        ['orders.customer_id'],
        tableSchema
      );

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({
        class: ExpressionClass.COLUMN_REF,
        type: ExpressionType.COLUMN_REF,
        alias: '',
        column_names: ['Customer ID'],
      });
    });

    it('should generate group by AST for multiple dimensions', () => {
      const tableSchema = createMockTableSchema([
        { name: 'customer_id' },
        { name: 'order_date', alias: 'Order Date' },
      ]);
      const result = cubeDimensionToGroupByAST(
        ['orders.customer_id', 'orders.order_date'],
        tableSchema
      );

      expect(result).toHaveLength(2);
      expect(result[0].column_names).toEqual(['orders__customer_id']);
      expect(result[1].column_names).toEqual(['Order Date']);
    });

    it('should return empty array when no dimensions provided', () => {
      const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
      const result = cubeDimensionToGroupByAST([], tableSchema);

      expect(result).toEqual([]);
    });

    it('should use unquoted alias for AST (DuckDB auto-quotes)', () => {
      const tableSchema = createMockTableSchema([
        { name: 'field', alias: 'Field.With.Dots' },
      ]);
      const result = cubeDimensionToGroupByAST(['orders.field'], tableSchema);

      // Should NOT have quotes - AST handles quoting automatically
      expect(result[0].column_names).toEqual(['Field.With.Dots']);
    });
  });
});
