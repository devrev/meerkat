import { TableSchema } from '../types/cube-types';
import {
  ExpressionClass,
  ExpressionType,
} from '../types/duckdb-serialization-types/serialization/Expression';
import { OrderType } from '../types/duckdb-serialization-types/serialization/Nodes';
import { ResultModifierType } from '../types/duckdb-serialization-types/serialization/ResultModifier';
import { cubeOrderByToAST } from './cube-order-by-transformer';

const defaultConfig = { useDotNotation: false };

describe('cube-order-by-transformer', () => {
  describe('cubeOrderByToAST', () => {
    const createMockTableSchema = (
      dimensions: { name: string; alias?: string }[] = [],
      measures: { name: string; alias?: string }[] = []
    ): TableSchema => ({
      name: 'orders',
      sql: 'SELECT * FROM orders',
      dimensions: dimensions.map((d) => ({
        name: d.name,
        sql: `orders.${d.name}`,
        type: 'string',
        alias: d.alias,
      })),
      measures: measures.map((m) => ({
        name: m.name,
        sql: `SUM(orders.${m.name})`,
        type: 'number',
        alias: m.alias,
      })),
    });

    it('should generate order by AST for ascending order without alias', () => {
      const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
      const result = cubeOrderByToAST(
        { 'orders.customer_id': 'asc' },
        tableSchema,
        defaultConfig
      );

      expect(result.type).toBe(ResultModifierType.ORDER_MODIFIER);
      expect(result.orders).toHaveLength(1);
      expect(result.orders[0]).toEqual({
        type: OrderType.ASCENDING,
        null_order: OrderType.ORDER_DEFAULT,
        expression: {
          class: ExpressionClass.COLUMN_REF,
          type: ExpressionType.COLUMN_REF,
          alias: '',
          column_names: ['orders__customer_id'],
        },
      });
    });

    it('should generate order by AST for descending order without alias', () => {
      const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
      const result = cubeOrderByToAST(
        { 'orders.customer_id': 'desc' },
        tableSchema,
        defaultConfig
      );

      expect(result.orders[0].type).toBe(OrderType.DESCENDING);
      expect(result.orders[0].expression.column_names).toEqual([
        'orders__customer_id',
      ]);
    });

    it('should generate order by AST with custom alias', () => {
      const tableSchema = createMockTableSchema([
        { name: 'customer_id', alias: 'Customer ID' },
      ]);
      const result = cubeOrderByToAST(
        { 'orders.customer_id': 'asc' },
        tableSchema,
        defaultConfig
      );

      // Should NOT have quotes - AST handles quoting automatically
      expect(result.orders[0].expression.column_names).toEqual(['Customer ID']);
    });

    it('should generate order by AST for multiple columns', () => {
      const tableSchema = createMockTableSchema(
        [{ name: 'customer_id' }],
        [{ name: 'total_amount', alias: 'Total Amount' }]
      );
      const result = cubeOrderByToAST(
        {
          'orders.customer_id': 'asc',
          'orders.total_amount': 'desc',
        },
        tableSchema,
        defaultConfig
      );

      expect(result.orders).toHaveLength(2);
      expect(result.orders[0].type).toBe(OrderType.ASCENDING);
      expect(result.orders[0].expression.column_names).toEqual([
        'orders__customer_id',
      ]);
      expect(result.orders[1].type).toBe(OrderType.DESCENDING);
      expect(result.orders[1].expression.column_names).toEqual([
        'Total Amount',
      ]);
    });

    it('should return empty orders array when no order provided', () => {
      const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
      const result = cubeOrderByToAST({}, tableSchema, defaultConfig);

      expect(result.type).toBe(ResultModifierType.ORDER_MODIFIER);
      expect(result.orders).toEqual([]);
    });

    it('should use unquoted alias for AST (DuckDB auto-quotes)', () => {
      const tableSchema = createMockTableSchema([
        { name: 'field', alias: 'Field.With.Dots' },
      ]);
      const result = cubeOrderByToAST({ 'orders.field': 'asc' }, tableSchema, defaultConfig);

      // Should NOT have quotes - AST handles quoting automatically
      expect(result.orders[0].expression.column_names).toEqual([
        'Field.With.Dots',
      ]);
    });
  });
});
