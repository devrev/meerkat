import { TableSchema } from '../../types/cube-types';
import {
  constructAlias,
  constructCompoundAlias,
  getAliasFromSchema,
} from '../get-alias';

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

describe('get-alias', () => {
  describe('constructAlias', () => {
    describe('with shouldWrapAliasWithQuotes: true', () => {
      it('should wrap custom alias in quotes', () => {
        const result = constructAlias({
          name: 'orders.total_amount',
          alias: 'Total Amount',
          shouldWrapAliasWithQuotes: true,
        });
        expect(result).toBe('"Total Amount"');
      });

      it('should return safe key without quotes when no custom alias', () => {
        const result = constructAlias({
          name: 'orders.total_amount',
          shouldWrapAliasWithQuotes: true,
        });
        expect(result).toBe('orders__total_amount');
      });

      it('should handle alias with special characters', () => {
        const result = constructAlias({
          name: 'orders.field',
          alias: 'Field.With.Dots',
          shouldWrapAliasWithQuotes: true,
        });
        expect(result).toBe('"Field.With.Dots"');
      });
    });

    describe('with shouldWrapAliasWithQuotes: false', () => {
      it('should return custom alias without quotes', () => {
        const result = constructAlias({
          name: 'orders.total_amount',
          alias: 'Total Amount',
          shouldWrapAliasWithQuotes: false,
        });
        expect(result).toBe('Total Amount');
      });

      it('should return safe key when no custom alias', () => {
        const result = constructAlias({
          name: 'orders.total_amount',
          shouldWrapAliasWithQuotes: false,
        });
        expect(result).toBe('orders__total_amount');
      });

      it('should handle alias with special characters without quoting', () => {
        const result = constructAlias({
          name: 'orders.field',
          alias: 'Field.With.Dots',
          shouldWrapAliasWithQuotes: false,
        });
        expect(result).toBe('Field.With.Dots');
      });
    });
  });

  describe('getAliasFromSchema', () => {
    describe('with shouldWrapAliasWithQuotes: true', () => {
      it('should return safe key for dimension without custom alias', () => {
        const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
        const result = getAliasFromSchema({
          name: 'orders.customer_id',
          tableSchema,
          shouldWrapAliasWithQuotes: true,
        });
        expect(result).toBe('orders__customer_id');
      });

      it('should wrap custom alias from schema in quotes', () => {
        const tableSchema = createMockTableSchema([
          { name: 'customer_id', alias: 'Customer ID' },
        ]);
        const result = getAliasFromSchema({
          name: 'orders.customer_id',
          tableSchema,
          shouldWrapAliasWithQuotes: true,
        });
        expect(result).toBe('"Customer ID"');
      });

      it('should handle measure with custom alias', () => {
        const tableSchema = createMockTableSchema(
          [],
          [{ name: 'total_amount', alias: 'Total Amount' }]
        );
        const result = getAliasFromSchema({
          name: 'orders.total_amount',
          tableSchema,
          shouldWrapAliasWithQuotes: true,
        });
        expect(result).toBe('"Total Amount"');
      });
    });

    describe('with shouldWrapAliasWithQuotes: false', () => {
      it('should return safe key for dimension without custom alias', () => {
        const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
        const result = getAliasFromSchema({
          name: 'orders.customer_id',
          tableSchema,
          shouldWrapAliasWithQuotes: false,
        });
        expect(result).toBe('orders__customer_id');
      });

      it('should return custom alias from schema without quotes', () => {
        const tableSchema = createMockTableSchema([
          { name: 'customer_id', alias: 'Customer ID' },
        ]);
        const result = getAliasFromSchema({
          name: 'orders.customer_id',
          tableSchema,
          shouldWrapAliasWithQuotes: false,
        });
        expect(result).toBe('Customer ID');
      });

      it('should handle member not found in schema', () => {
        const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
        const result = getAliasFromSchema({
          name: 'orders.unknown_field',
          tableSchema,
          shouldWrapAliasWithQuotes: false,
        });
        expect(result).toBe('orders__unknown_field');
      });
    });
  });

  describe('constructCompoundAlias', () => {
    it('should join two aliases with " - "', () => {
      const result = constructCompoundAlias('Owners', 'Display Name');
      expect(result).toBe('Owners - Display Name');
    });

    it('should handle aliases with special characters', () => {
      const result = constructCompoundAlias('Field.With.Dots', 'Another.Field');
      expect(result).toBe('Field.With.Dots - Another.Field');
    });

    it('should handle empty base alias', () => {
      const result = constructCompoundAlias('', 'Display Name');
      expect(result).toBe(' - Display Name');
    });

    it('should handle empty resolution alias', () => {
      const result = constructCompoundAlias('Owners', '');
      expect(result).toBe('Owners - ');
    });

    it('should handle aliases with spaces', () => {
      const result = constructCompoundAlias('First Name', 'Last Name');
      expect(result).toBe('First Name - Last Name');
    });
  });
});
