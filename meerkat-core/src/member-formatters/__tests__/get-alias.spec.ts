import { TableSchema } from '../../types/cube-types';
import {
  constructAlias,
  constructAliasForAST,
  constructAliasForSQL,
  constructCompoundAlias,
  getAliasForAST,
  getAliasForSQL,
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

const defaultConfig = { useDotNotation: false };
const dotNotationConfig = { useDotNotation: true };

describe('get-alias', () => {
  describe('constructAlias', () => {
    describe('with shouldWrapAliasWithQuotes: true, useDotNotation: false', () => {
      it('should wrap custom alias in quotes', () => {
        const result = constructAlias({
          name: 'orders.total_amount',
          alias: 'Total Amount',
          shouldWrapAliasWithQuotes: true,
          config: defaultConfig,
        });
        expect(result).toBe('"Total Amount"');
      });

      it('should return safe key without quotes when no custom alias', () => {
        const result = constructAlias({
          name: 'orders.total_amount',
          shouldWrapAliasWithQuotes: true,
          config: defaultConfig,
        });
        expect(result).toBe('orders__total_amount');
      });

      it('should handle alias with special characters', () => {
        const result = constructAlias({
          name: 'orders.field',
          alias: 'Field.With.Dots',
          shouldWrapAliasWithQuotes: true,
          config: defaultConfig,
        });
        expect(result).toBe('"Field.With.Dots"');
      });
    });

    describe('with shouldWrapAliasWithQuotes: false, useDotNotation: false', () => {
      it('should return custom alias without quotes', () => {
        const result = constructAlias({
          name: 'orders.total_amount',
          alias: 'Total Amount',
          shouldWrapAliasWithQuotes: false,
          config: defaultConfig,
        });
        expect(result).toBe('Total Amount');
      });

      it('should return safe key when no custom alias', () => {
        const result = constructAlias({
          name: 'orders.total_amount',
          shouldWrapAliasWithQuotes: false,
          config: defaultConfig,
        });
        expect(result).toBe('orders__total_amount');
      });

      it('should handle alias with special characters without quoting', () => {
        const result = constructAlias({
          name: 'orders.field',
          alias: 'Field.With.Dots',
          shouldWrapAliasWithQuotes: false,
          config: defaultConfig,
        });
        expect(result).toBe('Field.With.Dots');
      });
    });

    describe('with shouldWrapAliasWithQuotes: true, useDotNotation: true', () => {
      it('should wrap custom alias in quotes', () => {
        const result = constructAlias({
          name: 'orders.total_amount',
          alias: 'Total Amount',
          shouldWrapAliasWithQuotes: true,
          config: dotNotationConfig,
        });
        expect(result).toBe('"Total Amount"');
      });

      it('should return quoted dot notation when no custom alias', () => {
        const result = constructAlias({
          name: 'orders.total_amount',
          shouldWrapAliasWithQuotes: true,
          config: dotNotationConfig,
        });
        expect(result).toBe('"orders.total_amount"');
      });
    });

    describe('with shouldWrapAliasWithQuotes: false, useDotNotation: true', () => {
      it('should return custom alias without quotes', () => {
        const result = constructAlias({
          name: 'orders.total_amount',
          alias: 'Total Amount',
          shouldWrapAliasWithQuotes: false,
          config: dotNotationConfig,
        });
        expect(result).toBe('Total Amount');
      });

      it('should return dot notation when no custom alias', () => {
        const result = constructAlias({
          name: 'orders.total_amount',
          shouldWrapAliasWithQuotes: false,
          config: dotNotationConfig,
        });
        expect(result).toBe('orders.total_amount');
      });
    });
  });

  describe('getAliasFromSchema', () => {
    describe('with shouldWrapAliasWithQuotes: true, useDotNotation: false', () => {
      it('should return safe key for dimension without custom alias', () => {
        const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
        const result = getAliasFromSchema({
          name: 'orders.customer_id',
          tableSchema,
          shouldWrapAliasWithQuotes: true,
          config: defaultConfig,
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
          config: defaultConfig,
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
          config: defaultConfig,
        });
        expect(result).toBe('"Total Amount"');
      });
    });

    describe('with shouldWrapAliasWithQuotes: false, useDotNotation: false', () => {
      it('should return safe key for dimension without custom alias', () => {
        const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
        const result = getAliasFromSchema({
          name: 'orders.customer_id',
          tableSchema,
          shouldWrapAliasWithQuotes: false,
          config: defaultConfig,
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
          config: defaultConfig,
        });
        expect(result).toBe('Customer ID');
      });

      it('should handle member not found in schema', () => {
        const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
        const result = getAliasFromSchema({
          name: 'orders.unknown_field',
          tableSchema,
          shouldWrapAliasWithQuotes: false,
          config: defaultConfig,
        });
        expect(result).toBe('orders__unknown_field');
      });
    });

    describe('with useDotNotation: true', () => {
      it('should return quoted dot notation for dimension without custom alias', () => {
        const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
        const result = getAliasFromSchema({
          name: 'orders.customer_id',
          tableSchema,
          shouldWrapAliasWithQuotes: true,
          config: dotNotationConfig,
        });
        expect(result).toBe('"orders.customer_id"');
      });

      it('should return dot notation without quotes when shouldWrapAliasWithQuotes is false', () => {
        const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
        const result = getAliasFromSchema({
          name: 'orders.customer_id',
          tableSchema,
          shouldWrapAliasWithQuotes: false,
          config: dotNotationConfig,
        });
        expect(result).toBe('orders.customer_id');
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

  // ============================================================================
  // NEW FLAG-AWARE API TESTS
  // ============================================================================

  describe('constructAliasForSQL', () => {
    describe('with useDotNotation: false', () => {
      it('should return safe key with underscores when no custom alias', () => {
        const result = constructAliasForSQL('orders.total_amount', undefined, {
          useDotNotation: false,
        });
        expect(result).toBe('orders__total_amount');
      });

      it('should wrap custom alias in quotes', () => {
        const result = constructAliasForSQL(
          'orders.total_amount',
          'Total Amount',
          { useDotNotation: false }
        );
        expect(result).toBe('"Total Amount"');
      });
    });

    describe('with useDotNotation: true', () => {
      it('should return quoted dot notation when no custom alias', () => {
        const result = constructAliasForSQL('orders.total_amount', undefined, {
          useDotNotation: true,
        });
        expect(result).toBe('"orders.total_amount"');
      });

      it('should wrap custom alias in quotes', () => {
        const result = constructAliasForSQL(
          'orders.total_amount',
          'Total Amount',
          { useDotNotation: true }
        );
        expect(result).toBe('"Total Amount"');
      });
    });
  });

  describe('constructAliasForAST', () => {
    describe('with useDotNotation: false', () => {
      it('should return safe key with underscores when no custom alias', () => {
        const result = constructAliasForAST('orders.total_amount', undefined, {
          useDotNotation: false,
        });
        expect(result).toBe('orders__total_amount');
      });

      it('should return custom alias without quotes', () => {
        const result = constructAliasForAST(
          'orders.total_amount',
          'Total Amount',
          { useDotNotation: false }
        );
        expect(result).toBe('Total Amount');
      });
    });

    describe('with useDotNotation: true', () => {
      it('should return dot notation when no custom alias', () => {
        const result = constructAliasForAST('orders.total_amount', undefined, {
          useDotNotation: true,
        });
        expect(result).toBe('orders.total_amount');
      });

      it('should return custom alias without quotes', () => {
        const result = constructAliasForAST(
          'orders.total_amount',
          'Total Amount',
          { useDotNotation: true }
        );
        expect(result).toBe('Total Amount');
      });
    });
  });

  describe('getAliasForSQL', () => {
    describe('with useDotNotation: false', () => {
      it('should return safe key with underscores', () => {
        const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
        const result = getAliasForSQL('orders.customer_id', tableSchema, {
          useDotNotation: false,
        });
        expect(result).toBe('orders__customer_id');
      });

      it('should wrap custom alias in quotes', () => {
        const tableSchema = createMockTableSchema([
          { name: 'customer_id', alias: 'Customer ID' },
        ]);
        const result = getAliasForSQL('orders.customer_id', tableSchema, {
          useDotNotation: false,
        });
        expect(result).toBe('"Customer ID"');
      });
    });

    describe('with useDotNotation: true', () => {
      it('should return quoted dot notation', () => {
        const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
        const result = getAliasForSQL('orders.customer_id', tableSchema, {
          useDotNotation: true,
        });
        expect(result).toBe('"orders.customer_id"');
      });

      it('should wrap custom alias in quotes', () => {
        const tableSchema = createMockTableSchema([
          { name: 'customer_id', alias: 'Customer ID' },
        ]);
        const result = getAliasForSQL('orders.customer_id', tableSchema, {
          useDotNotation: true,
        });
        expect(result).toBe('"Customer ID"');
      });
    });
  });

  describe('getAliasForAST', () => {
    describe('with useDotNotation: false', () => {
      it('should return safe key with underscores', () => {
        const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
        const result = getAliasForAST('orders.customer_id', tableSchema, {
          useDotNotation: false,
        });
        expect(result).toBe('orders__customer_id');
      });

      it('should return custom alias without quotes', () => {
        const tableSchema = createMockTableSchema([
          { name: 'customer_id', alias: 'Customer ID' },
        ]);
        const result = getAliasForAST('orders.customer_id', tableSchema, {
          useDotNotation: false,
        });
        expect(result).toBe('Customer ID');
      });
    });

    describe('with useDotNotation: true', () => {
      it('should return dot notation without quotes', () => {
        const tableSchema = createMockTableSchema([{ name: 'customer_id' }]);
        const result = getAliasForAST('orders.customer_id', tableSchema, {
          useDotNotation: true,
        });
        expect(result).toBe('orders.customer_id');
      });

      it('should return custom alias without quotes', () => {
        const tableSchema = createMockTableSchema([
          { name: 'customer_id', alias: 'Customer ID' },
        ]);
        const result = getAliasForAST('orders.customer_id', tableSchema, {
          useDotNotation: true,
        });
        expect(result).toBe('Customer ID');
      });
    });
  });
});
