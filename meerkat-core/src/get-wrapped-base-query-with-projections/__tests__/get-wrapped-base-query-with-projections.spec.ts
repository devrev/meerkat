import { Query, TableSchema } from '../../types/cube-types';
import { getWrappedBaseQueryWithProjections } from '../get-wrapped-base-query-with-projections';

describe('get-wrapped-base-query-with-projections', () => {
  describe('getWrappedBaseQueryWithProjections', () => {
    const createMockTableSchema = (
      name: string,
      dimensions: { name: string; sql: string; alias?: string }[] = [],
      measures: { name: string; sql: string; alias?: string }[] = []
    ): TableSchema => ({
      name,
      sql: `SELECT * FROM ${name}`,
      dimensions: dimensions.map((d) => ({
        name: d.name,
        sql: d.sql,
        type: 'string',
        alias: d.alias,
      })),
      measures: measures.map((m) => ({
        name: m.name,
        sql: m.sql,
        type: 'number',
        alias: m.alias,
      })),
    });

    it('should wrap base query with projections using underscores', () => {
      const baseQuery = 'SELECT * FROM orders';
      const tableSchema = createMockTableSchema('orders', [
        { name: 'customer_id', sql: 'orders.customer_id' },
      ]);
      const query: Query = {
        measures: [],
        dimensions: ['orders.customer_id'],
      };

      const result = getWrappedBaseQueryWithProjections({
        baseQuery,
        tableSchema,
        query,
      });

      expect(result).toContain('SELECT');
      expect(result).toContain('orders__customer_id');
      expect(result).toContain('FROM (SELECT * FROM orders) AS orders');
    });

    it('should include measures in projections with underscores', () => {
      const baseQuery = 'SELECT * FROM orders';
      const tableSchema = createMockTableSchema(
        'orders',
        [],
        [{ name: 'total', sql: 'SUM(orders.total)' }]
      );
      const query: Query = {
        measures: ['orders.total'],
        dimensions: [],
      };

      const result = getWrappedBaseQueryWithProjections({
        baseQuery,
        tableSchema,
        query,
      });

      expect(result).toContain('orders__total');
    });

    it('should handle dimension with underscore in name', () => {
      const baseQuery = 'SELECT * FROM data';
      const tableSchema = createMockTableSchema('data', [
        { name: 'customer_nested_id', sql: 'data.customer_nested_id' },
      ]);
      const query: Query = {
        measures: [],
        dimensions: ['data.customer_nested_id'],
      };

      const result = getWrappedBaseQueryWithProjections({
        baseQuery,
        tableSchema,
        query,
      });

      expect(result).toContain('data__customer_nested_id');
    });

    it('should use custom alias when provided', () => {
      const baseQuery = 'SELECT * FROM orders';
      const tableSchema = createMockTableSchema('orders', [
        { name: 'customer_id', sql: 'orders.customer_id', alias: 'Customer' },
      ]);
      const query: Query = {
        measures: [],
        dimensions: ['orders.customer_id'],
      };

      const result = getWrappedBaseQueryWithProjections({
        baseQuery,
        tableSchema,
        query,
      });

      expect(result).toContain('"Customer"');
    });

    it('should handle filters and add projections for filter members', () => {
      const baseQuery = 'SELECT * FROM orders';
      const tableSchema = createMockTableSchema('orders', [
        { name: 'customer_id', sql: 'orders.customer_id' },
        { name: 'status', sql: 'orders.status' },
      ]);
      const query: Query = {
        measures: [],
        dimensions: ['orders.customer_id'],
        filters: [
          { member: 'orders.status', operator: 'equals', values: ['active'] },
        ],
      };

      const result = getWrappedBaseQueryWithProjections({
        baseQuery,
        tableSchema,
        query,
      });

      expect(result).toContain('orders__customer_id');
    });

    it('should always include * in final select', () => {
      const baseQuery = 'SELECT * FROM orders';
      const tableSchema = createMockTableSchema('orders', []);
      const query: Query = {
        measures: [],
        dimensions: [],
      };

      const result = getWrappedBaseQueryWithProjections({
        baseQuery,
        tableSchema,
        query,
      });

      expect(result).toContain('*');
    });

    it('should handle empty dimensions and measures', () => {
      const baseQuery = 'SELECT * FROM orders';
      const tableSchema = createMockTableSchema('orders', []);
      const query: Query = {
        measures: [],
        dimensions: [],
      };

      const result = getWrappedBaseQueryWithProjections({
        baseQuery,
        tableSchema,
        query,
      });

      expect(result).toContain('SELECT');
      expect(result).toContain('*');
    });

    it('should handle both dimensions and measures together', () => {
      const baseQuery = 'SELECT * FROM orders';
      const tableSchema = createMockTableSchema(
        'orders',
        [{ name: 'customer_id', sql: 'orders.customer_id' }],
        [{ name: 'total', sql: 'SUM(orders.total)' }]
      );
      const query: Query = {
        measures: ['orders.total'],
        dimensions: ['orders.customer_id'],
      };

      const result = getWrappedBaseQueryWithProjections({
        baseQuery,
        tableSchema,
        query,
      });

      expect(result).toContain('orders__customer_id');
      expect(result).toContain('orders__total');
    });
  });
});
