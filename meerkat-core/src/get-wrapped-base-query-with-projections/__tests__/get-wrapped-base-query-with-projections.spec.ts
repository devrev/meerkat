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

      expect(result).toBe(
        'SELECT orders.customer_id AS orders__customer_id, * FROM (SELECT * FROM orders) AS orders'
      );
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

      expect(result).toBe(
        'SELECT orders.total AS orders__total, * FROM (SELECT * FROM orders) AS orders'
      );
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

      expect(result).toBe(
        'SELECT data.customer_nested_id AS data__customer_nested_id, * FROM (SELECT * FROM data) AS data'
      );
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

      expect(result).toBe(
        'SELECT orders.customer_id AS "Customer", * FROM (SELECT * FROM orders) AS orders'
      );
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

      expect(result).toBe(
        'SELECT orders.status AS orders__status, orders.customer_id AS orders__customer_id, * FROM (SELECT * FROM orders) AS orders'
      );
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

      expect(result).toBe('SELECT * FROM (SELECT * FROM orders) AS orders');
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

      expect(result).toBe(
        'SELECT orders.customer_id AS orders__customer_id, orders.total AS orders__total, * FROM (SELECT * FROM orders) AS orders'
      );
    });

    it('should not duplicate projection when filter member is already in dimensions', () => {
      const baseQuery = 'SELECT * FROM orders';
      const tableSchema = createMockTableSchema('orders', [
        { name: 'status', sql: 'orders.status' },
      ]);
      const query: Query = {
        measures: [],
        dimensions: ['orders.status'],
        filters: [
          { member: 'orders.status', operator: 'equals', values: ['active'] },
        ],
      };

      const result = getWrappedBaseQueryWithProjections({
        baseQuery,
        tableSchema,
        query,
      });

      expect(result).toBe(
        'SELECT orders.status AS orders__status, * FROM (SELECT * FROM orders) AS orders'
      );
    });

    it('should handle multiple dimensions in correct order', () => {
      const baseQuery = 'SELECT * FROM orders';
      const tableSchema = createMockTableSchema('orders', [
        { name: 'id', sql: 'orders.id' },
        { name: 'customer_id', sql: 'orders.customer_id' },
        { name: 'status', sql: 'orders.status' },
      ]);
      const query: Query = {
        measures: [],
        dimensions: ['orders.id', 'orders.customer_id', 'orders.status'],
      };

      const result = getWrappedBaseQueryWithProjections({
        baseQuery,
        tableSchema,
        query,
      });

      expect(result).toBe(
        'SELECT orders.id AS orders__id, orders.customer_id AS orders__customer_id, orders.status AS orders__status, * FROM (SELECT * FROM orders) AS orders'
      );
    });

    it('should handle complex base query with subqueries', () => {
      const baseQuery =
        'SELECT id, name FROM (SELECT * FROM users WHERE active = true) AS filtered_users';
      const tableSchema = createMockTableSchema('users', [
        { name: 'email', sql: 'users.email' },
      ]);
      const query: Query = {
        measures: [],
        dimensions: ['users.email'],
      };

      const result = getWrappedBaseQueryWithProjections({
        baseQuery,
        tableSchema,
        query,
      });

      expect(result).toBe(
        'SELECT users.email AS users__email, * FROM (SELECT id, name FROM (SELECT * FROM users WHERE active = true) AS filtered_users) AS users'
      );
    });

    it('should handle measure with custom alias using sql from schema', () => {
      const baseQuery = 'SELECT * FROM sales';
      const tableSchema = createMockTableSchema(
        'sales',
        [],
        [{ name: 'amount', sql: 'SUM(sales.amount)', alias: 'Total Revenue' }]
      );
      const query: Query = {
        measures: ['sales.amount'],
        dimensions: [],
      };

      const result = getWrappedBaseQueryWithProjections({
        baseQuery,
        tableSchema,
        query,
      });

      expect(result).toBe(
        'SELECT sales.amount AS "Total Revenue", * FROM (SELECT * FROM sales) AS sales'
      );
    });

    it('should handle nested filters with and/or operators', () => {
      const baseQuery = 'SELECT * FROM orders';
      const tableSchema = createMockTableSchema('orders', [
        { name: 'customer_id', sql: 'orders.customer_id' },
        { name: 'status', sql: 'orders.status' },
        { name: 'priority', sql: 'orders.priority' },
      ]);
      const query: Query = {
        measures: [],
        dimensions: ['orders.customer_id'],
        filters: [
          {
            and: [
              { member: 'orders.status', operator: 'equals', values: ['active'] },
              { member: 'orders.priority', operator: 'gt', values: ['5'] },
            ],
          },
        ],
      };

      const result = getWrappedBaseQueryWithProjections({
        baseQuery,
        tableSchema,
        query,
      });

      expect(result).toBe(
        'SELECT orders.status AS orders__status, orders.priority AS orders__priority, orders.customer_id AS orders__customer_id, * FROM (SELECT * FROM orders) AS orders'
      );
    });

    it('should handle filters with or operator', () => {
      const baseQuery = 'SELECT * FROM orders';
      const tableSchema = createMockTableSchema('orders', [
        { name: 'customer_id', sql: 'orders.customer_id' },
        { name: 'region', sql: 'orders.region' },
        { name: 'country', sql: 'orders.country' },
      ]);
      const query: Query = {
        measures: [],
        dimensions: ['orders.customer_id'],
        filters: [
          {
            or: [
              { member: 'orders.region', operator: 'equals', values: ['US'] },
              { member: 'orders.country', operator: 'equals', values: ['Canada'] },
            ],
          },
        ],
      };

      const result = getWrappedBaseQueryWithProjections({
        baseQuery,
        tableSchema,
        query,
      });

      expect(result).toBe(
        'SELECT orders.region AS orders__region, orders.country AS orders__country, orders.customer_id AS orders__customer_id, * FROM (SELECT * FROM orders) AS orders'
      );
    });

    it('should handle alias with special characters', () => {
      const baseQuery = 'SELECT * FROM orders';
      const tableSchema = createMockTableSchema('orders', [
        { name: 'customer_id', sql: 'orders.customer_id', alias: 'Customer (ID)' },
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

      expect(result).toBe(
        'SELECT orders.customer_id AS "Customer (ID)", * FROM (SELECT * FROM orders) AS orders'
      );
    });
  });
});
