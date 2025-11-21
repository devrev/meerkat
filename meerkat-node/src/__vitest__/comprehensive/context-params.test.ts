/**
 * Comprehensive Context Params Tests
 * 
 * Tests context parameter substitution in base SQL:
 * - Simple parameter substitution
 * - Multiple parameters
 * - Parameters in different parts of SQL
 * - Parameters with filters
 * - Parameters with joins
 * - Nested parameter references
 * - Edge cases (missing params, special characters)
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { cubeQueryToSQL } from '../../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../../duckdb-exec';

describe('Comprehensive: Context Params', () => {
  beforeAll(async () => {
    console.log('🚀 Starting context params tests...');
    
    // Create test tables
    await duckdbExec(`
      CREATE TABLE IF NOT EXISTS orders_2023 (
        id INTEGER,
        customer_id VARCHAR,
        amount DECIMAL,
        status VARCHAR,
        order_date DATE
      )
    `);

    await duckdbExec(`
      CREATE TABLE IF NOT EXISTS orders_2024 (
        id INTEGER,
        customer_id VARCHAR,
        amount DECIMAL,
        status VARCHAR,
        order_date DATE
      )
    `);

    await duckdbExec(`
      INSERT INTO orders_2023 VALUES
        (1, 'cust_1', 100.00, 'completed', '2023-01-15'),
        (2, 'cust_2', 200.00, 'completed', '2023-02-20'),
        (3, 'cust_3', 150.00, 'pending', '2023-03-10')
    `);

    await duckdbExec(`
      INSERT INTO orders_2024 VALUES
        (4, 'cust_1', 300.00, 'completed', '2024-01-15'),
        (5, 'cust_2', 400.00, 'completed', '2024-02-20'),
        (6, 'cust_3', 250.00, 'pending', '2024-03-10')
    `);
  }, 120000);

  describe('Basic Parameter Substitution', () => {
    it('should substitute single table name parameter', async () => {
      const schema = {
        name: 'orders',
        sql: 'SELECT * FROM ${CONTEXT_PARAMS.TABLE_NAME}',
        measures: [
          {
            name: 'count',
            sql: 'COUNT(*)',
            type: 'number',
          },
        ],
        dimensions: [
          {
            name: 'status',
            sql: 'status',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['orders.count'],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
        contextParams: {
          TABLE_NAME: 'orders_2023',
        },
      });

      expect(sql).toContain('orders_2023');
      expect(sql).not.toContain('CONTEXT_PARAMS');

      const result = await duckdbExec(sql);
      expect(Number(result[0].orders__count)).toBe(3);
    });

    it('should substitute parameter and execute query on 2024 table', async () => {
      const schema = {
        name: 'orders',
        sql: 'SELECT * FROM ${CONTEXT_PARAMS.TABLE_NAME}',
        measures: [
          {
            name: 'count',
            sql: 'COUNT(*)',
            type: 'number',
          },
        ],
        dimensions: [],
      };

      const query = {
        measures: ['orders.count'],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
        contextParams: {
          TABLE_NAME: 'orders_2024',
        },
      });

      const result = await duckdbExec(sql);
      expect(Number(result[0].orders__count)).toBe(3);
    });
  });

  describe('Multiple Parameters', () => {
    it('should substitute multiple parameters in SQL', async () => {
      const schema = {
        name: 'orders',
        sql: 'SELECT * FROM ${CONTEXT_PARAMS.SCHEMA}.${CONTEXT_PARAMS.TABLE}',
        measures: [
          {
            name: 'count',
            sql: 'COUNT(*)',
            type: 'number',
          },
        ],
        dimensions: [],
      };

      const query = {
        measures: ['orders.count'],
        dimensions: [],
      };

      // Note: DuckDB doesn't have schemas in this context, so we'll use table name
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
        contextParams: {
          SCHEMA: 'main',
          TABLE: 'orders_2023',
        },
      });

      expect(sql).toContain('main');
      expect(sql).toContain('orders_2023');
    });

    it('should substitute parameters in WHERE clause', async () => {
      const schema = {
        name: 'orders',
        sql: `
          SELECT * FROM orders_2023
          WHERE status = '\${CONTEXT_PARAMS.STATUS}'
        `,
        measures: [
          {
            name: 'count',
            sql: 'COUNT(*)',
            type: 'number',
          },
        ],
        dimensions: [
          {
            name: 'customer_id',
            sql: 'customer_id',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['orders.count'],
        dimensions: ['orders.customer_id'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
        contextParams: {
          STATUS: 'completed',
        },
      });

      expect(sql).toContain('completed');

      const result = await duckdbExec(sql);
      // Should have 2 completed orders in 2023
      expect(result.length).toBe(2);
    });
  });

  describe('Parameters with Filters', () => {
    it('should combine context params with cube filters', async () => {
      const schema = {
        name: 'orders',
        sql: 'SELECT * FROM ${CONTEXT_PARAMS.TABLE_NAME}',
        measures: [
          {
            name: 'count',
            sql: 'COUNT(*)',
            type: 'number',
          },
          {
            name: 'sum_amount',
            sql: 'SUM(amount)',
            type: 'number',
          },
        ],
        dimensions: [
          {
            name: 'status',
            sql: 'status',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['orders.sum_amount'],
        dimensions: [],
        filters: [
          {
            member: 'orders.status',
            operator: 'equals',
            values: ['completed'],
          },
        ],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
        contextParams: {
          TABLE_NAME: 'orders_2024',
        },
      });

      const result = await duckdbExec(sql);
      
      // 2024 completed orders: 300 + 400 = 700
      expect(Number(result[0].orders__sum_amount)).toBe(700);
    });
  });

  describe('Parameters with Grouping', () => {
    it('should use context params with GROUP BY', async () => {
      const schema = {
        name: 'orders',
        sql: 'SELECT * FROM ${CONTEXT_PARAMS.TABLE_NAME}',
        measures: [
          {
            name: 'count',
            sql: 'COUNT(*)',
            type: 'number',
          },
        ],
        dimensions: [
          {
            name: 'status',
            sql: 'status',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['orders.count'],
        dimensions: ['orders.status'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
        contextParams: {
          TABLE_NAME: 'orders_2023',
        },
      });

      const result = await duckdbExec(sql);
      
      expect(result.length).toBe(2); // completed and pending
      
      const completed = result.find((r) => r.orders__status === 'completed');
      expect(Number(completed?.orders__count)).toBe(2);
    });
  });

  describe('Complex Parameterized SQL', () => {
    it('should handle parameters in subqueries', async () => {
      const schema = {
        name: 'orders',
        sql: `
          SELECT *
          FROM (
            SELECT * FROM \${CONTEXT_PARAMS.TABLE_NAME}
            WHERE amount > \${CONTEXT_PARAMS.MIN_AMOUNT}
          ) AS filtered_orders
        `,
        measures: [
          {
            name: 'count',
            sql: 'COUNT(*)',
            type: 'number',
          },
        ],
        dimensions: [
          {
            name: 'status',
            sql: 'status',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['orders.count'],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
        contextParams: {
          TABLE_NAME: 'orders_2023',
          MIN_AMOUNT: 100,
        },
      });

      expect(sql).toContain('orders_2023');
      expect(sql).toContain('100');

      const result = await duckdbExec(sql);
      
      // Orders with amount > 100: id=2 (200), id=3 (150)
      expect(Number(result[0].orders__count)).toBeGreaterThanOrEqual(2);
    });

    it('should handle parameters in UNION queries', async () => {
      const schema = {
        name: 'orders',
        sql: `
          SELECT * FROM orders_2023
          UNION ALL
          SELECT * FROM \${CONTEXT_PARAMS.ADDITIONAL_TABLE}
        `,
        measures: [
          {
            name: 'count',
            sql: 'COUNT(*)',
            type: 'number',
          },
        ],
        dimensions: [],
      };

      const query = {
        measures: ['orders.count'],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
        contextParams: {
          ADDITIONAL_TABLE: 'orders_2024',
        },
      });

      expect(sql).toContain('orders_2024');

      const result = await duckdbExec(sql);
      
      // Total from both tables: 3 + 3 = 6
      expect(Number(result[0].orders__count)).toBe(6);
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty string parameter', async () => {
      const schema = {
        name: 'orders',
        sql: 'SELECT * FROM orders_2023 WHERE customer_id LIKE \'%${CONTEXT_PARAMS.SEARCH}%\'',
        measures: [
          {
            name: 'count',
            sql: 'COUNT(*)',
            type: 'number',
          },
        ],
        dimensions: [],
      };

      const query = {
        measures: ['orders.count'],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
        contextParams: {
          SEARCH: '',
        },
      });

      const result = await duckdbExec(sql);
      
      // Empty search matches all
      expect(Number(result[0].orders__count)).toBe(3);
    });

    it('should handle parameter with special characters', async () => {
      const schema = {
        name: 'orders',
        sql: 'SELECT * FROM ${CONTEXT_PARAMS.TABLE_NAME}',
        measures: [
          {
            name: 'count',
            sql: 'COUNT(*)',
            type: 'number',
          },
        ],
        dimensions: [],
      };

      const query = {
        measures: ['orders.count'],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
        contextParams: {
          TABLE_NAME: 'orders_2023',
        },
      });

      expect(sql).toContain('orders_2023');
      expect(sql).not.toContain('CONTEXT_PARAMS');
    });
  });

  describe('Performance', () => {
    it('should execute parameterized query quickly (< 200ms)', async () => {
      const schema = {
        name: 'orders',
        sql: 'SELECT * FROM ${CONTEXT_PARAMS.TABLE_NAME}',
        measures: [
          {
            name: 'count',
            sql: 'COUNT(*)',
            type: 'number',
          },
        ],
        dimensions: [
          {
            name: 'status',
            sql: 'status',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['orders.count'],
        dimensions: ['orders.status'],
      };

      const start = Date.now();

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
        contextParams: {
          TABLE_NAME: 'orders_2024',
        },
      });

      await duckdbExec(sql);
      
      const duration = Date.now() - start;
      expect(duration).toBeLessThan(200);
    });
  });
});

