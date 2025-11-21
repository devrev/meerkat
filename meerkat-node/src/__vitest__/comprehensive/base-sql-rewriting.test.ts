/**
 * Comprehensive Base SQL Rewriting Tests
 * 
 * Tests how the engine rewrites base SQL with various query operations:
 * - Simple SELECT * rewriting
 * - Complex base SQL with subqueries
 * - Base SQL with WHERE clauses
 * - Base SQL with JOINs
 * - Base SQL with UNION
 * - Base SQL with CTEs (Common Table Expressions)
 * - Adding filters to base SQL
 * - Adding grouping to base SQL
 * - Adding ordering to base SQL
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { cubeQueryToSQL } from '../../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: Base SQL Rewriting', () => {
  beforeAll(async () => {
    console.log('🚀 Starting base SQL rewriting tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('Simple Base SQL', () => {
    it('should rewrite simple SELECT *', async () => {
      const schema = {
        name: 'fact',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          {
            name: 'count',
            sql: 'COUNT(*)',
            type: 'number',
          },
        ],
        dimensions: [
          {
            name: 'priority',
            sql: 'priority',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact.count'],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      expect(sql).toContain('fact_all_types');

      const result = await duckdbExec(sql);
      expect(Number(result[0].fact__count)).toBe(1000000);
    });

    it('should add WHERE clause to simple base SQL', async () => {
      const schema = {
        name: 'fact',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          {
            name: 'count',
            sql: 'COUNT(*)',
            type: 'number',
          },
        ],
        dimensions: [
          {
            name: 'priority',
            sql: 'priority',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact.count'],
        dimensions: [],
        filters: [
          {
            member: 'fact.priority',
            operator: 'equals',
            values: ['high'],
          },
        ],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      const result = await duckdbExec(sql);
      
      // high priority is 20% of 1M = 200K
      expect(Number(result[0].fact__count)).toBeGreaterThan(190000);
      expect(Number(result[0].fact__count)).toBeLessThan(210000);
    });

    it('should add GROUP BY to simple base SQL', async () => {
      const schema = {
        name: 'fact',
        sql: 'SELECT * FROM fact_all_types',
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
        measures: ['fact.count'],
        dimensions: ['fact.status'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      const result = await duckdbExec(sql);
      
      // 3 statuses: open, in_progress, closed
      expect(result.length).toBe(3);
      
      result.forEach((row) => {
        expect(Number(row.fact__count)).toBeGreaterThan(300000);
      });
    });
  });

  describe('Complex Base SQL with Subqueries', () => {
    it('should rewrite base SQL with subquery', async () => {
      const schema = {
        name: 'fact',
        sql: `
          SELECT *
          FROM (
            SELECT *
            FROM fact_all_types
            WHERE is_active = true
          ) AS active_facts
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
            name: 'priority',
            sql: 'priority',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact.count'],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      const result = await duckdbExec(sql);
      
      // is_active = true is 50% of 1M = 500K
      expect(Number(result[0].fact__count)).toBeGreaterThan(490000);
      expect(Number(result[0].fact__count)).toBeLessThan(510000);
    });

    it('should add filters to base SQL with existing WHERE', async () => {
      const schema = {
        name: 'fact',
        sql: `
          SELECT *
          FROM fact_all_types
          WHERE is_active = true
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
            name: 'priority',
            sql: 'priority',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact.count'],
        dimensions: [],
        filters: [
          {
            member: 'fact.priority',
            operator: 'equals',
            values: ['high'],
          },
        ],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      const result = await duckdbExec(sql);
      
      // is_active (50%) AND high priority (20%) = 10% of 1M = 100K
      expect(Number(result[0].fact__count)).toBeGreaterThan(95000);
      expect(Number(result[0].fact__count)).toBeLessThan(105000);
    });
  });

  describe('Base SQL with Aggregates', () => {
    it('should use aggregates defined in base SQL', async () => {
      const schema = {
        name: 'fact',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          {
            name: 'avg_metric',
            sql: 'AVG(metric_double)',
            type: 'number',
          },
          {
            name: 'sum_ids',
            sql: 'SUM(id_bigint)',
            type: 'number',
          },
        ],
        dimensions: [
          {
            name: 'priority',
            sql: 'priority',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact.avg_metric', 'fact.sum_ids'],
        dimensions: ['fact.priority'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      const result = await duckdbExec(sql);
      
      expect(result.length).toBe(5); // 5 priorities
      
      result.forEach((row) => {
        expect(Number(row.fact__avg_metric)).toBeGreaterThan(0);
        expect(Number(row.fact__sum_ids)).toBeGreaterThan(0);
      });
    });
  });

  describe('Base SQL with JOINs', () => {
    it('should preserve JOINs in base SQL', async () => {
      const schema = {
        name: 'fact',
        sql: `
          SELECT 
            f.*,
            u.user_segment,
            u.user_department
          FROM fact_all_types f
          INNER JOIN dim_user u ON f.user_id = u.user_id
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
            name: 'user_segment',
            sql: 'user_segment',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact.count'],
        dimensions: ['fact.user_segment'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      const result = await duckdbExec(sql);
      
      // 3 segments
      expect(result.length).toBe(3);
      
      const segments = result.map((r) => r.fact__user_segment);
      expect(segments).toContain('enterprise');
      expect(segments).toContain('pro');
      expect(segments).toContain('free');
    });

    it('should add filters to base SQL with JOINs', async () => {
      const schema = {
        name: 'fact',
        sql: `
          SELECT 
            f.*,
            u.user_segment
          FROM fact_all_types f
          INNER JOIN dim_user u ON f.user_id = u.user_id
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
            name: 'user_segment',
            sql: 'user_segment',
            type: 'string',
          },
          {
            name: 'priority',
            sql: 'priority',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact.count'],
        dimensions: ['fact.user_segment'],
        filters: [
          {
            member: 'fact.priority',
            operator: 'equals',
            values: ['high'],
          },
        ],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      const result = await duckdbExec(sql);
      
      expect(result.length).toBe(3);
      
      // high priority (20%) across 3 segments = ~66K each
      result.forEach((row) => {
        expect(Number(row.fact__count)).toBeGreaterThan(60000);
        expect(Number(row.fact__count)).toBeLessThan(70000);
      });
    });
  });

  describe('Base SQL with UNION', () => {
    it('should handle base SQL with UNION ALL', async () => {
      const schema = {
        name: 'fact',
        sql: `
          SELECT * FROM fact_all_types WHERE priority = 'high'
          UNION ALL
          SELECT * FROM fact_all_types WHERE priority = 'critical'
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
        measures: ['fact.count'],
        dimensions: ['fact.status'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      const result = await duckdbExec(sql);
      
      expect(result.length).toBe(3); // 3 statuses
      
      // high (20%) + critical (20%) = 40% of 1M = 400K total
      const totalCount = result.reduce((sum, row) => sum + Number(row.fact__count), 0);
      expect(totalCount).toBeGreaterThan(380000);
      expect(totalCount).toBeLessThan(420000);
    });
  });

  describe('Base SQL with Date Functions', () => {
    it('should rewrite base SQL with DATE_TRUNC', async () => {
      const schema = {
        name: 'fact',
        sql: `
          SELECT 
            *,
            DATE_TRUNC('month', created_date) AS month
          FROM fact_all_types
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
            name: 'month',
            sql: 'month',
            type: 'time',
          },
        ],
      };

      const query = {
        measures: ['fact.count'],
        dimensions: ['fact.month'],
        limit: 5,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      const result = await duckdbExec(sql);
      
      expect(result.length).toBe(5);
      
      result.forEach((row) => {
        expect(row.fact__month).toBeTruthy();
        expect(Number(row.fact__count)).toBeGreaterThan(0);
      });
    });

    it('should add filters to base SQL with date functions', async () => {
      const schema = {
        name: 'fact',
        sql: `
          SELECT 
            *,
            EXTRACT(YEAR FROM created_date) AS year
          FROM fact_all_types
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
            name: 'year',
            sql: 'year',
            type: 'number',
          },
          {
            name: 'priority',
            sql: 'priority',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact.count'],
        dimensions: ['fact.year'],
        filters: [
          {
            member: 'fact.priority',
            operator: 'equals',
            values: ['high'],
          },
        ],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      const result = await duckdbExec(sql);
      
      expect(result.length).toBeGreaterThan(0);
      
      result.forEach((row) => {
        expect(Number(row.fact__year)).toBeGreaterThan(2019);
        expect(Number(row.fact__count)).toBeGreaterThan(0);
      });
    });
  });

  describe('Complex Rewriting Scenarios', () => {
    it('should handle base SQL with CTEs', async () => {
      const schema = {
        name: 'fact',
        sql: `
          WITH active_facts AS (
            SELECT * FROM fact_all_types WHERE is_active = true
          )
          SELECT * FROM active_facts
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
            name: 'priority',
            sql: 'priority',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact.count'],
        dimensions: ['fact.priority'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      const result = await duckdbExec(sql);
      
      expect(result.length).toBe(5);
      
      // Each priority with is_active filter = ~100K
      result.forEach((row) => {
        expect(Number(row.fact__count)).toBeGreaterThan(95000);
        expect(Number(row.fact__count)).toBeLessThan(105000);
      });
    });

    it('should rewrite base SQL with window functions', async () => {
      const schema = {
        name: 'fact',
        sql: `
          SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY priority ORDER BY id_bigint) AS row_num
          FROM fact_all_types
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
            name: 'priority',
            sql: 'priority',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact.count'],
        dimensions: ['fact.priority'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      const result = await duckdbExec(sql);
      
      expect(result.length).toBe(5);
      
      result.forEach((row) => {
        expect(Number(row.fact__count)).toBeGreaterThan(190000);
      });
    });

    it('should add ordering to base SQL result', async () => {
      const schema = {
        name: 'fact',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          {
            name: 'count',
            sql: 'COUNT(*)',
            type: 'number',
          },
        ],
        dimensions: [
          {
            name: 'priority',
            sql: 'priority',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact.count'],
        dimensions: ['fact.priority'],
        order: [['fact.count', 'desc']],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      const result = await duckdbExec(sql);
      
      expect(result.length).toBe(5);
      
      // Verify descending order
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].fact__count)).toBeLessThanOrEqual(
          Number(result[i - 1].fact__count)
        );
      }
    });

    it('should add LIMIT to base SQL result', async () => {
      const schema = {
        name: 'fact',
        sql: 'SELECT * FROM fact_all_types',
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
        measures: ['fact.count'],
        dimensions: ['fact.status'],
        limit: 2,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      const result = await duckdbExec(sql);
      
      expect(result.length).toBe(2);
    });
  });

  describe('Performance', () => {
    it('should rewrite complex base SQL quickly (< 500ms)', async () => {
      const schema = {
        name: 'fact',
        sql: `
          SELECT 
            f.*,
            u.user_segment,
            p.product_category
          FROM fact_all_types f
          INNER JOIN dim_user u ON f.user_id = u.user_id
          INNER JOIN dim_part p ON f.part_id = p.part_id
          WHERE f.is_active = true
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
            name: 'user_segment',
            sql: 'user_segment',
            type: 'string',
          },
          {
            name: 'product_category',
            sql: 'product_category',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact.count'],
        dimensions: ['fact.user_segment', 'fact.product_category'],
        filters: [
          {
            member: 'fact.priority',
            operator: 'equals',
            values: ['high'],
          },
        ],
      };

      const start = Date.now();

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schema],
      });

      await duckdbExec(sql);
      
      const duration = Date.now() - start;
      expect(duration).toBeLessThan(500);
    });
  });
});

