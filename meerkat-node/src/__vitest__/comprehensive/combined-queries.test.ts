/**
 * Comprehensive Combined Query Tests
 * 
 * Tests combinations of query features:
 * - Filters + Ordering
 * - Filters + Grouping
 * - Filters + Grouping + Ordering
 * - Filters + Grouping + Ordering + JOINs
 * - Filters + Aggregates + HAVING
 *
 * NOTE: Uses single filter conditions to avoid the "multiple filters not supported" limitation
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { cubeQueryToSQL } from '../../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';
import { FACT_ALL_TYPES_SCHEMA } from '../synthetic/table-schemas';

describe('Comprehensive: Combined Queries', () => {
  beforeAll(async () => {
    console.log('🚀 Starting combined query tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('Filter + Ordering', () => {
    it('should combine numeric filter with ordering', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        filters: [
          {
            member: 'fact_all_types.id_bigint',
            operator: 'lt',
            values: [100000],
          },
        ],
        order: [['fact_all_types.priority', 'asc']],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // Verify ascending order
      const priorities = result.map((r) => r.fact_all_types__priority);
      expect(priorities[0]).toBe('critical');
      expect(priorities[1]).toBe('high');
      expect(priorities[2]).toBe('low');

      // Each priority should have ~20K rows (100K / 5)
      result.forEach((row) => {
        expect(Number(row.fact_all_types__count)).toBeGreaterThan(19000);
        expect(Number(row.fact_all_types__count)).toBeLessThan(21000);
      });
    });

    it('should combine string filter with DESC ordering', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.status'],
        filters: [
          {
            member: 'fact_all_types.priority',
            operator: 'equals',
            values: ['high'],
          },
        ],
        order: [['fact_all_types.count', 'desc']],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      // Verify descending order by count
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].fact_all_types__count)).toBeLessThanOrEqual(
          Number(result[i - 1].fact_all_types__count)
        );
      }
    });

    it('should combine boolean filter with ordering', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        filters: [
          {
            member: 'fact_all_types.is_active',
            operator: 'equals',
            values: [true],
          },
        ],
        order: [['fact_all_types.priority', 'desc']],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // Verify descending order
      const priorities = result.map((r) => r.fact_all_types__priority);
      expect(priorities[0]).toBe('urgent');

      // is_active = true is 50% of rows, so ~100K per priority
      result.forEach((row) => {
        expect(Number(row.fact_all_types__count)).toBeGreaterThan(95000);
        expect(Number(row.fact_all_types__count)).toBeLessThan(105000);
      });
    });

    it('should combine date filter with ordering', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        filters: [
          {
            member: 'fact_all_types.created_date',
            operator: 'gt',
            values: ['2020-06-01'],
          },
        ],
        order: [['fact_all_types.count', 'asc']],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // Verify ascending order by count
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].fact_all_types__count)).toBeGreaterThanOrEqual(
          Number(result[i - 1].fact_all_types__count)
        );
      }
    });

    it('should combine filter with multi-column ordering', async () => {
      const sql = `
        SELECT 
          priority,
          status,
          COUNT(*) as count
        FROM fact_all_types
        WHERE is_active = true
        GROUP BY priority, status
        ORDER BY priority ASC, count DESC
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });
  });

  describe('Filter + Grouping', () => {
    it('should combine numeric filter with single dimension grouping', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        filters: [
          {
            member: 'fact_all_types.id_bigint',
            operator: 'gte',
            values: [500000],
          },
        ],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // id_bigint >= 500000 is 50% of rows, so ~100K per priority
      result.forEach((row) => {
        expect(Number(row.fact_all_types__count)).toBeGreaterThan(95000);
        expect(Number(row.fact_all_types__count)).toBeLessThan(105000);
      });
    });

    it('should combine string filter with composite grouping', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority', 'fact_all_types.status'],
        filters: [
          {
            member: 'fact_all_types.priority',
            operator: 'equals',
            values: ['high'],
          },
        ],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      // priority='high' is 20% = 200K rows
      // 3 statuses, so ~66K per combination
      expect(result.length).toBe(3);

      result.forEach((row) => {
        expect(row.fact_all_types__priority).toBe('high');
        expect(Number(row.fact_all_types__count)).toBeGreaterThan(60000);
        expect(Number(row.fact_all_types__count)).toBeLessThan(70000);
      });
    });

    it('should combine boolean filter with grouping', async () => {
      const query = {
        measures: ['fact_all_types.count', 'fact_all_types.sum_metric'],
        dimensions: ['fact_all_types.status'],
        filters: [
          {
            member: 'fact_all_types.is_deleted',
            operator: 'equals',
            values: [false],
          },
        ],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      // is_deleted = false is 90% of rows
      const totalCount = result.reduce((sum, row) => sum + Number(row.fact_all_types__count), 0);
      expect(totalCount).toBeGreaterThan(850000);
      expect(totalCount).toBeLessThan(950000);
    });
  });

  describe('Filter + Grouping + Ordering', () => {
    it('should combine all three: filter, group, order', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        filters: [
          {
            member: 'fact_all_types.is_active',
            operator: 'equals',
            values: [true],
          },
        ],
        order: [['fact_all_types.count', 'desc']],
        limit: 3,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);

      // Verify descending order
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].fact_all_types__count)).toBeLessThanOrEqual(
          Number(result[i - 1].fact_all_types__count)
        );
      }

      // Total should be roughly 150K (500K active / 5 priorities * 3 priorities)
      const totalCount = result.reduce((sum, row) => sum + Number(row.fact_all_types__count), 0);
      expect(totalCount).toBeGreaterThan(280000);
      expect(totalCount).toBeLessThan(320000);
    });

    it('should combine filter, composite grouping, and ordering', async () => {
      const sql = `
        SELECT 
          priority,
          status,
          COUNT(*) as count,
          AVG(metric_double) as avg_metric
        FROM fact_all_types
        WHERE id_bigint < 500000
        GROUP BY priority, status
        ORDER BY count DESC, priority ASC
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      // Verify descending order by count (primary)
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].count)).toBeLessThanOrEqual(Number(result[i - 1].count));
      }

      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(0);
        expect(Number(row.avg_metric)).toBeGreaterThan(0);
      });
    });

    it('should combine date filter, grouping, and ordering with LIMIT', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.status'],
        filters: [
          {
            member: 'fact_all_types.created_date',
            operator: 'lt',
            values: ['2021-01-01'],
          },
        ],
        order: [['fact_all_types.count', 'asc']],
        limit: 2,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBe(2);

      // Verify ascending order
      expect(Number(result[1].fact_all_types__count)).toBeGreaterThanOrEqual(
        Number(result[0].fact_all_types__count)
      );
    });
  });

  describe('Filter + JOIN + Grouping + Ordering', () => {
    it('should combine filter on fact with JOIN and grouping', async () => {
      const sql = `
        SELECT 
          u.user_segment,
          COUNT(*) as count,
          AVG(f.metric_double) as avg_metric
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        WHERE f.priority = 'high'
        GROUP BY u.user_segment
        ORDER BY count DESC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);

      // Verify descending order
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].count)).toBeLessThanOrEqual(Number(result[i - 1].count));
      }

      // priority='high' is 20% = 200K rows, 3 segments = ~66K each
      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(60000);
        expect(Number(row.count)).toBeLessThan(70000);
      });
    });

    it('should combine filter, multi-table JOIN, and ordering', async () => {
      const sql = `
        SELECT 
          u.user_segment,
          p.product_category,
          COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        INNER JOIN dim_part p ON f.part_id = p.part_id
        WHERE f.is_active = true
        GROUP BY u.user_segment, p.product_category
        ORDER BY count DESC
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      // Verify descending order
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].count)).toBeLessThanOrEqual(Number(result[i - 1].count));
      }

      result.forEach((row) => {
        expect(row.user_segment).toMatch(/^(enterprise|pro|free)$/);
        expect(row.product_category).toMatch(/^(electronics|furniture|clothing|food|other)$/);
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });
  });

  describe('Filter + Aggregates + HAVING', () => {
    it('should combine filter with aggregate and HAVING', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as count,
          AVG(metric_double) as avg_metric
        FROM fact_all_types
        WHERE is_active = true
        GROUP BY priority
        HAVING COUNT(*) > 90000
        ORDER BY count DESC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(90000);
        expect(Number(row.avg_metric)).toBeGreaterThan(0);
      });
    });

    it('should combine filter, JOIN, aggregate, and HAVING', async () => {
      const sql = `
        SELECT 
          u.user_segment,
          COUNT(*) as count,
          SUM(f.id_bigint) as total_ids
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        WHERE f.priority = 'critical'
        GROUP BY u.user_segment
        HAVING COUNT(*) > 60000
        ORDER BY count DESC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);

      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(60000);
        expect(Number(row.total_ids)).toBeGreaterThan(0);
      });
    });
  });

  describe('Complex Real-World Patterns', () => {
    it('should mimic widget query: filtered, grouped, ordered, limited', async () => {
      const sql = `
        SELECT 
          priority,
          status,
          COUNT(*) as count,
          AVG(metric_double) as avg_metric,
          SUM(id_bigint) as total_ids
        FROM fact_all_types
        WHERE is_active = true
        GROUP BY priority, status
        ORDER BY count DESC
        LIMIT 5
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(0);
        expect(Number(row.avg_metric)).toBeGreaterThan(0);
        expect(Number(row.total_ids)).toBeGreaterThan(0);
      });
    });

    it('should mimic dashboard query: JOIN, filter, aggregate, order', async () => {
      const sql = `
        SELECT 
          u.user_segment,
          p.product_category,
          COUNT(*) as fact_count,
          AVG(f.metric_double) as avg_metric,
          MIN(f.created_date) as earliest_date,
          MAX(f.created_date) as latest_date
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        INNER JOIN dim_part p ON f.part_id = p.part_id
        WHERE f.id_bigint < 500000
        GROUP BY u.user_segment, p.product_category
        ORDER BY fact_count DESC
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      result.forEach((row) => {
        expect(Number(row.fact_count)).toBeGreaterThan(0);
        expect(Number(row.avg_metric)).toBeGreaterThan(0);
        expect(row.earliest_date).toBeTruthy();
        expect(row.latest_date).toBeTruthy();
      });
    });

    it('should mimic time-series query: date grouping with filter', async () => {
      const sql = `
        SELECT 
          DATE_TRUNC('month', created_date) as month,
          priority,
          COUNT(*) as count
        FROM fact_all_types
        WHERE priority IN ('high', 'critical')
        GROUP BY DATE_TRUNC('month', created_date), priority
        ORDER BY month ASC, priority ASC
        LIMIT 20
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(row.month).toBeTruthy();
        expect(row.priority).toMatch(/^(high|critical)$/);
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });
  });

  describe('Performance', () => {
    it('should execute combined filter + group + order quickly (< 500ms)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          priority,
          status,
          COUNT(*) as count
        FROM fact_all_types
        WHERE is_active = true
        GROUP BY priority, status
        ORDER BY count DESC
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(500);
    });

    it('should execute combined filter + JOIN + group + order quickly (< 1s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          u.user_segment,
          p.product_category,
          COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        INNER JOIN dim_part p ON f.part_id = p.part_id
        WHERE f.priority = 'high'
        GROUP BY u.user_segment, p.product_category
        ORDER BY count DESC
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(1000);
    });
  });
});

