/**
 * Comprehensive HAVING Clause Tests
 * 
 * Tests HAVING clause functionality with aggregates:
 * - HAVING with COUNT
 * - HAVING with SUM, AVG, MIN, MAX
 * - HAVING with comparison operators
 * - HAVING with complex conditions
 * - Performance on large datasets
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: HAVING Clauses', () => {
  beforeAll(async () => {
    console.log('🚀 Starting HAVING clause tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('HAVING with COUNT', () => {
    it('should filter groups by COUNT > threshold', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY priority
        HAVING COUNT(*) > 150000
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      // All 5 priorities have ~200K rows each, so all should pass
      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(150000);
      });
    });

    it('should filter groups by COUNT < threshold', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY priority
        HAVING COUNT(*) < 10000
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      // No priority has fewer than 10K rows
      expect(result.length).toBe(0);
    });

    it('should filter groups by COUNT = exact value', async () => {
      const sql = `
        SELECT 
          user_id,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY user_id
        HAVING COUNT(*) = 100
        ORDER BY user_id
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      // Each user_id appears exactly 100 times (1M / 10K users)
      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(Number(row.count)).toBe(100);
      });
    });

    it('should filter groups by COUNT DISTINCT', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(DISTINCT user_id) as distinct_users
        FROM fact_all_types
        GROUP BY priority
        HAVING COUNT(DISTINCT user_id) > 9000
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      // Each priority should have all 10K distinct users
      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.distinct_users)).toBeGreaterThan(9000);
      });
    });
  });

  describe('HAVING with SUM', () => {
    it('should filter groups by SUM > threshold', async () => {
      const sql = `
        SELECT 
          priority,
          SUM(id_bigint) as total_ids
        FROM fact_all_types
        GROUP BY priority
        HAVING SUM(id_bigint) > 99000000000
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      // Each priority has ~200K rows, sum should be significant
      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(Number(row.total_ids)).toBeGreaterThan(99000000000);
      });
    });

    it('should filter groups by SUM with WHERE clause', async () => {
      const sql = `
        SELECT 
          status,
          SUM(id_bigint) as total_ids
        FROM fact_all_types
        WHERE id_bigint < 100000
        GROUP BY status
        HAVING SUM(id_bigint) > 1000000000
        ORDER BY status
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(Number(row.total_ids)).toBeGreaterThan(1000000000);
      });
    });
  });

  describe('HAVING with AVG', () => {
    it('should filter groups by AVG > threshold', async () => {
      const sql = `
        SELECT 
          priority,
          AVG(metric_double) as avg_metric
        FROM fact_all_types
        GROUP BY priority
        HAVING AVG(metric_double) > 0
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      // All priorities should have avg > 0
      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.avg_metric)).toBeGreaterThan(0);
      });
    });

    it('should filter groups by AVG in range', async () => {
      const sql = `
        SELECT 
          status,
          AVG(id_bigint) as avg_id
        FROM fact_all_types
        GROUP BY status
        HAVING AVG(id_bigint) > 400000 AND AVG(id_bigint) < 600000
        ORDER BY status
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        const avgId = Number(row.avg_id);
        expect(avgId).toBeGreaterThan(400000);
        expect(avgId).toBeLessThan(600000);
      });
    });
  });

  describe('HAVING with MIN/MAX', () => {
    it('should filter groups by MAX > threshold', async () => {
      const sql = `
        SELECT 
          priority,
          MAX(id_bigint) as max_id
        FROM fact_all_types
        GROUP BY priority
        HAVING MAX(id_bigint) > 900000
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      // All priorities should have max > 900000
      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.max_id)).toBeGreaterThan(900000);
      });
    });

    it('should filter groups by MIN < threshold', async () => {
      const sql = `
        SELECT 
          status,
          MIN(id_bigint) as min_id
        FROM fact_all_types
        GROUP BY status
        HAVING MIN(id_bigint) < 100
        ORDER BY status
      `;
      const result = await duckdbExec(sql);

      // All statuses should have min < 100
      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(Number(row.min_id)).toBeLessThan(100);
      });
    });

    it('should filter groups by MAX - MIN range', async () => {
      const sql = `
        SELECT 
          priority,
          MAX(id_bigint) - MIN(id_bigint) as id_range
        FROM fact_all_types
        GROUP BY priority
        HAVING (MAX(id_bigint) - MIN(id_bigint)) > 900000
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      // All priorities span the full range
      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.id_range)).toBeGreaterThan(900000);
      });
    });
  });

  describe('HAVING with Multiple Aggregates', () => {
    it('should filter by multiple aggregate conditions', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as count,
          AVG(metric_double) as avg_metric
        FROM fact_all_types
        GROUP BY priority
        HAVING COUNT(*) > 150000 AND AVG(metric_double) > 0
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(150000);
        expect(Number(row.avg_metric)).toBeGreaterThan(0);
      });
    });

    it('should combine WHERE, GROUP BY, and HAVING', async () => {
      const sql = `
        SELECT 
          priority,
          status,
          COUNT(*) as count,
          SUM(id_bigint) as total_ids
        FROM fact_all_types
        WHERE is_active = true
        GROUP BY priority, status
        HAVING COUNT(*) > 10000
        ORDER BY priority, status
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(10000);
        expect(Number(row.total_ids)).toBeGreaterThan(0);
      });
    });
  });

  describe('HAVING with Date Aggregates', () => {
    it('should filter groups by date range', async () => {
      const sql = `
        SELECT 
          priority,
          MIN(created_date) as earliest_date,
          MAX(created_date) as latest_date,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY priority
        HAVING MAX(created_date) > DATE '2020-06-01'
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      // All priorities span the full date range
      expect(result.length).toBe(5);

      result.forEach((row) => {
        const latestDate = row.latest_date instanceof Date
          ? row.latest_date.toISOString().split('T')[0]
          : row.latest_date;
        expect(latestDate > '2020-06-01').toBe(true);
      });
    });

    it('should filter by date count in month', async () => {
      const sql = `
        SELECT 
          EXTRACT(MONTH FROM created_date) as month,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY EXTRACT(MONTH FROM created_date)
        HAVING COUNT(*) > 75000
        ORDER BY month
      `;
      const result = await duckdbExec(sql);

      // Months with enough rows
      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(75000);
      });
    });
  });

  describe('HAVING with JOINs', () => {
    it('should filter joined groups by aggregate', async () => {
      const sql = `
        SELECT 
          u.user_segment,
          COUNT(*) as fact_count,
          AVG(f.metric_double) as avg_metric
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        GROUP BY u.user_segment
        HAVING COUNT(*) > 300000
        ORDER BY u.user_segment
      `;
      const result = await duckdbExec(sql);

      // Each segment should have ~333K rows
      expect(result.length).toBe(3);

      result.forEach((row) => {
        expect(Number(row.fact_count)).toBeGreaterThan(300000);
      });
    });

    it('should filter multi-table join by aggregate', async () => {
      const sql = `
        SELECT 
          u.user_segment,
          p.product_category,
          COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        INNER JOIN dim_part p ON f.part_id = p.part_id
        GROUP BY u.user_segment, p.product_category
        HAVING COUNT(*) > 60000
        ORDER BY u.user_segment, p.product_category
      `;
      const result = await duckdbExec(sql);

      // 3 segments * 5 categories = 15 combinations, ~66K each
      expect(result.length).toBe(15);

      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(60000);
      });
    });
  });

  describe('HAVING with ORDER BY and LIMIT', () => {
    it('should combine HAVING with ORDER BY on aggregate', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY priority
        HAVING COUNT(*) > 100000
        ORDER BY count DESC
        LIMIT 3
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);

      // Verify descending order
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].count)).toBeLessThanOrEqual(Number(result[i - 1].count));
      }
    });

    it('should filter top N groups by aggregate', async () => {
      const sql = `
        SELECT 
          user_id,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY user_id
        HAVING COUNT(*) >= 100
        ORDER BY count DESC
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThanOrEqual(100);
      });
    });
  });

  describe('HAVING Edge Cases', () => {
    it('should handle HAVING with no groups passing', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY priority
        HAVING COUNT(*) > 5000000
      `;
      const result = await duckdbExec(sql);

      // No group has > 5M rows
      expect(result.length).toBe(0);
    });

    it('should handle HAVING with all groups passing', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY priority
        HAVING COUNT(*) > 0
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      // All groups have > 0 rows
      expect(result.length).toBe(5);
    });

    it('should handle HAVING with complex expression', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as count,
          AVG(id_bigint) as avg_id
        FROM fact_all_types
        GROUP BY priority
        HAVING (COUNT(*) * AVG(id_bigint)) > 50000000000
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        const product = Number(row.count) * Number(row.avg_id);
        expect(product).toBeGreaterThan(50000000000);
      });
    });
  });

  describe('Performance', () => {
    it('should execute HAVING query quickly (< 500ms)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          priority,
          status,
          COUNT(*) as count,
          AVG(metric_double) as avg_metric
        FROM fact_all_types
        GROUP BY priority, status
        HAVING COUNT(*) > 50000
        ORDER BY priority, status
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(500);
    });

    it('should execute complex HAVING with JOIN quickly (< 1s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          u.user_segment,
          p.product_category,
          COUNT(*) as count,
          SUM(f.id_bigint) as total_ids
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        INNER JOIN dim_part p ON f.part_id = p.part_id
        GROUP BY u.user_segment, p.product_category
        HAVING COUNT(*) > 60000
        ORDER BY count DESC
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(1000);
    });
  });
});

