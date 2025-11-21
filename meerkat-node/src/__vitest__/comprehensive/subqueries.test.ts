/**
 * Comprehensive Subqueries Tests
 * 
 * Tests subquery patterns in various contexts:
 * - WHERE EXISTS
 * - WHERE NOT EXISTS
 * - WHERE IN (subquery)
 * - WHERE NOT IN (subquery)
 * - Scalar subqueries in SELECT
 * - Correlated subqueries
 * - Multiple levels of nesting
 * - Subqueries in FROM clause
 * - Subqueries with aggregates
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: Subqueries', () => {
  beforeAll(async () => {
    console.log('🚀 Starting subquery tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('WHERE EXISTS', () => {
    it('should use EXISTS to check for related records', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types f
        WHERE EXISTS (
          SELECT 1 
          FROM dim_user u 
          WHERE u.user_id = f.user_id 
            AND u.user_segment = 'enterprise'
        )
        AND f.id_bigint < 10000
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should use EXISTS with multiple conditions', async () => {
      const sql = `
        SELECT f.priority, COUNT(*) as count
        FROM fact_all_types f
        WHERE EXISTS (
          SELECT 1 
          FROM dim_user u 
          WHERE u.user_id = f.user_id 
            AND u.user_segment IN ('enterprise', 'pro')
            AND u.is_active_user = true
        )
        AND f.id_bigint < 10000
        GROUP BY f.priority
        ORDER BY f.priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5); // All 5 priorities
      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use EXISTS with aggregates in subquery', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM dim_user u
        WHERE EXISTS (
          SELECT 1 
          FROM fact_all_types f 
          WHERE f.user_id = u.user_id 
            AND f.priority = 'high'
          HAVING COUNT(*) > 10
        )
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });
  });

  describe('WHERE NOT EXISTS', () => {
    it('should use NOT EXISTS to find unmatched records', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM dim_user u
        WHERE NOT EXISTS (
          SELECT 1 
          FROM fact_all_types f 
          WHERE f.user_id = u.user_id 
            AND f.priority = 'urgent'
            AND f.id_bigint < 10000
        )
      `;
      const result = await duckdbExec(sql);

      // Some users should not have urgent priority items
      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should use NOT EXISTS with date filters', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM dim_user u
        WHERE NOT EXISTS (
          SELECT 1 
          FROM fact_all_types f 
          WHERE f.user_id = u.user_id 
            AND f.created_date > DATE '2023-01-01'
            AND f.id_bigint < 10000
        )
      `;
      const result = await duckdbExec(sql);

      expect(result[0].count).toBeDefined();
    });
  });

  describe('WHERE IN (subquery)', () => {
    it('should use IN with subquery returning single column', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE user_id IN (
          SELECT user_id 
          FROM dim_user 
          WHERE user_segment = 'enterprise'
        )
        AND id_bigint < 10000
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should use IN with subquery and aggregates', async () => {
      const sql = `
        SELECT priority, COUNT(*) as count
        FROM fact_all_types
        WHERE user_id IN (
          SELECT user_id 
          FROM dim_user 
          WHERE user_department = 'engineering'
        )
        AND id_bigint < 10000
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
    });

    it('should use IN with filtered subquery', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types f
        WHERE f.part_id IN (
          SELECT p.part_id 
          FROM dim_part p 
          WHERE p.product_category IN ('backend', 'frontend')
        )
        AND f.id_bigint < 10000
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });
  });

  describe('WHERE NOT IN (subquery)', () => {
    it('should use NOT IN with subquery', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE user_id NOT IN (
          SELECT user_id 
          FROM dim_user 
          WHERE user_segment = 'free'
        )
        AND id_bigint < 10000
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should handle NOT IN with NULL values correctly', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 100
          AND id_bigint NOT IN (
            SELECT resolved_by 
            FROM fact_all_types 
            WHERE resolved_by IS NOT NULL 
              AND id_bigint < 50
          )
      `;
      const result = await duckdbExec(sql);

      expect(result[0].count).toBeDefined();
    });
  });

  describe('Scalar Subqueries in SELECT', () => {
    it('should use scalar subquery to get related value', async () => {
      const sql = `
        SELECT 
          f.id_bigint,
          f.user_id,
          (SELECT u.user_segment FROM dim_user u WHERE u.user_id = f.user_id) as segment
        FROM fact_all_types f
        WHERE f.id_bigint < 10
        ORDER BY f.id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(row.segment).toBeTruthy();
      });
    });

    it('should use scalar subquery with aggregate', async () => {
      const sql = `
        SELECT 
          u.user_id,
          u.user_segment,
          (SELECT COUNT(*) FROM fact_all_types f WHERE f.user_id = u.user_id AND f.id_bigint < 10000) as fact_count
        FROM dim_user u
        WHERE u.user_id IN ('user_0', 'user_1', 'user_2')
        ORDER BY u.user_id
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);
      result.forEach((row) => {
        expect(Number(row.fact_count)).toBeGreaterThan(0);
      });
    });

    it('should use multiple scalar subqueries', async () => {
      const sql = `
        SELECT 
          f.id_bigint,
          (SELECT u.user_segment FROM dim_user u WHERE u.user_id = f.user_id) as segment,
          (SELECT p.product_category FROM dim_part p WHERE p.part_id = f.part_id) as category
        FROM fact_all_types f
        WHERE f.id_bigint < 10
        ORDER BY f.id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(row.segment).toBeTruthy();
        expect(row.category).toBeTruthy();
      });
    });
  });

  describe('Correlated Subqueries', () => {
    it('should use correlated subquery in WHERE', async () => {
      const sql = `
        SELECT 
          f1.id_bigint,
          f1.priority,
          f1.metric_double
        FROM fact_all_types f1
        WHERE f1.metric_double > (
          SELECT AVG(f2.metric_double)
          FROM fact_all_types f2
          WHERE f2.priority = f1.priority
            AND f2.id_bigint < 10000
        )
        AND f1.id_bigint < 1000
        ORDER BY f1.id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result.length).toBeLessThanOrEqual(10);
    });

    it('should use correlated subquery in SELECT', async () => {
      const sql = `
        SELECT 
          u.user_id,
          u.user_segment,
          (
            SELECT AVG(f.metric_double)
            FROM fact_all_types f
            WHERE f.user_id = u.user_id
              AND f.id_bigint < 10000
          ) as avg_metric
        FROM dim_user u
        WHERE u.user_id IN ('user_0', 'user_1', 'user_2')
        ORDER BY u.user_id
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);
      result.forEach((row) => {
        expect(Number(row.avg_metric)).toBeGreaterThan(0);
      });
    });

    it('should use correlated EXISTS with complex condition', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types f1
        WHERE EXISTS (
          SELECT 1
          FROM fact_all_types f2
          WHERE f2.priority = f1.priority
            AND f2.user_id = f1.user_id
            AND f2.metric_double > f1.metric_double
            AND f2.id_bigint < 10000
        )
        AND f1.id_bigint < 10000
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });
  });

  describe('Nested Subqueries', () => {
    it('should use 2-level nested subqueries', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE user_id IN (
          SELECT user_id FROM dim_user
          WHERE user_segment IN (
            SELECT DISTINCT user_segment FROM dim_user WHERE is_active_user = true
          )
        )
        AND id_bigint < 10000
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should use 3-level nested subqueries', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types f
        WHERE f.priority IN (
          SELECT DISTINCT f2.priority
          FROM fact_all_types f2
          WHERE f2.user_id IN (
            SELECT u.user_id
            FROM dim_user u
            WHERE u.user_segment IN (
              SELECT DISTINCT user_segment 
              FROM dim_user 
              WHERE user_department = 'engineering'
            )
          )
          AND f2.id_bigint < 5000
        )
        AND f.id_bigint < 10000
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });
  });

  describe('Subqueries in FROM (Derived Tables)', () => {
    it('should use subquery in FROM clause', async () => {
      const sql = `
        SELECT 
          priority,
          AVG(avg_metric) as overall_avg
        FROM (
          SELECT 
            priority,
            user_id,
            AVG(metric_double) as avg_metric
          FROM fact_all_types
          WHERE id_bigint < 10000
          GROUP BY priority, user_id
        ) AS user_averages
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
      result.forEach((row) => {
        expect(Number(row.overall_avg)).toBeGreaterThan(0);
      });
    });

    it('should join with subquery', async () => {
      const sql = `
        SELECT 
          u.user_segment,
          agg.avg_metric,
          agg.total_count
        FROM dim_user u
        INNER JOIN (
          SELECT 
            user_id,
            AVG(metric_double) as avg_metric,
            COUNT(*) as total_count
          FROM fact_all_types
          WHERE id_bigint < 10000
          GROUP BY user_id
        ) AS agg ON u.user_id = agg.user_id
        WHERE u.user_id IN ('user_0', 'user_1', 'user_2')
        ORDER BY u.user_id
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);
      result.forEach((row) => {
        expect(Number(row.avg_metric)).toBeGreaterThan(0);
        expect(Number(row.total_count)).toBeGreaterThan(0);
      });
    });

    it('should use multiple subqueries in FROM', async () => {
      const sql = `
        SELECT 
          fact_agg.priority,
          fact_agg.fact_count,
          user_agg.user_count
        FROM (
          SELECT priority, COUNT(*) as fact_count
          FROM fact_all_types
          WHERE id_bigint < 10000
          GROUP BY priority
        ) AS fact_agg
        CROSS JOIN (
          SELECT user_segment, COUNT(*) as user_count
          FROM dim_user
          GROUP BY user_segment
        ) AS user_agg
        ORDER BY fact_agg.priority
        LIMIT 15
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(15); // 5 priorities × 3 segments
    });
  });

  describe('Subqueries with Aggregates', () => {
    it('should use subquery with GROUP BY and HAVING', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM (
          SELECT user_id, COUNT(*) as user_count
          FROM fact_all_types
          WHERE id_bigint < 10000
          GROUP BY user_id
          HAVING COUNT(*) > 5
        ) AS active_users
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should compare against aggregate subquery', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types f
        WHERE f.metric_double > (
          SELECT AVG(metric_double) FROM fact_all_types WHERE id_bigint < 100000
        )
        AND f.id_bigint < 10000
      `;
      const result = await duckdbExec(sql);

      const count = Number(result[0].count);
      expect(count).toBeGreaterThan(0);
      expect(count).toBeLessThan(10000);
    });
  });

  describe('Performance', () => {
    it('should execute simple subquery efficiently (< 1s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE user_id IN (
          SELECT user_id FROM dim_user WHERE user_segment = 'enterprise'
        )
        AND id_bigint < 100000
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(1000);
    });

    it('should execute correlated subquery efficiently (< 2s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          u.user_id,
          (SELECT COUNT(*) FROM fact_all_types f WHERE f.user_id = u.user_id AND f.id_bigint < 50000) as count
        FROM dim_user u
        WHERE u.user_id LIKE 'user_%'
        LIMIT 100
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(2000);
    });
  });
});

