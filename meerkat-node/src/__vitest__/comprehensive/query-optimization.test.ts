/**
 * Comprehensive Query Optimization Tests
 * 
 * Tests query optimization and performance:
 * - Query planning with EXPLAIN
 * - Index usage patterns
 * - Large result set handling
 * - Memory-intensive operations
 * - Query complexity limits
 * - Materialization strategies
 * - Join order optimization
 * - Filter pushdown
 * - Projection pushdown
 * - Performance benchmarks
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: Query Optimization', () => {
  beforeAll(async () => {
    console.log('🚀 Starting query optimization tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('EXPLAIN Query Planning', () => {
    it('should generate query plan with EXPLAIN', async () => {
      const sql = `
        EXPLAIN 
        SELECT COUNT(*) 
        FROM fact_all_types 
        WHERE priority = 'high'
      `;
      const result = await duckdbExec(sql);

      expect(result).toBeDefined();
      expect(result.length).toBeGreaterThan(0);
    });

    it('should show JOIN optimization in EXPLAIN', async () => {
      const sql = `
        EXPLAIN
        SELECT f.priority, u.user_segment, COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        WHERE f.id_bigint < 10000
        GROUP BY f.priority, u.user_segment
      `;
      const result = await duckdbExec(sql);

      expect(result).toBeDefined();
    });

    it('should analyze complex query plan', async () => {
      const sql = `
        EXPLAIN ANALYZE
        SELECT 
          f.priority,
          COUNT(*) as count,
          AVG(f.metric_double) as avg_metric
        FROM fact_all_types f
        WHERE f.id_bigint < 50000
          AND f.is_active = true
        GROUP BY f.priority
        HAVING COUNT(*) > 100
        ORDER BY count DESC
      `;
      const result = await duckdbExec(sql);

      expect(result).toBeDefined();
    });
  });

  describe('Large Result Set Handling', () => {
    it('should handle 100K row result set efficiently (< 2s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          id_bigint,
          priority,
          status,
          metric_double
        FROM fact_all_types
        WHERE id_bigint < 100000
        ORDER BY id_bigint
      `;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(result.length).toBe(100000);
      expect(duration).toBeLessThan(2000);
    });

    it('should handle 500K row aggregation efficiently (< 3s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          priority,
          status,
          COUNT(*) as count,
          AVG(metric_double) as avg_metric,
          SUM(metric_numeric) as sum_numeric
        FROM fact_all_types
        WHERE id_bigint < 500000
        GROUP BY priority, status
      `;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(result.length).toBeGreaterThan(0);
      expect(duration).toBeLessThan(3000);
    });

    it('should handle full 1M row scan efficiently (< 5s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          COUNT(*) as total_count,
          SUM(metric_double) as total_metric,
          AVG(metric_numeric) as avg_numeric
        FROM fact_all_types
      `;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(Number(result[0].total_count)).toBe(1000000);
      expect(duration).toBeLessThan(5000);
    });
  });

  describe('Memory-Intensive Operations', () => {
    it('should handle large GROUP BY with many groups (< 3s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          user_id,
          priority,
          status,
          COUNT(*) as count,
          AVG(metric_double) as avg_metric
        FROM fact_all_types
        WHERE id_bigint < 100000
        GROUP BY user_id, priority, status
        ORDER BY count DESC
        LIMIT 100
      `;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(result.length).toBe(100);
      expect(duration).toBeLessThan(3000);
    });

    it('should handle large JOIN operation (< 5s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          u.user_segment,
          p.product_category,
          COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        INNER JOIN dim_part p ON f.part_id = p.part_id
        WHERE f.id_bigint < 500000
        GROUP BY u.user_segment, p.product_category
      `;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(result.length).toBeGreaterThan(0);
      expect(duration).toBeLessThan(5000);
    });

    it('should handle large window function operation (< 5s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          id_bigint,
          priority,
          ROW_NUMBER() OVER (PARTITION BY priority ORDER BY id_bigint) as row_num,
          AVG(metric_double) OVER (PARTITION BY priority) as avg_metric
        FROM fact_all_types
        WHERE id_bigint < 100000
        ORDER BY id_bigint
        LIMIT 1000
      `;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(result.length).toBe(1000);
      expect(duration).toBeLessThan(5000);
    });
  });

  describe('Filter Pushdown Optimization', () => {
    it('should push filters before JOIN', async () => {
      const start = Date.now();

      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        WHERE f.priority = 'high'
          AND u.user_segment = 'enterprise'
          AND f.id_bigint < 100000
      `;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(Number(result[0].count)).toBeGreaterThan(0);
      expect(duration).toBeLessThan(2000);
    });

    it('should optimize multiple filter conditions', async () => {
      const start = Date.now();

      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE priority IN ('high', 'critical', 'urgent')
          AND status = 'open'
          AND is_active = true
          AND id_bigint < 500000
      `;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(result[0].count).toBeDefined();
      expect(duration).toBeLessThan(1000);
    });
  });

  describe('Projection Pushdown', () => {
    it('should optimize column selection (< 1s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT priority, status
        FROM fact_all_types
        WHERE id_bigint < 500000
        LIMIT 10000
      `;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(result.length).toBe(10000);
      expect(duration).toBeLessThan(1000);
    });

    it('should compare full vs partial column selection', async () => {
      const start1 = Date.now();
      await duckdbExec(`SELECT * FROM fact_all_types WHERE id_bigint < 10000 LIMIT 1000`);
      const duration1 = Date.now() - start1;

      const start2 = Date.now();
      await duckdbExec(`SELECT id_bigint, priority FROM fact_all_types WHERE id_bigint < 10000 LIMIT 1000`);
      const duration2 = Date.now() - start2;

      // Partial selection should be faster or equal
      expect(duration2).toBeLessThanOrEqual(duration1 + 100); // Allow 100ms tolerance
    });
  });

  describe('Join Order Optimization', () => {
    it('should optimize small-to-large JOIN order', async () => {
      const start = Date.now();

      const sql = `
        SELECT COUNT(*) as count
        FROM dim_user u
        INNER JOIN fact_all_types f ON u.user_id = f.user_id
        WHERE u.user_segment = 'enterprise'
          AND f.id_bigint < 100000
      `;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(Number(result[0].count)).toBeGreaterThan(0);
      expect(duration).toBeLessThan(2000);
    });

    it('should handle multi-table JOIN optimization', async () => {
      const start = Date.now();

      const sql = `
        SELECT COUNT(*) as count
        FROM dim_user u
        INNER JOIN fact_all_types f ON u.user_id = f.user_id
        INNER JOIN dim_part p ON f.part_id = p.part_id
        WHERE u.user_segment = 'pro'
          AND p.product_category = 'frontend'
          AND f.id_bigint < 100000
      `;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(result[0].count).toBeDefined();
      expect(duration).toBeLessThan(3000);
    });
  });

  describe('Aggregate Optimization', () => {
    it('should optimize simple COUNT(*) (< 500ms)', async () => {
      const start = Date.now();

      const sql = `SELECT COUNT(*) as count FROM fact_all_types`;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(Number(result[0].count)).toBe(1000000);
      expect(duration).toBeLessThan(500);
    });

    it('should optimize grouped aggregates with filter (< 2s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          priority,
          COUNT(*) as count,
          AVG(metric_double) as avg_metric,
          MIN(metric_numeric) as min_metric,
          MAX(metric_numeric) as max_metric
        FROM fact_all_types
        WHERE id_bigint < 500000
        GROUP BY priority
      `;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(result.length).toBe(5);
      expect(duration).toBeLessThan(2000);
    });
  });

  describe('Subquery Optimization', () => {
    it('should optimize correlated subquery (< 2s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          u.user_id,
          (SELECT COUNT(*) FROM fact_all_types f WHERE f.user_id = u.user_id AND f.id_bigint < 10000) as fact_count
        FROM dim_user u
        WHERE u.user_id LIKE 'user_%'
        LIMIT 100
      `;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(result.length).toBe(100);
      expect(duration).toBeLessThan(2000);
    });

    it('should optimize IN subquery (< 2s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE user_id IN (SELECT user_id FROM dim_user WHERE user_segment = 'enterprise')
          AND id_bigint < 100000
      `;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(Number(result[0].count)).toBeGreaterThan(0);
      expect(duration).toBeLessThan(2000);
    });
  });

  describe('Performance Benchmarks', () => {
    it('should benchmark simple SELECT performance', async () => {
      const iterations = 5;
      const durations = [];

      for (let i = 0; i < iterations; i++) {
        const start = Date.now();
        await duckdbExec(`SELECT COUNT(*) FROM fact_all_types WHERE id_bigint < 100000`);
        durations.push(Date.now() - start);
      }

      const avgDuration = durations.reduce((a, b) => a + b) / iterations;
      expect(avgDuration).toBeLessThan(1000);
    });

    it('should benchmark JOIN performance', async () => {
      const iterations = 3;
      const durations = [];

      for (let i = 0; i < iterations; i++) {
        const start = Date.now();
        await duckdbExec(`
          SELECT COUNT(*) 
          FROM fact_all_types f 
          INNER JOIN dim_user u ON f.user_id = u.user_id 
          WHERE f.id_bigint < 50000
        `);
        durations.push(Date.now() - start);
      }

      const avgDuration = durations.reduce((a, b) => a + b) / iterations;
      expect(avgDuration).toBeLessThan(2000);
    });

    it('should benchmark aggregate performance', async () => {
      const iterations = 3;
      const durations = [];

      for (let i = 0; i < iterations; i++) {
        const start = Date.now();
        await duckdbExec(`
          SELECT priority, COUNT(*), AVG(metric_double) 
          FROM fact_all_types 
          WHERE id_bigint < 100000
          GROUP BY priority
        `);
        durations.push(Date.now() - start);
      }

      const avgDuration = durations.reduce((a, b) => a + b) / iterations;
      expect(avgDuration).toBeLessThan(1500);
    });
  });
});

