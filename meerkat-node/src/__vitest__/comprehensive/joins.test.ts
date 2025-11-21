/**
 * Comprehensive JOIN Tests
 * 
 * Tests JOIN functionality with dimension tables:
 * - Simple joins (fact + single dimension)
 * - Multi-table joins (fact + multiple dimensions)
 * - Joins with aggregates
 * - Joins with filters
 * - Joins with ordering
 * - Performance on 1M+ rows
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';
import { measureExecutionTime } from '../helpers/test-helpers';

describe('Comprehensive: JOINs', () => {
  beforeAll(async () => {
    console.log('🚀 Starting JOIN tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('Simple JOIN: fact + dim_user', () => {
    it('should join fact_all_types with dim_user', async () => {
      const sql = `
        SELECT 
          f.user_id,
          u.user_name,
          u.user_segment,
          COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        GROUP BY f.user_id, u.user_name, u.user_segment
        ORDER BY f.user_id
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      result.forEach((row) => {
        expect(row.user_id).toBeTruthy();
        expect(row.user_name).toBeTruthy();
        expect(row.user_segment).toMatch(/^(enterprise|pro|free)$/);
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should aggregate across join: COUNT by user_segment', async () => {
      const sql = `
        SELECT 
          u.user_segment,
          COUNT(*) as fact_count
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        GROUP BY u.user_segment
        ORDER BY u.user_segment
      `;
      const result = await duckdbExec(sql);

      // Should have 3 segments: enterprise, pro, free
      expect(result.length).toBe(3);

      // Each segment should have roughly 333K rows (1M / 3)
      result.forEach((row) => {
        expect(Number(row.fact_count)).toBeGreaterThan(330000);
        expect(Number(row.fact_count)).toBeLessThan(340000);
      });
    });

    it('should aggregate across join: SUM by user_department', async () => {
      const sql = `
        SELECT 
          u.user_department,
          COUNT(*) as fact_count,
          SUM(f.id_bigint) as total_ids
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        WHERE f.id_bigint < 10000
        GROUP BY u.user_department
        ORDER BY u.user_department
      `;
      const result = await duckdbExec(sql);

      // Should have 4 departments
      expect(result.length).toBe(4);

      result.forEach((row) => {
        expect(row.user_department).toMatch(/^(engineering|product|support|sales)$/);
        expect(Number(row.fact_count)).toBeGreaterThan(0);
        expect(Number(row.total_ids)).toBeGreaterThan(0);
      });
    });
  });

  describe('Simple JOIN: fact + dim_part', () => {
    it('should join fact_all_types with dim_part', async () => {
      const sql = `
        SELECT 
          f.part_id,
          p.part_name,
          p.product_category,
          COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_part p ON f.part_id = p.part_id
        GROUP BY f.part_id, p.part_name, p.product_category
        ORDER BY f.part_id
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      result.forEach((row) => {
        expect(row.part_id).toBeTruthy();
        expect(row.part_name).toBeTruthy();
        expect(row.product_category).toMatch(/^(electronics|furniture|clothing|food|other)$/);
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should aggregate across join: COUNT by product_category', async () => {
      const sql = `
        SELECT 
          p.product_category,
          COUNT(*) as fact_count
        FROM fact_all_types f
        INNER JOIN dim_part p ON f.part_id = p.part_id
        GROUP BY p.product_category
        ORDER BY p.product_category
      `;
      const result = await duckdbExec(sql);

      // Should have 5 categories
      expect(result.length).toBe(5);

      // Each category should have roughly 200K rows (1M / 5)
      result.forEach((row) => {
        expect(Number(row.fact_count)).toBeGreaterThan(190000);
        expect(Number(row.fact_count)).toBeLessThan(210000);
      });
    });

    it('should aggregate across join: AVG price by product_tier', async () => {
      const sql = `
        SELECT 
          p.product_tier,
          COUNT(*) as fact_count,
          AVG(p.price) as avg_price,
          MIN(p.price) as min_price,
          MAX(p.price) as max_price
        FROM fact_all_types f
        INNER JOIN dim_part p ON f.part_id = p.part_id
        GROUP BY p.product_tier
        ORDER BY p.product_tier
      `;
      const result = await duckdbExec(sql);

      // Should have 3 tiers: premium, standard, budget
      expect(result.length).toBe(3);

      result.forEach((row) => {
        expect(row.product_tier).toMatch(/^(premium|standard|budget)$/);
        expect(Number(row.fact_count)).toBeGreaterThan(0);
        expect(Number(row.avg_price)).toBeGreaterThanOrEqual(0);
      });
    });
  });

  describe('Multi-Table JOINs: fact + user + part', () => {
    it('should join fact_all_types with both dim_user and dim_part', async () => {
      const sql = `
        SELECT 
          u.user_segment,
          p.product_category,
          COUNT(*) as fact_count
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        INNER JOIN dim_part p ON f.part_id = p.part_id
        GROUP BY u.user_segment, p.product_category
        ORDER BY u.user_segment, p.product_category
      `;
      const result = await duckdbExec(sql);

      // 3 segments * 5 categories = 15 combinations
      expect(result.length).toBe(15);

      // Each combination should have roughly 66K rows (1M / 15)
      result.forEach((row) => {
        expect(row.user_segment).toMatch(/^(enterprise|pro|free)$/);
        expect(row.product_category).toMatch(/^(electronics|furniture|clothing|food|other)$/);
        expect(Number(row.fact_count)).toBeGreaterThan(60000);
        expect(Number(row.fact_count)).toBeLessThan(72000);
      });
    });

    it('should join with aggregates on all tables', async () => {
      const sql = `
        SELECT 
          u.user_segment,
          p.product_tier,
          COUNT(*) as fact_count,
          AVG(f.metric_double) as avg_metric,
          AVG(p.price) as avg_price
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        INNER JOIN dim_part p ON f.part_id = p.part_id
        WHERE f.id_bigint < 100000
        GROUP BY u.user_segment, p.product_tier
        ORDER BY u.user_segment, p.product_tier
      `;
      const result = await duckdbExec(sql);

      // Should have multiple segment/tier combinations
      // (data distribution depends on modulo patterns)
      expect(result.length).toBeGreaterThanOrEqual(6);
      expect(result.length).toBeLessThanOrEqual(9);

      result.forEach((row) => {
        expect(Number(row.fact_count)).toBeGreaterThan(0);
        expect(Number(row.avg_metric)).toBeGreaterThan(0);
        expect(Number(row.avg_price)).toBeGreaterThanOrEqual(0);
      });
    });
  });

  describe('JOINs with Filters', () => {
    it('should filter on fact table before join', async () => {
      const sql = `
        SELECT 
          u.user_segment,
          COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        WHERE f.priority = 'high'
        GROUP BY u.user_segment
        ORDER BY u.user_segment
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);

      // high priority is 20% of rows, so ~66K per segment
      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(65000);
        expect(Number(row.count)).toBeLessThan(68000);
      });
    });

    it('should filter on dimension table after join', async () => {
      const sql = `
        SELECT 
          u.user_segment,
          COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        WHERE u.is_active_user = true
        GROUP BY u.user_segment
        ORDER BY u.user_segment
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);

      // Active users are 50%, so ~166K per segment
      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(160000);
        expect(Number(row.count)).toBeLessThan(172000);
      });
    });

    it('should filter on both fact and dimension tables', async () => {
      const sql = `
        SELECT 
          p.product_category,
          COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_part p ON f.part_id = p.part_id
        WHERE f.priority = 'high'
          AND p.in_stock = true
        GROUP BY p.product_category
        ORDER BY p.product_category
      `;
      const result = await duckdbExec(sql);

      // Should have at least 1 category (data distribution may vary)
      expect(result.length).toBeGreaterThanOrEqual(1);
      expect(result.length).toBeLessThanOrEqual(5);

      // Each returned category should have some rows
      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(0);
      });

      // Total should be roughly 10% of 1M (high priority 20% * in_stock 50%)
      const total = result.reduce((sum, row) => sum + Number(row.count), 0);
      expect(total).toBeGreaterThan(95000);
      expect(total).toBeLessThan(105000);
    });
  });

  describe('JOINs with Ordering', () => {
    it('should order joined results by dimension field', async () => {
      const sql = `
        SELECT 
          u.user_segment,
          COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        GROUP BY u.user_segment
        ORDER BY u.user_segment ASC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);

      // Alphabetical order: enterprise, free, pro
      expect(result[0].user_segment).toBe('enterprise');
      expect(result[1].user_segment).toBe('free');
      expect(result[2].user_segment).toBe('pro');
    });

    it('should order by aggregate on joined data', async () => {
      const sql = `
        SELECT 
          p.product_category,
          COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_part p ON f.part_id = p.part_id
        GROUP BY p.product_category
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
  });

  describe('JOIN Performance', () => {
    it('should execute simple join quickly (< 1s on 1M rows)', async () => {
      const { result, duration } = await measureExecutionTime(async () => {
        return duckdbExec(`
          SELECT 
            u.user_segment,
            COUNT(*) as count
          FROM fact_all_types f
          INNER JOIN dim_user u ON f.user_id = u.user_id
          GROUP BY u.user_segment
        `);
      });

      expect(result.length).toBe(3);
      expect(duration).toBeLessThan(1000);
    });

    it('should execute multi-table join quickly (< 2s)', async () => {
      const { result, duration } = await measureExecutionTime(async () => {
        return duckdbExec(`
          SELECT 
            u.user_segment,
            p.product_category,
            COUNT(*) as count
          FROM fact_all_types f
          INNER JOIN dim_user u ON f.user_id = u.user_id
          INNER JOIN dim_part p ON f.part_id = p.part_id
          GROUP BY u.user_segment, p.product_category
        `);
      });

      expect(result.length).toBe(15);
      expect(duration).toBeLessThan(2000);
    });
  });

  describe('JOIN Edge Cases', () => {
    it('should handle join with LIMIT', async () => {
      const sql = `
        SELECT 
          f.user_id,
          u.user_name
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        ORDER BY f.id_bigint
        LIMIT 5
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
    });

    it('should handle join with distinct dimension values', async () => {
      const sql = `
        SELECT DISTINCT
          u.user_segment,
          u.user_department
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        ORDER BY u.user_segment, u.user_department
      `;
      const result = await duckdbExec(sql);

      // 3 segments * 4 departments = 12 combinations
      expect(result.length).toBe(12);
    });

    it('should handle LEFT JOIN (all fact rows)', async () => {
      const sql = `
        SELECT 
          COUNT(*) as total_rows,
          COUNT(u.user_id) as joined_rows
        FROM fact_all_types f
        LEFT JOIN dim_user u ON f.user_id = u.user_id
      `;
      const result = await duckdbExec(sql);

      // All rows should join since user_ids exist
      expect(Number(result[0].total_rows)).toBe(1000000);
      expect(Number(result[0].joined_rows)).toBe(1000000);
    });
  });
});

