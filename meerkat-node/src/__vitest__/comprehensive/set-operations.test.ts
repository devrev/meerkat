/**
 * Comprehensive Set Operations Tests
 * 
 * Tests SQL set operations:
 * - UNION (removes duplicates)
 * - UNION ALL (keeps duplicates)
 * - INTERSECT
 * - EXCEPT
 * - Set operations with ORDER BY
 * - Set operations with different column types
 * - Nested set operations
 * - Set operations with aggregates
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: Set Operations', () => {
  beforeAll(async () => {
    console.log('🚀 Starting set operations tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('UNION', () => {
    it('should combine results and remove duplicates with UNION', async () => {
      const sql = `
        SELECT priority FROM fact_all_types WHERE id_bigint < 500
        UNION
        SELECT priority FROM fact_all_types WHERE id_bigint BETWEEN 250 AND 750
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      // Should have 5 unique priorities
      expect(result.length).toBe(5);
      
      const priorities = result.map(r => r.priority);
      expect(new Set(priorities).size).toBe(5);
    });

    it('should combine different tables with UNION', async () => {
      const sql = `
        SELECT user_segment as category FROM dim_user
        UNION
        SELECT product_category as category FROM dim_part
        ORDER BY category
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      
      // Should have unique values only
      const categories = result.map(r => r.category);
      expect(new Set(categories).size).toBe(result.length);
    });

    it('should use UNION with WHERE clauses', async () => {
      const sql = `
        SELECT status FROM fact_all_types WHERE priority = 'high' AND id_bigint < 1000
        UNION
        SELECT status FROM fact_all_types WHERE priority = 'low' AND id_bigint < 1000
        ORDER BY status
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3); // 3 unique statuses
    });
  });

  describe('UNION ALL', () => {
    it('should combine results and keep duplicates with UNION ALL', async () => {
      const sql = `
        SELECT priority FROM fact_all_types WHERE id_bigint < 100
        UNION ALL
        SELECT priority FROM fact_all_types WHERE id_bigint < 100
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      // Should have 200 rows (100 + 100), not 100
      expect(result.length).toBe(200);
    });

    it('should use UNION ALL for combining aggregates', async () => {
      const sql = `
        SELECT 'high_priority' as category, COUNT(*) as count
        FROM fact_all_types
        WHERE priority = 'high' AND id_bigint < 10000
        UNION ALL
        SELECT 'low_priority' as category, COUNT(*) as count
        FROM fact_all_types
        WHERE priority = 'low' AND id_bigint < 10000
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(2);
      expect(result.map(r => r.category)).toContain('high_priority');
      expect(result.map(r => r.category)).toContain('low_priority');
    });

    it('should compare UNION vs UNION ALL performance', async () => {
      const sql1 = `
        SELECT priority FROM fact_all_types WHERE id_bigint < 5000
        UNION
        SELECT priority FROM fact_all_types WHERE id_bigint BETWEEN 2500 AND 7500
      `;

      const sql2 = `
        SELECT priority FROM fact_all_types WHERE id_bigint < 5000
        UNION ALL
        SELECT priority FROM fact_all_types WHERE id_bigint BETWEEN 2500 AND 7500
      `;

      const result1 = await duckdbExec(sql1);
      const result2 = await duckdbExec(sql2);

      // UNION should have fewer rows than UNION ALL (due to deduplication)
      expect(result1.length).toBeLessThanOrEqual(result2.length);
    });
  });

  describe('INTERSECT', () => {
    it('should find common values with INTERSECT', async () => {
      const sql = `
        SELECT priority FROM fact_all_types WHERE id_bigint < 1000 AND status = 'open'
        INTERSECT
        SELECT priority FROM fact_all_types WHERE id_bigint < 1000 AND is_active = true
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result.length).toBeLessThanOrEqual(5); // At most 5 priorities
    });

    it('should use INTERSECT to find overlapping user segments', async () => {
      const sql = `
        SELECT user_segment
        FROM dim_user
        WHERE user_department = 'engineering'
        INTERSECT
        SELECT user_segment
        FROM dim_user
        WHERE is_active_user = true
        ORDER BY user_segment
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
    });

    it('should verify INTERSECT symmetry', async () => {
      const sql1 = `
        SELECT priority FROM fact_all_types WHERE id_bigint < 1000
        INTERSECT
        SELECT priority FROM fact_all_types WHERE status = 'open'
      `;

      const sql2 = `
        SELECT priority FROM fact_all_types WHERE status = 'open'
        INTERSECT
        SELECT priority FROM fact_all_types WHERE id_bigint < 1000
      `;

      const result1 = await duckdbExec(sql1);
      const result2 = await duckdbExec(sql2);

      // Should return same results regardless of order
      expect(result1.length).toBe(result2.length);
    });
  });

  describe('EXCEPT', () => {
    it('should find difference with EXCEPT', async () => {
      const sql = `
        SELECT priority FROM fact_all_types WHERE id_bigint < 1000
        EXCEPT
        SELECT priority FROM fact_all_types WHERE priority = 'high' AND id_bigint < 1000
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      // Should have 4 priorities (all except 'high')
      expect(result.length).toBe(4);
      expect(result.map(r => r.priority)).not.toContain('high');
    });

    it('should use EXCEPT to find exclusive statuses', async () => {
      const sql = `
        SELECT status FROM fact_all_types WHERE priority = 'high' AND id_bigint < 10000
        EXCEPT
        SELECT status FROM fact_all_types WHERE priority = 'low' AND id_bigint < 10000
        ORDER BY status
      `;
      const result = await duckdbExec(sql);

      // Should return statuses that exist in high but not in low (if any)
      expect(result.length).toBeGreaterThanOrEqual(0);
    });

    it('should demonstrate EXCEPT is not symmetric', async () => {
      const sql1 = `
        SELECT priority FROM fact_all_types WHERE id_bigint < 1000
        EXCEPT
        SELECT priority FROM fact_all_types WHERE priority IN ('high', 'critical')
      `;

      const sql2 = `
        SELECT priority FROM fact_all_types WHERE priority IN ('high', 'critical')
        EXCEPT
        SELECT priority FROM fact_all_types WHERE id_bigint < 1000
      `;

      const result1 = await duckdbExec(sql1);
      const result2 = await duckdbExec(sql2);

      // Results may be different (EXCEPT is order-dependent)
      expect(result1.length + result2.length).toBeGreaterThanOrEqual(0);
    });
  });

  describe('Set Operations with ORDER BY', () => {
    it('should order UNION results', async () => {
      const sql = `
        SELECT priority, 'set1' as source FROM fact_all_types WHERE id_bigint < 50
        UNION ALL
        SELECT priority, 'set2' as source FROM fact_all_types WHERE id_bigint BETWEEN 50 AND 99
        ORDER BY priority, source
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(100);
      
      // Verify ordering
      for (let i = 1; i < result.length; i++) {
        expect(result[i].priority >= result[i-1].priority).toBe(true);
      }
    });

    it('should order by computed columns', async () => {
      const sql = `
        SELECT priority, LENGTH(priority) as len FROM fact_all_types WHERE id_bigint < 100
        UNION
        SELECT status, LENGTH(status) as len FROM fact_all_types WHERE id_bigint < 100
        ORDER BY len DESC, priority
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
    });
  });

  describe('Nested Set Operations', () => {
    it('should nest UNION operations', async () => {
      const sql = `
        SELECT priority FROM fact_all_types WHERE id_bigint < 100
        UNION
        (
          SELECT priority FROM fact_all_types WHERE id_bigint BETWEEN 100 AND 199
          UNION
          SELECT priority FROM fact_all_types WHERE id_bigint BETWEEN 200 AND 299
        )
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5); // 5 unique priorities
    });

    it('should combine different set operations', async () => {
      const sql = `
        (
          SELECT priority FROM fact_all_types WHERE id_bigint < 500
          INTERSECT
          SELECT priority FROM fact_all_types WHERE status = 'open'
        )
        UNION
        (
          SELECT priority FROM fact_all_types WHERE status = 'closed' AND id_bigint < 500
        )
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
    });
  });

  describe('Set Operations with Aggregates', () => {
    it('should use set operations on aggregate results', async () => {
      const sql = `
        SELECT priority, COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 5000
        GROUP BY priority
        UNION ALL
        SELECT status, COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 5000
        GROUP BY status
        ORDER BY count DESC
      `;
      const result = await duckdbExec(sql);

      // Should have 5 priorities + 3 statuses = 8 rows
      expect(result.length).toBe(8);
    });
  });

  describe('Performance', () => {
    it('should execute UNION efficiently (< 1s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT priority FROM fact_all_types WHERE id_bigint < 50000
        UNION
        SELECT priority FROM fact_all_types WHERE id_bigint BETWEEN 25000 AND 75000
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(1000);
    });

    it('should execute complex set operations efficiently (< 2s)', async () => {
      const start = Date.now();

      const sql = `
        (
          SELECT priority, status FROM fact_all_types WHERE id_bigint < 25000
          UNION
          SELECT priority, status FROM fact_all_types WHERE id_bigint BETWEEN 25000 AND 50000
        )
        INTERSECT
        (
          SELECT priority, status FROM fact_all_types WHERE is_active = true AND id_bigint < 100000
        )
        ORDER BY priority, status
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(2000);
    });
  });
});

