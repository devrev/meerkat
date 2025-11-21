/**
 * Comprehensive DISTINCT Operations Tests
 * 
 * Tests DISTINCT keyword in various contexts:
 * - SELECT DISTINCT
 * - DISTINCT with single column
 * - DISTINCT with multiple columns
 * - DISTINCT with NULL values
 * - DISTINCT with aggregates (COUNT DISTINCT)
 * - DISTINCT with GROUP BY
 * - DISTINCT with ORDER BY
 * - DISTINCT with JOINs
 * - DISTINCT ALL (opposite of DISTINCT)
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: DISTINCT Operations', () => {
  beforeAll(async () => {
    console.log('🚀 Starting DISTINCT operations tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('SELECT DISTINCT Single Column', () => {
    it('should return distinct priorities', async () => {
      const sql = `
        SELECT DISTINCT priority
        FROM fact_all_types
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      const priorities = result.map((r) => r.priority);
      expect(priorities).toContain('low');
      expect(priorities).toContain('medium');
      expect(priorities).toContain('high');
      expect(priorities).toContain('critical');
      expect(priorities).toContain('urgent');
    });

    it('should return distinct statuses', async () => {
      const sql = `
        SELECT DISTINCT status
        FROM fact_all_types
        ORDER BY status
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);

      const statuses = result.map((r) => r.status);
      expect(statuses).toContain('open');
      expect(statuses).toContain('in_progress');
      expect(statuses).toContain('closed');
    });

    it('should work with DISTINCT on numeric column', async () => {
      const sql = `
        SELECT DISTINCT is_active
        FROM fact_all_types
        ORDER BY is_active
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(2); // true and false

      const values = result.map((r) => r.is_active);
      expect(values).toContain(true);
      expect(values).toContain(false);
    });
  });

  describe('SELECT DISTINCT Multiple Columns', () => {
    it('should return distinct combinations of priority and status', async () => {
      const sql = `
        SELECT DISTINCT priority, status
        FROM fact_all_types
        ORDER BY priority, status
      `;
      const result = await duckdbExec(sql);

      // 5 priorities × 3 statuses = 15 combinations
      expect(result.length).toBe(15);

      result.forEach((row) => {
        expect(row.priority).toBeTruthy();
        expect(row.status).toBeTruthy();
      });
    });

    it('should return distinct combinations of three columns', async () => {
      const sql = `
        SELECT DISTINCT priority, status, is_active
        FROM fact_all_types
        ORDER BY priority, status, is_active
      `;
      const result = await duckdbExec(sql);

      // 5 priorities × 3 statuses × 2 is_active = 30 combinations
      expect(result.length).toBe(30);
    });

    it('should handle DISTINCT with complex column selection', async () => {
      const sql = `
        SELECT DISTINCT 
          priority,
          status,
          EXTRACT(YEAR FROM created_date) as year
        FROM fact_all_types
        WHERE id_bigint < 10000
        ORDER BY year, priority, status
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(row.priority).toBeTruthy();
        expect(row.status).toBeTruthy();
        expect(Number(row.year)).toBeGreaterThan(2019);
      });
    });
  });

  describe('DISTINCT with NULL Values', () => {
    it('should treat NULL as a distinct value', async () => {
      const sql = `
        SELECT DISTINCT resolved_by
        FROM fact_all_types
        WHERE id_bigint < 1000
        ORDER BY resolved_by NULLS FIRST
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      // First result should be NULL (due to NULLS FIRST)
      expect(result[0].resolved_by).toBeNull();
    });

    it('should return distinct values including NULL in combinations', async () => {
      const sql = `
        SELECT DISTINCT priority, resolved_by IS NULL as is_unresolved
        FROM fact_all_types
        WHERE id_bigint < 10000
        ORDER BY priority, is_unresolved
      `;
      const result = await duckdbExec(sql);

      // 5 priorities × 2 (NULL/not NULL) = 10 combinations
      expect(result.length).toBe(10);
    });
  });

  describe('DISTINCT with Aggregates', () => {
    it('should use COUNT(DISTINCT)', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(DISTINCT user_id) as distinct_users,
          COUNT(DISTINCT part_id) as distinct_parts,
          COUNT(*) as total_rows
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.distinct_users)).toBeGreaterThan(0);
        expect(Number(row.distinct_parts)).toBeGreaterThan(0);
        expect(Number(row.total_rows)).toBeGreaterThan(0);
        
        // distinct counts should be less than or equal to total rows
        expect(Number(row.distinct_users)).toBeLessThanOrEqual(Number(row.total_rows));
        expect(Number(row.distinct_parts)).toBeLessThanOrEqual(Number(row.total_rows));
      });
    });

    it('should use SUM(DISTINCT) for unique value sums', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as total_rows,
          SUM(DISTINCT CAST(SUBSTRING(user_id, 6) AS INTEGER)) as sum_distinct_user_nums
        FROM fact_all_types
        WHERE id_bigint < 1000
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.total_rows)).toBeGreaterThan(0);
        expect(Number(row.sum_distinct_user_nums)).toBeGreaterThan(0);
      });
    });

    it('should combine COUNT(*) and COUNT(DISTINCT)', async () => {
      const sql = `
        SELECT 
          status,
          COUNT(*) as total_count,
          COUNT(DISTINCT priority) as distinct_priorities,
          COUNT(DISTINCT user_id) as distinct_users
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY status
        ORDER BY status
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);

      result.forEach((row) => {
        // Should have all 5 priorities in each status
        expect(Number(row.distinct_priorities)).toBe(5);
        expect(Number(row.distinct_users)).toBeGreaterThan(0);
      });
    });
  });

  describe('DISTINCT with GROUP BY', () => {
    it('should use DISTINCT in subquery with GROUP BY in outer query', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as distinct_statuses
        FROM (
          SELECT DISTINCT priority, status
          FROM fact_all_types
        ) AS distinct_combos
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        // Each priority should have 3 distinct statuses
        expect(Number(row.distinct_statuses)).toBe(3);
      });
    });

    it('should avoid need for DISTINCT with proper GROUP BY', async () => {
      const sql1 = `
        SELECT priority, status
        FROM fact_all_types
        GROUP BY priority, status
        ORDER BY priority, status
      `;

      const sql2 = `
        SELECT DISTINCT priority, status
        FROM fact_all_types
        ORDER BY priority, status
      `;

      const result1 = await duckdbExec(sql1);
      const result2 = await duckdbExec(sql2);

      // Both should return the same results
      expect(result1.length).toBe(result2.length);
      expect(result1.length).toBe(15);
    });
  });

  describe('DISTINCT with ORDER BY', () => {
    it('should order DISTINCT results', async () => {
      const sql = `
        SELECT DISTINCT priority
        FROM fact_all_types
        ORDER BY priority DESC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // Verify descending order
      const priorities = result.map((r) => r.priority);
      expect(priorities[0]).toBe('urgent');
      expect(priorities[4]).toBe('critical');
    });

    it('should order DISTINCT multiple columns', async () => {
      const sql = `
        SELECT DISTINCT priority, status
        FROM fact_all_types
        ORDER BY priority ASC, status DESC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(15);

      // Verify first priority has statuses in descending order
      const firstPriority = result[0].priority;
      let lastStatus = 'zzz';
      
      for (const row of result) {
        if (row.priority === firstPriority) {
          expect(row.status <= lastStatus).toBe(true);
          lastStatus = row.status;
        } else {
          break;
        }
      }
    });

    it('should use ORDER BY with expression on DISTINCT results', async () => {
      const sql = `
        SELECT DISTINCT 
          priority,
          LENGTH(priority) as priority_length
        FROM fact_all_types
        ORDER BY priority_length DESC, priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // Verify descending order by length
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].priority_length)).toBeLessThanOrEqual(
          Number(result[i - 1].priority_length)
        );
      }
    });
  });

  describe('DISTINCT with JOINs', () => {
    it('should use DISTINCT with INNER JOIN', async () => {
      const sql = `
        SELECT DISTINCT 
          f.priority,
          u.user_segment
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        WHERE f.id_bigint < 10000
        ORDER BY f.priority, u.user_segment
      `;
      const result = await duckdbExec(sql);

      // 5 priorities × 3 segments = 15 combinations
      expect(result.length).toBe(15);
    });

    it('should use DISTINCT on joined columns', async () => {
      const sql = `
        SELECT DISTINCT u.user_segment
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        WHERE f.priority = 'high'
        ORDER BY u.user_segment
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);

      const segments = result.map((r) => r.user_segment);
      expect(segments).toContain('enterprise');
      expect(segments).toContain('pro');
      expect(segments).toContain('free');
    });

    it('should combine DISTINCT with multi-table JOIN', async () => {
      const sql = `
        SELECT DISTINCT 
          u.user_segment,
          p.product_category
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        INNER JOIN dim_part p ON f.part_id = p.part_id
        WHERE f.id_bigint < 10000
        ORDER BY u.user_segment, p.product_category
      `;
      const result = await duckdbExec(sql);

      // 3 segments × 5 categories = 15 combinations
      expect(result.length).toBe(15);
    });
  });

  describe('DISTINCT with WHERE', () => {
    it('should apply filter before DISTINCT', async () => {
      const sql = `
        SELECT DISTINCT status
        FROM fact_all_types
        WHERE priority = 'high' AND id_bigint < 10000
        ORDER BY status
      `;
      const result = await duckdbExec(sql);

      // Even with filter, should still have all 3 statuses
      expect(result.length).toBe(3);
    });

    it('should use DISTINCT with complex WHERE', async () => {
      const sql = `
        SELECT DISTINCT priority, is_active
        FROM fact_all_types
        WHERE metric_double > 500 
          AND created_date >= DATE '2020-06-01'
          AND id_bigint < 10000
        ORDER BY priority, is_active
      `;
      const result = await duckdbExec(sql);

      // Should have combinations that match the filter
      expect(result.length).toBeGreaterThan(0);
      expect(result.length).toBeLessThanOrEqual(10); // 5 priorities × 2 is_active values
    });
  });

  describe('DISTINCT with LIMIT', () => {
    it('should limit DISTINCT results', async () => {
      const sql = `
        SELECT DISTINCT user_id
        FROM fact_all_types
        ORDER BY user_id
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      // Verify they're in order
      for (let i = 1; i < result.length; i++) {
        expect(result[i].user_id >= result[i - 1].user_id).toBe(true);
      }
    });

    it('should use DISTINCT with LIMIT and OFFSET', async () => {
      const sql1 = `
        SELECT DISTINCT user_id
        FROM fact_all_types
        ORDER BY user_id
        LIMIT 5 OFFSET 0
      `;

      const sql2 = `
        SELECT DISTINCT user_id
        FROM fact_all_types
        ORDER BY user_id
        LIMIT 5 OFFSET 5
      `;

      const result1 = await duckdbExec(sql1);
      const result2 = await duckdbExec(sql2);

      expect(result1.length).toBe(5);
      expect(result2.length).toBe(5);

      // Second batch should start after first batch
      expect(result2[0].user_id > result1[4].user_id).toBe(true);
    });
  });

  describe('DISTINCT Performance Comparisons', () => {
    it('should compare performance: DISTINCT vs GROUP BY', async () => {
      const startDistinct = Date.now();
      const sqlDistinct = `
        SELECT DISTINCT priority, status
        FROM fact_all_types
        WHERE id_bigint < 100000
      `;
      await duckdbExec(sqlDistinct);
      const durationDistinct = Date.now() - startDistinct;

      const startGroupBy = Date.now();
      const sqlGroupBy = `
        SELECT priority, status
        FROM fact_all_types
        WHERE id_bigint < 100000
        GROUP BY priority, status
      `;
      await duckdbExec(sqlGroupBy);
      const durationGroupBy = Date.now() - startGroupBy;

      // Both should complete quickly
      expect(durationDistinct).toBeLessThan(1000);
      expect(durationGroupBy).toBeLessThan(1000);
    });

    it('should execute DISTINCT on large dataset efficiently (< 1s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT DISTINCT user_id, part_id
        FROM fact_all_types
        WHERE id_bigint < 100000
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(1000);
    });
  });

  describe('Complex DISTINCT Scenarios', () => {
    it('should use DISTINCT in UNION (redundant but valid)', async () => {
      const sql = `
        SELECT DISTINCT priority FROM fact_all_types WHERE id_bigint < 1000
        UNION
        SELECT DISTINCT priority FROM fact_all_types WHERE id_bigint BETWEEN 1000 AND 2000
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
    });

    it('should use DISTINCT with CASE expressions', async () => {
      const sql = `
        SELECT DISTINCT
          CASE 
            WHEN priority IN ('high', 'critical', 'urgent') THEN 'Important'
            ELSE 'Not Important'
          END as importance_level
        FROM fact_all_types
        ORDER BY importance_level
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(2);
      
      const levels = result.map((r) => r.importance_level);
      expect(levels).toContain('Important');
      expect(levels).toContain('Not Important');
    });

    it('should use DISTINCT with window functions in subquery', async () => {
      const sql = `
        SELECT DISTINCT priority, row_num_category
        FROM (
          SELECT 
            priority,
            CASE 
              WHEN ROW_NUMBER() OVER (PARTITION BY priority ORDER BY id_bigint) <= 10 THEN 'Top 10'
              ELSE 'Others'
            END as row_num_category
          FROM fact_all_types
          WHERE id_bigint < 1000
        ) AS ranked
        ORDER BY priority, row_num_category
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10); // 5 priorities × 2 categories
    });
  });
});

