/**
 * Comprehensive Window Functions Tests
 * 
 * Tests window function support in queries:
 * - ROW_NUMBER()
 * - RANK() / DENSE_RANK()
 * - NTILE()
 * - LEAD() / LAG()
 * - FIRST_VALUE() / LAST_VALUE()
 * - Aggregate window functions (SUM OVER, AVG OVER)
 * - PARTITION BY
 * - ORDER BY within windows
 * - Window frames (ROWS BETWEEN, RANGE BETWEEN)
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: Window Functions', () => {
  beforeAll(async () => {
    console.log('🚀 Starting window function tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('ROW_NUMBER()', () => {
    it('should use ROW_NUMBER() without PARTITION BY', async () => {
      const sql = `
        SELECT 
          priority,
          status,
          ROW_NUMBER() OVER (ORDER BY id_bigint) as row_num
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      // Row numbers should be 1, 2, 3, ..., 10
      result.forEach((row, index) => {
        expect(Number(row.row_num)).toBe(index + 1);
      });
    });

    it('should use ROW_NUMBER() with PARTITION BY', async () => {
      const sql = `
        SELECT 
          priority,
          id_bigint,
          ROW_NUMBER() OVER (PARTITION BY priority ORDER BY id_bigint) as row_num_per_priority
        FROM fact_all_types
        WHERE id_bigint < 25
        ORDER BY priority, id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(25);

      // Within each priority, row_num should restart at 1
      let lastPriority = null;
      let expectedRowNum = 1;

      result.forEach((row) => {
        if (lastPriority !== row.priority) {
          expectedRowNum = 1;
          lastPriority = row.priority;
        }
        expect(Number(row.row_num_per_priority)).toBe(expectedRowNum);
        expectedRowNum++;
      });
    });

    it('should filter using ROW_NUMBER() in subquery', async () => {
      const sql = `
        SELECT *
        FROM (
          SELECT 
            priority,
            id_bigint,
            ROW_NUMBER() OVER (PARTITION BY priority ORDER BY id_bigint) as rn
          FROM fact_all_types
          WHERE id_bigint < 1000
        ) AS numbered
        WHERE rn <= 10
        ORDER BY priority, id_bigint
      `;
      const result = await duckdbExec(sql);

      // 10 rows per priority * 5 priorities = 50 rows
      expect(result.length).toBe(50);
    });
  });

  describe('RANK() and DENSE_RANK()', () => {
    it('should use RANK() to handle ties', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as count,
          RANK() OVER (ORDER BY COUNT(*) DESC) as rank
        FROM fact_all_types
        GROUP BY priority
        ORDER BY rank
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // Ranks should be assigned (with possible gaps if there are ties)
      result.forEach((row, index) => {
        expect(Number(row.rank)).toBeGreaterThanOrEqual(index + 1);
      });
    });

    it('should use DENSE_RANK() to handle ties without gaps', async () => {
      const sql = `
        SELECT 
          status,
          COUNT(*) as count,
          DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) as dense_rank
        FROM fact_all_types
        GROUP BY status
        ORDER BY dense_rank
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      // Dense ranks should be consecutive without gaps
      result.forEach((row, index) => {
        expect(Number(row.dense_rank)).toBeLessThanOrEqual(index + 1);
      });
    });

    it('should compare RANK() vs DENSE_RANK()', async () => {
      const sql = `
        SELECT 
          priority,
          metric_double,
          RANK() OVER (ORDER BY metric_double DESC) as rank,
          DENSE_RANK() OVER (ORDER BY metric_double DESC) as dense_rank
        FROM (
          SELECT DISTINCT priority, metric_double 
          FROM fact_all_types 
          WHERE id_bigint < 100
        ) AS distinct_vals
        ORDER BY metric_double DESC
        LIMIT 20
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(Number(row.dense_rank)).toBeLessThanOrEqual(Number(row.rank));
      });
    });
  });

  describe('LEAD() and LAG()', () => {
    it('should use LAG() to get previous row value', async () => {
      const sql = `
        SELECT 
          id_bigint,
          priority,
          LAG(priority) OVER (ORDER BY id_bigint) as prev_priority
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      // First row should have NULL for prev_priority
      expect(result[0].prev_priority).toBeNull();

      // Each subsequent row's prev_priority should match previous row's priority
      for (let i = 1; i < result.length; i++) {
        expect(result[i].prev_priority).toBe(result[i - 1].priority);
      }
    });

    it('should use LEAD() to get next row value', async () => {
      const sql = `
        SELECT 
          id_bigint,
          status,
          LEAD(status) OVER (ORDER BY id_bigint) as next_status
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      // Last row should have NULL for next_status
      expect(result[result.length - 1].next_status).toBeNull();

      // Each row's next_status should match next row's status
      for (let i = 0; i < result.length - 1; i++) {
        expect(result[i].next_status).toBe(result[i + 1].status);
      }
    });

    it('should use LAG() with offset and default', async () => {
      const sql = `
        SELECT 
          id_bigint,
          metric_double,
          LAG(metric_double, 2, 0.0) OVER (ORDER BY id_bigint) as lag_2_rows
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      // First 2 rows should have default value (0.0)
      expect(Number(result[0].lag_2_rows)).toBe(0);
      expect(Number(result[1].lag_2_rows)).toBe(0);

      // Row 2 should have row 0's value
      expect(Number(result[2].lag_2_rows)).toBe(Number(result[0].metric_double));
    });

    it('should use LEAD() with PARTITION BY', async () => {
      const sql = `
        SELECT 
          priority,
          id_bigint,
          LEAD(id_bigint) OVER (PARTITION BY priority ORDER BY id_bigint) as next_id_in_priority
        FROM fact_all_types
        WHERE id_bigint < 50
        ORDER BY priority, id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(50);

      // Verify LEAD resets per partition
      let lastPriority = null;
      result.forEach((row, index) => {
        if (lastPriority !== row.priority) {
          lastPriority = row.priority;
        }

        // If not the last row in this partition and there's a next row with same priority
        const nextRow = result[index + 1];
        if (nextRow && nextRow.priority === row.priority) {
          expect(Number(row.next_id_in_priority)).toBe(Number(nextRow.id_bigint));
        }
      });
    });
  });

  describe('FIRST_VALUE() and LAST_VALUE()', () => {
    it('should use FIRST_VALUE()', async () => {
      const sql = `
        SELECT 
          priority,
          id_bigint,
          FIRST_VALUE(id_bigint) OVER (PARTITION BY priority ORDER BY id_bigint) as first_id
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY priority, id_bigint
        LIMIT 20
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(20);

      // Within each priority, first_id should be the same
      let lastPriority = null;
      let firstIdInPartition = null;

      result.forEach((row) => {
        if (lastPriority !== row.priority) {
          firstIdInPartition = Number(row.first_id);
          lastPriority = row.priority;
        }
        expect(Number(row.first_id)).toBe(firstIdInPartition);
      });
    });

    it('should use LAST_VALUE() with proper frame', async () => {
      const sql = `
        SELECT 
          priority,
          id_bigint,
          LAST_VALUE(id_bigint) OVER (
            PARTITION BY priority 
            ORDER BY id_bigint 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) as last_id
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY priority, id_bigint
        LIMIT 20
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(20);

      // Within each priority, last_id should be the same (largest value)
      let lastPriority = null;
      let lastIdInPartition = null;

      result.forEach((row) => {
        if (lastPriority !== row.priority) {
          lastIdInPartition = Number(row.last_id);
          lastPriority = row.priority;
        }
        expect(Number(row.last_id)).toBe(lastIdInPartition);
      });
    });
  });

  describe('Aggregate Window Functions', () => {
    it('should use SUM() OVER as running total', async () => {
      const sql = `
        SELECT 
          id_bigint,
          id_bigint as value,
          SUM(id_bigint) OVER (ORDER BY id_bigint ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      // Verify running total
      let expectedTotal = 0;
      result.forEach((row) => {
        expectedTotal += Number(row.value);
        expect(Number(row.running_total)).toBe(expectedTotal);
      });
    });

    it('should use AVG() OVER with PARTITION BY', async () => {
      const sql = `
        SELECT 
          priority,
          metric_double,
          AVG(metric_double) OVER (PARTITION BY priority) as avg_metric_per_priority
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY priority, id_bigint
        LIMIT 20
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(20);

      // Within each priority, avg should be the same
      let lastPriority = null;
      let avgForPartition = null;

      result.forEach((row) => {
        if (lastPriority !== row.priority) {
          avgForPartition = Number(row.avg_metric_per_priority);
          lastPriority = row.priority;
        }
        expect(Number(row.avg_metric_per_priority)).toBeCloseTo(avgForPartition, 5);
      });
    });

    it('should use COUNT() OVER with moving window', async () => {
      const sql = `
        SELECT 
          id_bigint,
          COUNT(*) OVER (
            ORDER BY id_bigint 
            ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
          ) as count_in_window
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      // First few and last few rows should have smaller windows
      expect(Number(result[0].count_in_window)).toBeLessThanOrEqual(3); // 0, 1, 2
      expect(Number(result[1].count_in_window)).toBeLessThanOrEqual(4); // 0, 1, 2, 3
      expect(Number(result[4].count_in_window)).toBe(5); // 2, 3, 4, 5, 6 (full window)
    });
  });

  describe('NTILE()', () => {
    it('should use NTILE() to divide into quartiles', async () => {
      const sql = `
        SELECT 
          id_bigint,
          NTILE(4) OVER (ORDER BY id_bigint) as quartile
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(100);

      // Check that we have 4 quartiles
      const quartiles = new Set(result.map((r) => Number(r.quartile)));
      expect(quartiles.size).toBe(4);
      expect(quartiles.has(1)).toBe(true);
      expect(quartiles.has(2)).toBe(true);
      expect(quartiles.has(3)).toBe(true);
      expect(quartiles.has(4)).toBe(true);

      // Each quartile should have approximately 25 rows
      [1, 2, 3, 4].forEach((q) => {
        const countInQuartile = result.filter((r) => Number(r.quartile) === q).length;
        expect(countInQuartile).toBeGreaterThanOrEqual(24);
        expect(countInQuartile).toBeLessThanOrEqual(26);
      });
    });

    it('should use NTILE() with PARTITION BY', async () => {
      const sql = `
        SELECT 
          priority,
          id_bigint,
          NTILE(3) OVER (PARTITION BY priority ORDER BY id_bigint) as tercile
        FROM fact_all_types
        WHERE id_bigint < 300
        ORDER BY priority, id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(300);

      // Within each priority, should have 3 terciles
      const priorityGroups = {};
      result.forEach((row) => {
        if (!priorityGroups[row.priority]) {
          priorityGroups[row.priority] = new Set();
        }
        priorityGroups[row.priority].add(Number(row.tercile));
      });

      Object.values(priorityGroups).forEach((terciles: Set<number>) => {
        expect(terciles.size).toBe(3);
      });
    });
  });

  describe('Complex Window Scenarios', () => {
    it('should use multiple window functions in same query', async () => {
      const sql = `
        SELECT 
          priority,
          id_bigint,
          ROW_NUMBER() OVER (PARTITION BY priority ORDER BY id_bigint) as row_num,
          RANK() OVER (PARTITION BY priority ORDER BY id_bigint) as rank,
          LAG(id_bigint) OVER (PARTITION BY priority ORDER BY id_bigint) as prev_id
        FROM fact_all_types
        WHERE id_bigint < 50
        ORDER BY priority, id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(50);

      result.forEach((row) => {
        expect(Number(row.row_num)).toBeGreaterThan(0);
        expect(Number(row.rank)).toBeGreaterThan(0);
      });
    });

    it('should use window functions with aggregates and GROUP BY', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as count,
          RANK() OVER (ORDER BY COUNT(*) DESC) as rank_by_count
        FROM fact_all_types
        WHERE id_bigint < 1000
        GROUP BY priority
        ORDER BY rank_by_count
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // Verify descending order by count
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].count)).toBeLessThanOrEqual(Number(result[i - 1].count));
      }
    });

    it('should use window functions with JOINs', async () => {
      const sql = `
        SELECT 
          f.priority,
          u.user_segment,
          COUNT(*) as count,
          RANK() OVER (PARTITION BY u.user_segment ORDER BY COUNT(*) DESC) as rank_in_segment
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        WHERE f.id_bigint < 10000
        GROUP BY f.priority, u.user_segment
        ORDER BY u.user_segment, rank_in_segment
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      // Each segment should have priorities ranked
      let lastSegment = null;
      let lastRank = 0;

      result.forEach((row) => {
        if (lastSegment !== row.user_segment) {
          lastSegment = row.user_segment;
          lastRank = 0;
        }
        expect(Number(row.rank_in_segment)).toBeGreaterThanOrEqual(lastRank);
        lastRank = Number(row.rank_in_segment);
      });
    });
  });

  describe('Performance', () => {
    it('should execute window functions efficiently (< 1s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          priority,
          id_bigint,
          ROW_NUMBER() OVER (PARTITION BY priority ORDER BY id_bigint) as row_num,
          SUM(id_bigint) OVER (PARTITION BY priority ORDER BY id_bigint) as running_sum
        FROM fact_all_types
        WHERE id_bigint < 10000
        ORDER BY priority, id_bigint
        LIMIT 100
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(1000);
    });
  });
});

