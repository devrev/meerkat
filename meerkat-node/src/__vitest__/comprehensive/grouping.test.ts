/**
 * Comprehensive Grouping Tests
 * 
 * Tests GROUP BY functionality with various scenarios:
 * - Single dimension grouping
 * - Multi-dimension (composite) grouping
 * - Grouping with aggregates
 * - Grouping with filters
 * - Grouping with ordering
 * - Performance on large datasets
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { cubeQueryToSQL } from '../../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';
import { FACT_ALL_TYPES_SCHEMA } from '../synthetic/table-schemas';
import { measureExecutionTime } from '../helpers/test-helpers';

describe('Comprehensive: Grouping', () => {
  beforeAll(async () => {
    console.log('🚀 Starting Grouping tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('Single Dimension Grouping', () => {
    it('should group by single string dimension (priority)', async () => {
      const sql = `
        SELECT priority, COUNT(*) as count
        FROM fact_all_types
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // Each priority should have 200K rows (i % 5)
      result.forEach((row) => {
        expect(Number(row.count)).toBe(200000);
      });

      // Verify all priorities are present
      const priorities = result.map((r) => r.priority);
      expect(priorities).toContain('high');
      expect(priorities).toContain('medium');
      expect(priorities).toContain('low');
      expect(priorities).toContain('critical');
      expect(priorities).toContain('unknown');
    });

    it('should group by status dimension', async () => {
      const sql = `
        SELECT status, COUNT(*) as count
        FROM fact_all_types
        GROUP BY status
        ORDER BY status
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(4);

      // Each status should have 250K rows (i % 4)
      result.forEach((row) => {
        expect(Number(row.count)).toBe(250000);
      });

      // Verify all statuses are present
      const statuses = result.map((r) => r.status);
      expect(statuses).toEqual(['closed', 'in_progress', 'open', 'resolved']);
    });

    it('should group by boolean dimension', async () => {
      const sql = `
        SELECT is_active, COUNT(*) as count
        FROM fact_all_types
        GROUP BY is_active
        ORDER BY is_active
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(2);

      // Each boolean value should have 500K rows (i % 2)
      expect(result[0].is_active).toBe(false);
      expect(Number(result[0].count)).toBe(500000);
      expect(result[1].is_active).toBe(true);
      expect(Number(result[1].count)).toBe(500000);
    });

    it('should group by date dimension', async () => {
      const sql = `
        SELECT record_date, COUNT(*) as count
        FROM fact_all_types
        GROUP BY record_date
        ORDER BY record_date
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      // Each date should appear ~685 times (1M / 1460)
      result.forEach((row) => {
        const count = Number(row.count);
        expect(count).toBeGreaterThan(680);
        expect(count).toBeLessThan(690);
      });

      // First date should be 2020-01-01
      expect(result[0].record_date).toBe('2020-01-01');
    });

    it('should group by numeric range (bucketing)', async () => {
      const sql = `
        SELECT 
          CASE 
            WHEN id_bigint < 250000 THEN '0-250K'
            WHEN id_bigint < 500000 THEN '250K-500K'
            WHEN id_bigint < 750000 THEN '500K-750K'
            ELSE '750K-1M'
          END as bucket,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY bucket
        ORDER BY bucket
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(4);

      // Each bucket should have 250K rows
      result.forEach((row) => {
        expect(Number(row.count)).toBe(250000);
      });
    });
  });

  describe('Multi-Dimension (Composite) Grouping', () => {
    it('should group by two dimensions: priority + status', async () => {
      const sql = `
        SELECT priority, status, COUNT(*) as count
        FROM fact_all_types
        GROUP BY priority, status
        ORDER BY priority, status
      `;
      const result = await duckdbExec(sql);

      // 5 priorities * 4 statuses = 20 combinations
      expect(result.length).toBe(20);

      // Each combination should have 50K rows (1M / 20)
      result.forEach((row) => {
        expect(Number(row.count)).toBe(50000);
      });
    });

    it('should group by three dimensions: priority + status + is_active', async () => {
      const sql = `
        SELECT priority, status, is_active, COUNT(*) as count
        FROM fact_all_types
        GROUP BY priority, status, is_active
        ORDER BY priority, status, is_active
      `;
      const result = await duckdbExec(sql);

      // 5 * 4 * 2 = 40 combinations
      expect(result.length).toBe(40);

      // Each combination should have 25K rows (1M / 40)
      result.forEach((row) => {
        expect(Number(row.count)).toBe(25000);
      });
    });

    it('should group by string + date dimensions', async () => {
      const sql = `
        SELECT priority, record_date, COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 1000
        GROUP BY priority, record_date
        ORDER BY priority, record_date
        LIMIT 20
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(20);

      // Verify grouping structure
      result.forEach((row) => {
        expect(row.priority).toBeTruthy();
        expect(row.record_date).toBeTruthy();
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should group by string + boolean dimensions', async () => {
      const sql = `
        SELECT priority, is_deleted, COUNT(*) as count
        FROM fact_all_types
        GROUP BY priority, is_deleted
        ORDER BY priority, is_deleted
      `;
      const result = await duckdbExec(sql);

      // 5 priorities * 2 boolean values = 10 combinations
      expect(result.length).toBe(10);

      result.forEach((row) => {
        expect(row.priority).toBeTruthy();
        expect(typeof row.is_deleted).toBe('boolean');
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });
  });

  describe('Grouping with Multiple Aggregates', () => {
    it('should group with COUNT, SUM, AVG', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as count,
          SUM(id_bigint) as total,
          AVG(id_bigint) as average
        FROM fact_all_types
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.count)).toBe(200000);
        expect(Number(row.total)).toBeGreaterThan(0);
        expect(Number(row.average)).toBeGreaterThan(0);
      });
    });

    it('should group with MIN and MAX', async () => {
      const sql = `
        SELECT 
          priority,
          MIN(id_bigint) as minimum,
          MAX(id_bigint) as maximum,
          MAX(id_bigint) - MIN(id_bigint) as range
        FROM fact_all_types
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.minimum)).toBeGreaterThanOrEqual(0);
        expect(Number(row.maximum)).toBeLessThan(1000000);
        expect(Number(row.range)).toBeGreaterThan(0);
      });
    });

    it('should group with COUNT DISTINCT', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as total_count,
          COUNT(DISTINCT status) as distinct_statuses
        FROM fact_all_types
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.total_count)).toBe(200000);
        expect(Number(row.distinct_statuses)).toBe(4); // All 4 statuses per priority
      });
    });
  });

  describe('Grouping with Filters', () => {
    it('should group with simple filter', async () => {
      const sql = `
        SELECT priority, COUNT(*) as count
        FROM fact_all_types
        WHERE is_active = true
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // With is_active filter, each priority should have 100K rows (200K * 50%)
      result.forEach((row) => {
        expect(Number(row.count)).toBe(100000);
      });
    });

    it('should group with numeric range filter', async () => {
      const sql = `
        SELECT priority, COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 100000
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // With id filter, each priority should have 20K rows (100K / 5)
      result.forEach((row) => {
        expect(Number(row.count)).toBe(20000);
      });
    });

    it('should group with string filter', async () => {
      const sql = `
        SELECT status, COUNT(*) as count
        FROM fact_all_types
        WHERE priority IN ('high', 'critical')
        GROUP BY status
        ORDER BY status
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(4);

      // 2 priorities * 250K per status / 5 priorities = 100K per status
      result.forEach((row) => {
        expect(Number(row.count)).toBe(100000);
      });
    });
  });

  describe('Grouping with Ordering', () => {
    it('should group and order by dimension', async () => {
      const sql = `
        SELECT priority, COUNT(*) as count
        FROM fact_all_types
        GROUP BY priority
        ORDER BY priority ASC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // Verify alphabetical order
      expect(result[0].priority).toBe('critical');
      expect(result[1].priority).toBe('high');
      expect(result[2].priority).toBe('low');
      expect(result[3].priority).toBe('medium');
      expect(result[4].priority).toBe('unknown');
    });

    it('should group and order by aggregate (COUNT DESC)', async () => {
      const sql = `
        SELECT priority, COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 500000
        GROUP BY priority
        ORDER BY count DESC, priority ASC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // All counts should be equal (100K each), so ordered by priority
      result.forEach((row) => {
        expect(Number(row.count)).toBe(100000);
      });
    });

    it('should group and order by multiple aggregates', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as count,
          AVG(metric_double) as average
        FROM fact_all_types
        GROUP BY priority
        ORDER BY count DESC, average ASC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // Verify ordering logic
      for (let i = 1; i < result.length; i++) {
        const prevCount = Number(result[i - 1].count);
        const currCount = Number(result[i].count);
        
        if (currCount === prevCount) {
          // If same count, should be ordered by average
          expect(Number(result[i].average)).toBeGreaterThanOrEqual(Number(result[i - 1].average));
        } else {
          expect(currCount).toBeLessThanOrEqual(prevCount);
        }
      }
    });
  });

  describe('Grouping Performance', () => {
    it('should group 1M rows by single dimension quickly (< 500ms)', async () => {
      const { result, duration } = await measureExecutionTime(async () => {
        return duckdbExec(`
          SELECT priority, COUNT(*) as count
          FROM fact_all_types
          GROUP BY priority
        `);
      });

      expect(result.length).toBe(5);
      expect(duration).toBeLessThan(500);
    });

    it('should group by multiple dimensions quickly (< 1000ms)', async () => {
      const { result, duration } = await measureExecutionTime(async () => {
        return duckdbExec(`
          SELECT priority, status, is_active, COUNT(*) as count
          FROM fact_all_types
          GROUP BY priority, status, is_active
        `);
      });

      expect(result.length).toBe(40);
      expect(duration).toBeLessThan(1000);
    });

    it('should group with complex aggregates quickly (< 1000ms)', async () => {
      const { result, duration } = await measureExecutionTime(async () => {
        return duckdbExec(`
          SELECT 
            priority,
            COUNT(*) as count,
            SUM(id_bigint) as total,
            AVG(metric_double) as average,
            MIN(id_bigint) as minimum,
            MAX(id_bigint) as maximum
          FROM fact_all_types
          GROUP BY priority
        `);
      });

      expect(result.length).toBe(5);
      expect(duration).toBeLessThan(1000);
    });
  });

  describe('Grouping Edge Cases', () => {
    it('should handle grouping with LIMIT', async () => {
      const sql = `
        SELECT priority, COUNT(*) as count
        FROM fact_all_types
        GROUP BY priority
        ORDER BY priority
        LIMIT 3
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);
      expect(result[0].priority).toBe('critical');
      expect(result[1].priority).toBe('high');
      expect(result[2].priority).toBe('low');
    });

    it('should handle grouping with OFFSET', async () => {
      const sql = `
        SELECT priority, COUNT(*) as count
        FROM fact_all_types
        GROUP BY priority
        ORDER BY priority
        LIMIT 2 OFFSET 2
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(2);
      expect(result[0].priority).toBe('low');
      expect(result[1].priority).toBe('medium');
    });

    it('should handle grouping with empty result set', async () => {
      const sql = `
        SELECT priority, COUNT(*) as count
        FROM fact_all_types
        WHERE false
        GROUP BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(0);
    });

    it('should handle grouping with single row per group', async () => {
      const sql = `
        SELECT priority, id_bigint, COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 5
        GROUP BY priority, id_bigint
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      // Each of 5 IDs should have its own priority
      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.count)).toBe(1);
      });
    });
  });

  describe('Grouping with Date Functions', () => {
    it('should group by year extracted from date', async () => {
      const sql = `
        SELECT 
          EXTRACT(YEAR FROM record_date) as year,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY year
        ORDER BY year
      `;
      const result = await duckdbExec(sql);

      // record_date spans 4 years (1460 days)
      expect(result.length).toBeGreaterThan(3);
      expect(result.length).toBeLessThan(6);

      // Verify counts
      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should group by month extracted from date', async () => {
      const sql = `
        SELECT 
          EXTRACT(MONTH FROM created_date) as month,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY month
        ORDER BY month
      `;
      const result = await duckdbExec(sql);

      // Should have 12 months
      expect(result.length).toBe(12);

      // Each month should have roughly 1M/12 rows
      // With 366-day cycle, distribution is slightly different
      result.forEach((row) => {
        const count = Number(row.count);
        expect(count).toBeGreaterThan(70000);
        expect(count).toBeLessThan(95000);
      });
    });

    it('should group by year-month combination', async () => {
      const sql = `
        SELECT 
          EXTRACT(YEAR FROM record_date) as year,
          EXTRACT(MONTH FROM record_date) as month,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY year, month
        ORDER BY year, month
        LIMIT 20
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(20);

      result.forEach((row) => {
        expect(row.year).toBeGreaterThan(2019);
        expect(row.month).toBeGreaterThan(0);
        expect(row.month).toBeLessThan(13);
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });
  });
});

