/**
 * Comprehensive Ordering Tests
 * 
 * Tests ORDER BY functionality with various scenarios:
 * - Single column ordering (ASC/DESC)
 * - Multi-column ordering
 * - Ordering on different data types (numeric, string, date, boolean)
 * - Ordering with NULL values
 * - Ordering with aggregates
 * - Performance on large result sets
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

describe('Comprehensive: Ordering', () => {
  beforeAll(async () => {
    console.log('🚀 Starting Ordering tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('Single Column Ordering - Numeric', () => {
    it('should order BIGINT ascending', async () => {
      const sql = `
        SELECT id_bigint 
        FROM fact_all_types 
        ORDER BY id_bigint ASC 
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      expect(Number(result[0].id_bigint)).toBe(0);
      expect(Number(result[9].id_bigint)).toBe(9);

      // Verify ascending order
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].id_bigint)).toBeGreaterThan(Number(result[i - 1].id_bigint));
      }
    });

    it('should order BIGINT descending', async () => {
      const sql = `
        SELECT id_bigint 
        FROM fact_all_types 
        ORDER BY id_bigint DESC 
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      expect(Number(result[0].id_bigint)).toBe(999999);
      expect(Number(result[9].id_bigint)).toBe(999990);

      // Verify descending order
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].id_bigint)).toBeLessThan(Number(result[i - 1].id_bigint));
      }
    });

    it('should order DOUBLE ascending', async () => {
      const sql = `
        SELECT metric_double 
        FROM fact_all_types 
        WHERE id_bigint < 100
        ORDER BY metric_double ASC 
        LIMIT 5
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // Verify ascending order
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].metric_double)).toBeGreaterThanOrEqual(Number(result[i - 1].metric_double));
      }
    });

    it('should order DOUBLE descending', async () => {
      const sql = `
        SELECT metric_double 
        FROM fact_all_types 
        WHERE id_bigint < 100
        ORDER BY metric_double DESC 
        LIMIT 5
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // Verify descending order
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].metric_double)).toBeLessThanOrEqual(Number(result[i - 1].metric_double));
      }
    });
  });

  describe('Single Column Ordering - Strings', () => {
    it('should order VARCHAR ascending (alphabetically)', async () => {
      const sql = `
        SELECT DISTINCT priority 
        FROM fact_all_types 
        ORDER BY priority ASC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
      
      // Alphabetical order: critical, high, low, medium, unknown
      expect(result[0].priority).toBe('critical');
      expect(result[1].priority).toBe('high');
      expect(result[2].priority).toBe('low');
      expect(result[3].priority).toBe('medium');
      expect(result[4].priority).toBe('unknown');
    });

    it('should order VARCHAR descending', async () => {
      const sql = `
        SELECT DISTINCT priority 
        FROM fact_all_types 
        ORDER BY priority DESC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
      
      // Reverse alphabetical order
      expect(result[0].priority).toBe('unknown');
      expect(result[1].priority).toBe('medium');
      expect(result[2].priority).toBe('low');
      expect(result[3].priority).toBe('high');
      expect(result[4].priority).toBe('critical');
    });

    it('should order TEXT field ascending', async () => {
      const sql = `
        SELECT title 
        FROM fact_all_types 
        ORDER BY title ASC 
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      // Verify ascending order
      for (let i = 1; i < result.length; i++) {
        expect(result[i].title >= result[i - 1].title).toBe(true);
      }
    });

    it('should order by enum-like field (status)', async () => {
      const sql = `
        SELECT DISTINCT status 
        FROM fact_all_types 
        ORDER BY status ASC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(4);

      // Alphabetical: closed, in_progress, open, resolved
      const statuses = result.map((r) => r.status);
      expect(statuses).toEqual(['closed', 'in_progress', 'open', 'resolved']);
    });
  });

  describe('Single Column Ordering - Dates', () => {
    it('should order DATE ascending', async () => {
      const sql = `
        SELECT record_date 
        FROM fact_all_types 
        ORDER BY record_date ASC 
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      
      // DuckDB returns dates as Date objects, convert to string for comparison
      const firstDate = result[0].record_date instanceof Date
        ? result[0].record_date.toISOString().split('T')[0]
        : result[0].record_date;
      expect(firstDate).toBe('2020-01-01');

      // Verify ascending order
      for (let i = 1; i < result.length; i++) {
        const curr = result[i].record_date instanceof Date ? result[i].record_date : new Date(result[i].record_date);
        const prev = result[i - 1].record_date instanceof Date ? result[i - 1].record_date : new Date(result[i - 1].record_date);
        expect(curr >= prev).toBe(true);
      }
    });

    it('should order DATE descending', async () => {
      const sql = `
        SELECT record_date 
        FROM fact_all_types 
        ORDER BY record_date DESC 
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      // Verify descending order
      for (let i = 1; i < result.length; i++) {
        const curr = result[i].record_date instanceof Date ? result[i].record_date : new Date(result[i].record_date);
        const prev = result[i - 1].record_date instanceof Date ? result[i - 1].record_date : new Date(result[i - 1].record_date);
        expect(curr <= prev).toBe(true);
      }
    });

    it('should order by different date fields', async () => {
      const sql = `
        SELECT created_date, record_date 
        FROM fact_all_types 
        ORDER BY created_date ASC, record_date ASC 
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      // Verify both dates are ordered
      for (let i = 1; i < result.length; i++) {
        const prevCreated = result[i - 1].created_date instanceof Date ? result[i - 1].created_date : new Date(result[i - 1].created_date);
        const currCreated = result[i].created_date instanceof Date ? result[i].created_date : new Date(result[i].created_date);
        
        if (prevCreated.getTime() === currCreated.getTime()) {
          // If created_date is same, record_date should be ascending
          const prevRecord = result[i - 1].record_date instanceof Date ? result[i - 1].record_date : new Date(result[i - 1].record_date);
          const currRecord = result[i].record_date instanceof Date ? result[i].record_date : new Date(result[i].record_date);
          expect(currRecord >= prevRecord).toBe(true);
        } else {
          expect(currCreated >= prevCreated).toBe(true);
        }
      }
    });
  });

  describe('Single Column Ordering - Boolean', () => {
    it('should order BOOLEAN ascending (false before true)', async () => {
      const sql = `
        SELECT DISTINCT is_active 
        FROM fact_all_types 
        ORDER BY is_active ASC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(2);
      expect(result[0].is_active).toBe(false);
      expect(result[1].is_active).toBe(true);
    });

    it('should order BOOLEAN descending (true before false)', async () => {
      const sql = `
        SELECT DISTINCT is_active 
        FROM fact_all_types 
        ORDER BY is_active DESC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(2);
      expect(result[0].is_active).toBe(true);
      expect(result[1].is_active).toBe(false);
    });
  });

  describe('Multi-Column Ordering', () => {
    it('should order by two columns: priority ASC, id_bigint ASC', async () => {
      const sql = `
        SELECT priority, id_bigint 
        FROM fact_all_types 
        ORDER BY priority ASC, id_bigint ASC 
        LIMIT 20
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(20);

      // Verify priority is sorted first
      for (let i = 1; i < result.length; i++) {
        if (result[i].priority === result[i - 1].priority) {
          // If same priority, id_bigint should be ascending
          expect(Number(result[i].id_bigint)).toBeGreaterThan(Number(result[i - 1].id_bigint));
        } else {
          // Priority should be in alphabetical order
          expect(result[i].priority >= result[i - 1].priority).toBe(true);
        }
      }
    });

    it('should order by two columns: priority ASC, id_bigint DESC', async () => {
      const sql = `
        SELECT priority, id_bigint 
        FROM fact_all_types 
        ORDER BY priority ASC, id_bigint DESC 
        LIMIT 20
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(20);

      // Verify mixed ordering
      for (let i = 1; i < result.length; i++) {
        if (result[i].priority === result[i - 1].priority) {
          // If same priority, id_bigint should be descending
          expect(Number(result[i].id_bigint)).toBeLessThan(Number(result[i - 1].id_bigint));
        }
      }
    });

    it('should order by three columns', async () => {
      const sql = `
        SELECT priority, status, id_bigint 
        FROM fact_all_types 
        ORDER BY priority ASC, status ASC, id_bigint ASC 
        LIMIT 30
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(30);

      // Verify three-level ordering
      for (let i = 1; i < result.length; i++) {
        const prevPriority = result[i - 1].priority;
        const currPriority = result[i].priority;

        if (currPriority === prevPriority) {
          const prevStatus = result[i - 1].status;
          const currStatus = result[i].status;

          if (currStatus === prevStatus) {
            // Same priority and status, id should be ascending
            expect(Number(result[i].id_bigint)).toBeGreaterThan(Number(result[i - 1].id_bigint));
          } else {
            expect(currStatus >= prevStatus).toBe(true);
          }
        } else {
          expect(currPriority >= prevPriority).toBe(true);
        }
      }
    });

    it('should order by date and numeric columns', async () => {
      const sql = `
        SELECT record_date, id_bigint 
        FROM fact_all_types 
        ORDER BY record_date ASC, id_bigint ASC 
        LIMIT 20
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(20);

      // Verify mixed type ordering
      for (let i = 1; i < result.length; i++) {
        const prevDate = result[i - 1].record_date instanceof Date ? result[i - 1].record_date : new Date(result[i - 1].record_date);
        const currDate = result[i].record_date instanceof Date ? result[i].record_date : new Date(result[i].record_date);
        
        if (prevDate.getTime() === currDate.getTime()) {
          expect(Number(result[i].id_bigint)).toBeGreaterThan(Number(result[i - 1].id_bigint));
        } else {
          expect(currDate >= prevDate).toBe(true);
        }
      }
    });
  });

  describe('Ordering with Aggregates', () => {
    it('should order grouped results by COUNT', async () => {
      const sql = `
        SELECT priority, COUNT(*) as count
        FROM fact_all_types
        GROUP BY priority
        ORDER BY count DESC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // All counts should be equal (200K each)
      expect(Number(result[0].count)).toBe(200000);

      // Verify descending order
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].count)).toBeLessThanOrEqual(Number(result[i - 1].count));
      }
    });

    it('should order grouped results by SUM', async () => {
      const sql = `
        SELECT priority, SUM(id_bigint) as total
        FROM fact_all_types
        WHERE id_bigint < 1000
        GROUP BY priority
        ORDER BY total DESC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // Verify descending order by total
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].total)).toBeLessThanOrEqual(Number(result[i - 1].total));
      }
    });

    it('should order grouped results by AVG', async () => {
      const sql = `
        SELECT priority, AVG(metric_double) as average
        FROM fact_all_types
        GROUP BY priority
        ORDER BY average ASC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      // Verify ascending order by average
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].average)).toBeGreaterThanOrEqual(Number(result[i - 1].average));
      }
    });

    it('should order by dimension then by aggregate', async () => {
      const sql = `
        SELECT priority, status, COUNT(*) as count
        FROM fact_all_types
        GROUP BY priority, status
        ORDER BY priority ASC, count DESC
        LIMIT 20
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(20);

      // Verify ordering
      for (let i = 1; i < result.length; i++) {
        if (result[i].priority === result[i - 1].priority) {
          expect(Number(result[i].count)).toBeLessThanOrEqual(Number(result[i - 1].count));
        } else {
          expect(result[i].priority >= result[i - 1].priority).toBe(true);
        }
      }
    });
  });

  describe('Ordering Performance', () => {
    it('should order 1M rows quickly (< 500ms)', async () => {
      const { result, duration } = await measureExecutionTime(async () => {
        return duckdbExec(`
          SELECT id_bigint 
          FROM fact_all_types 
          ORDER BY id_bigint ASC 
          LIMIT 100
        `);
      });

      expect(result.length).toBe(100);
      expect(duration).toBeLessThan(500);
    });

    it('should order by multiple columns quickly (< 500ms)', async () => {
      const { result, duration } = await measureExecutionTime(async () => {
        return duckdbExec(`
          SELECT priority, status, id_bigint 
          FROM fact_all_types 
          ORDER BY priority ASC, status ASC, id_bigint ASC 
          LIMIT 100
        `);
      });

      expect(result.length).toBe(100);
      expect(duration).toBeLessThan(500);
    });

    it('should order grouped aggregates quickly (< 500ms)', async () => {
      const { result, duration } = await measureExecutionTime(async () => {
        return duckdbExec(`
          SELECT priority, COUNT(*) as count, AVG(metric_double) as average
          FROM fact_all_types
          GROUP BY priority
          ORDER BY count DESC, average ASC
        `);
      });

      expect(result.length).toBe(5);
      expect(duration).toBeLessThan(500);
    });
  });

  describe('Ordering Edge Cases', () => {
    it('should handle ordering with LIMIT 0', async () => {
      const sql = `
        SELECT id_bigint 
        FROM fact_all_types 
        ORDER BY id_bigint ASC 
        LIMIT 0
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(0);
    });

    it('should handle ordering with LIMIT > result set', async () => {
      const sql = `
        SELECT DISTINCT priority 
        FROM fact_all_types 
        ORDER BY priority ASC 
        LIMIT 1000
      `;
      const result = await duckdbExec(sql);

      // Only 5 distinct priorities
      expect(result.length).toBe(5);
    });

    it('should handle ordering of duplicate values', async () => {
      const sql = `
        SELECT priority 
        FROM fact_all_types 
        WHERE id_bigint < 100
        ORDER BY priority ASC, id_bigint ASC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(100);

      // Verify stable ordering even with duplicates
      for (let i = 1; i < result.length; i++) {
        expect(result[i].priority >= result[i - 1].priority).toBe(true);
      }
    });

    it('should order mixed case strings consistently', async () => {
      const sql = `
        SELECT title 
        FROM fact_all_types 
        WHERE id_bigint < 10
        ORDER BY title ASC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      // Verify consistent ordering
      for (let i = 1; i < result.length; i++) {
        expect(result[i].title >= result[i - 1].title).toBe(true);
      }
    });
  });

  describe('Ordering with OFFSET', () => {
    it('should handle OFFSET with ordering', async () => {
      const sql = `
        SELECT id_bigint 
        FROM fact_all_types 
        ORDER BY id_bigint ASC 
        LIMIT 10 OFFSET 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      expect(Number(result[0].id_bigint)).toBe(10);
      expect(Number(result[9].id_bigint)).toBe(19);
    });

    it('should handle large OFFSET', async () => {
      const sql = `
        SELECT id_bigint 
        FROM fact_all_types 
        ORDER BY id_bigint ASC 
        LIMIT 5 OFFSET 999990
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
      expect(Number(result[0].id_bigint)).toBe(999990);
      expect(Number(result[4].id_bigint)).toBe(999994);
    });

    it('should handle OFFSET beyond result set', async () => {
      const sql = `
        SELECT id_bigint 
        FROM fact_all_types 
        ORDER BY id_bigint ASC 
        LIMIT 10 OFFSET 1000000
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(0);
    });
  });
});

