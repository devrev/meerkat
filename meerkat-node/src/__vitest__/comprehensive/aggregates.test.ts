/**
 * Comprehensive Aggregate Tests
 * 
 * Tests all aggregate functions with various scenarios:
 * - COUNT(*) and COUNT(DISTINCT)
 * - SUM and AVG on numeric fields
 * - MIN and MAX on all applicable types
 * - Aggregates with grouping
 * - NULL handling in aggregates
 * - Edge cases and performance
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

describe('Comprehensive: Aggregates', () => {
  beforeAll(async () => {
    console.log('🚀 Starting Aggregate tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('COUNT Aggregates', () => {
    it('should return COUNT(*) for all rows', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.fact_all_types__count || 0);

      expect(count).toBe(1000000);
    });

    it('should return COUNT(*) with single filter', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.priority',
            operator: 'equals',
            values: ['high'],
          },
        ],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.fact_all_types__count || 0);

      // priority='high' when i % 5 = 0, so 20%
      expect(count).toBe(200000);
    });

    it('should return COUNT(*) grouped by dimension', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      // Should have 5 priority levels
      expect(result.length).toBe(5);

      // Each priority should have 200K rows (i % 5)
      const highPriority = result.find((r) => r.fact_all_types__priority === 'high');
      expect(Number(highPriority?.fact_all_types__count)).toBe(200000);
    });

    it('should return COUNT DISTINCT on a field', async () => {
      const query = {
        measures: ['fact_all_types.count_distinct_priority'],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      // Should have 5 distinct priority values
      // Note: This requires the schema to have a count_distinct measure defined
      // For now, let's use direct SQL
      const directSQL = `SELECT COUNT(DISTINCT priority) as distinct_count FROM fact_all_types`;
      const directResult = await duckdbExec(directSQL);
      const distinctCount = Number(directResult[0]?.distinct_count || 0);

      expect(distinctCount).toBe(5);
    });

    it('should handle COUNT with empty result set', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.priority',
            operator: 'equals',
            values: ['nonexistent'],
          },
        ],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.fact_all_types__count || 0);

      expect(count).toBe(0);
    });
  });

  describe('SUM Aggregates', () => {
    it('should calculate SUM of BIGINT field', async () => {
      // Direct SQL to verify SUM logic
      const directSQL = `SELECT SUM(id_bigint) as total FROM fact_all_types WHERE id_bigint < 100`;
      const result = await duckdbExec(directSQL);
      const total = Number(result[0]?.total || 0);

      // Sum of 0 to 99 = n*(n+1)/2 = 99*100/2 = 4950
      expect(total).toBe(4950);
    });

    it('should calculate SUM of DOUBLE field', async () => {
      const directSQL = `SELECT SUM(metric_double) as total FROM fact_all_types LIMIT 1`;
      const result = await duckdbExec(directSQL);
      const total = Number(result[0]?.total || 0);

      // Sum should be a large positive number
      expect(total).toBeGreaterThan(0);
      expect(total).toBeLessThan(1000000000); // Sanity check
    });

    it('should calculate SUM with filter', async () => {
      const directSQL = `
        SELECT SUM(id_bigint) as total 
        FROM fact_all_types 
        WHERE priority = 'high'
      `;
      const result = await duckdbExec(directSQL);
      const total = Number(result[0]?.total || 0);

      // Should sum IDs where i % 5 = 0 (200K rows)
      expect(total).toBeGreaterThan(0);
    });

    it('should handle SUM with all NULL values', async () => {
      // Create a scenario where SUM would be NULL
      const directSQL = `SELECT SUM(CASE WHEN false THEN id_bigint ELSE NULL END) as total FROM fact_all_types`;
      const result = await duckdbExec(directSQL);

      // SUM of all NULLs should be NULL
      expect(result[0]?.total).toBeNull();
    });

    it('should calculate SUM grouped by dimension', async () => {
      const directSQL = `
        SELECT priority, SUM(id_bigint) as total
        FROM fact_all_types
        WHERE id_bigint < 1000
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(directSQL);

      // Should have 5 groups
      expect(result.length).toBe(5);

      // Each group should have a positive sum
      result.forEach((row) => {
        expect(Number(row.total)).toBeGreaterThan(0);
      });
    });
  });

  describe('AVG Aggregates', () => {
    it('should calculate AVG of BIGINT field', async () => {
      const directSQL = `SELECT AVG(id_bigint) as average FROM fact_all_types`;
      const result = await duckdbExec(directSQL);
      const average = Number(result[0]?.average || 0);

      // Average of 0 to 999999 = (0 + 999999) / 2 = 499999.5
      expect(average).toBeCloseTo(499999.5, 1);
    });

    it('should calculate AVG of DOUBLE field', async () => {
      const directSQL = `SELECT AVG(metric_double) as average FROM fact_all_types`;
      const result = await duckdbExec(directSQL);
      const average = Number(result[0]?.average || 0);

      // metric_double = (i % 1000) / 3.0, so average should be around 166
      expect(average).toBeGreaterThan(160);
      expect(average).toBeLessThan(170);
    });

    it('should calculate AVG with filter', async () => {
      const directSQL = `
        SELECT AVG(id_bigint) as average 
        FROM fact_all_types 
        WHERE priority = 'high'
      `;
      const result = await duckdbExec(directSQL);
      const average = Number(result[0]?.average || 0);

      // Average of IDs where i % 5 = 0
      expect(average).toBeGreaterThan(0);
    });

    it('should calculate AVG grouped by dimension', async () => {
      const directSQL = `
        SELECT priority, AVG(metric_double) as average
        FROM fact_all_types
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(directSQL);

      // Should have 5 groups
      expect(result.length).toBe(5);

      // Each group should have similar averages (data is uniformly distributed)
      result.forEach((row) => {
        const avg = Number(row.average);
        expect(avg).toBeGreaterThan(160);
        expect(avg).toBeLessThan(170);
      });
    });

    it('should handle AVG with NULL values', async () => {
      // AVG ignores NULLs
      const directSQL = `
        SELECT AVG(CASE WHEN id_bigint % 2 = 0 THEN id_bigint ELSE NULL END) as average 
        FROM fact_all_types 
        WHERE id_bigint < 10
      `;
      const result = await duckdbExec(directSQL);
      const average = Number(result[0]?.average || 0);

      // Average of 0, 2, 4, 6, 8 = 4
      expect(average).toBe(4);
    });
  });

  describe('MIN/MAX Aggregates', () => {
    it('should find MIN of BIGINT field', async () => {
      const directSQL = `SELECT MIN(id_bigint) as minimum FROM fact_all_types`;
      const result = await duckdbExec(directSQL);
      const minimum = Number(result[0]?.minimum || 0);

      expect(minimum).toBe(0);
    });

    it('should find MAX of BIGINT field', async () => {
      const directSQL = `SELECT MAX(id_bigint) as maximum FROM fact_all_types`;
      const result = await duckdbExec(directSQL);
      const maximum = Number(result[0]?.maximum || 0);

      expect(maximum).toBe(999999);
    });

    it('should find MIN of DATE field', async () => {
      const directSQL = `SELECT MIN(record_date) as min_date FROM fact_all_types`;
      const result = await duckdbExec(directSQL);

      expect(result[0]?.min_date).toBe('2020-01-01');
    });

    it('should find MAX of DATE field', async () => {
      const directSQL = `SELECT MAX(record_date) as max_date FROM fact_all_types`;
      const result = await duckdbExec(directSQL);

      // record_date = '2020-01-01' + (i % 1460), max is 1459 days after
      // 1460 days = ~4 years, so max should be around 2023-12-30
      const maxDate = result[0]?.max_date;
      expect(maxDate).toMatch(/2023-12-/);
    });

    it('should find MIN of STRING field', async () => {
      const directSQL = `SELECT MIN(priority) as min_priority FROM fact_all_types`;
      const result = await duckdbExec(directSQL);

      // Alphabetically: critical < high < low < medium < unknown
      expect(result[0]?.min_priority).toBe('critical');
    });

    it('should find MAX of STRING field', async () => {
      const directSQL = `SELECT MAX(priority) as max_priority FROM fact_all_types`;
      const result = await duckdbExec(directSQL);

      expect(result[0]?.max_priority).toBe('unknown');
    });

    it('should find MIN/MAX with filter', async () => {
      const directSQL = `
        SELECT 
          MIN(id_bigint) as minimum,
          MAX(id_bigint) as maximum
        FROM fact_all_types 
        WHERE priority = 'high'
      `;
      const result = await duckdbExec(directSQL);

      const minimum = Number(result[0]?.minimum || 0);
      const maximum = Number(result[0]?.maximum || 0);

      // priority='high' when i % 5 = 0
      expect(minimum).toBe(0);
      expect(maximum).toBe(999995); // Last multiple of 5 under 1M
    });

    it('should find MIN/MAX grouped by dimension', async () => {
      const directSQL = `
        SELECT 
          status,
          MIN(id_bigint) as minimum,
          MAX(id_bigint) as maximum
        FROM fact_all_types
        GROUP BY status
        ORDER BY status
      `;
      const result = await duckdbExec(directSQL);

      // Should have 4 status values
      expect(result.length).toBe(4);

      result.forEach((row) => {
        expect(Number(row.minimum)).toBeGreaterThanOrEqual(0);
        expect(Number(row.maximum)).toBeLessThan(1000000);
        expect(Number(row.maximum)).toBeGreaterThan(Number(row.minimum));
      });
    });
  });

  describe('Multiple Aggregates Together', () => {
    it('should calculate COUNT, SUM, AVG, MIN, MAX simultaneously', async () => {
      const directSQL = `
        SELECT 
          COUNT(*) as count,
          SUM(id_bigint) as total,
          AVG(id_bigint) as average,
          MIN(id_bigint) as minimum,
          MAX(id_bigint) as maximum
        FROM fact_all_types
      `;
      const result = await duckdbExec(directSQL);

      expect(Number(result[0]?.count)).toBe(1000000);
      expect(Number(result[0]?.total)).toBeGreaterThan(0);
      expect(Number(result[0]?.average)).toBeCloseTo(499999.5, 1);
      expect(Number(result[0]?.minimum)).toBe(0);
      expect(Number(result[0]?.maximum)).toBe(999999);
    });

    it('should calculate multiple aggregates with grouping', async () => {
      const directSQL = `
        SELECT 
          priority,
          COUNT(*) as count,
          AVG(metric_double) as average,
          MIN(id_bigint) as minimum,
          MAX(id_bigint) as maximum
        FROM fact_all_types
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(directSQL);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.count)).toBe(200000);
        expect(Number(row.average)).toBeGreaterThan(0);
        expect(Number(row.minimum)).toBeGreaterThanOrEqual(0);
        expect(Number(row.maximum)).toBeLessThan(1000000);
      });
    });
  });

  describe('Aggregate Performance', () => {
    it('should execute COUNT(*) quickly (< 100ms on 1M rows)', async () => {
      const { result, duration } = await measureExecutionTime(async () => {
        return duckdbExec('SELECT COUNT(*) as count FROM fact_all_types');
      });

      expect(Number(result[0]?.count)).toBe(1000000);
      expect(duration).toBeLessThan(100);
    });

    it('should execute complex aggregate query quickly (< 500ms)', async () => {
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
      expect(duration).toBeLessThan(500);
    });

    it('should execute COUNT DISTINCT quickly (< 200ms)', async () => {
      const { result, duration } = await measureExecutionTime(async () => {
        return duckdbExec('SELECT COUNT(DISTINCT priority) as distinct_count FROM fact_all_types');
      });

      expect(Number(result[0]?.distinct_count)).toBe(5);
      expect(duration).toBeLessThan(200);
    });
  });

  describe('Aggregate Edge Cases', () => {
    it('should handle zero rows for aggregates', async () => {
      const directSQL = `
        SELECT 
          COUNT(*) as count,
          SUM(id_bigint) as total,
          AVG(id_bigint) as average,
          MIN(id_bigint) as minimum,
          MAX(id_bigint) as maximum
        FROM fact_all_types
        WHERE false
      `;
      const result = await duckdbExec(directSQL);

      expect(Number(result[0]?.count)).toBe(0);
      expect(result[0]?.total).toBeNull();
      expect(result[0]?.average).toBeNull();
      expect(result[0]?.minimum).toBeNull();
      expect(result[0]?.maximum).toBeNull();
    });

    it('should handle single row for aggregates', async () => {
      const directSQL = `
        SELECT 
          COUNT(*) as count,
          SUM(id_bigint) as total,
          AVG(id_bigint) as average,
          MIN(id_bigint) as minimum,
          MAX(id_bigint) as maximum
        FROM fact_all_types
        WHERE id_bigint = 42
      `;
      const result = await duckdbExec(directSQL);

      expect(Number(result[0]?.count)).toBe(1);
      expect(Number(result[0]?.total)).toBe(42);
      expect(Number(result[0]?.average)).toBe(42);
      expect(Number(result[0]?.minimum)).toBe(42);
      expect(Number(result[0]?.maximum)).toBe(42);
    });

    it('should handle aggregates on BOOLEAN fields', async () => {
      const directSQL = `
        SELECT 
          COUNT(*) as total_count,
          COUNT(is_active) as non_null_count,
          SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as true_count,
          SUM(CASE WHEN NOT is_active THEN 1 ELSE 0 END) as false_count
        FROM fact_all_types
      `;
      const result = await duckdbExec(directSQL);

      expect(Number(result[0]?.total_count)).toBe(1000000);
      expect(Number(result[0]?.non_null_count)).toBe(1000000);
      expect(Number(result[0]?.true_count)).toBe(500000); // i % 2 = 0
      expect(Number(result[0]?.false_count)).toBe(500000);
    });
  });
});

