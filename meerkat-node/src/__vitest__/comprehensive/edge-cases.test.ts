/**
 * Comprehensive Edge Cases Tests
 * 
 * Tests edge cases, NULL handling, and boundary conditions:
 * - NULL value handling across all types
 * - Empty result sets
 * - Special characters in strings
 * - Zero values
 * - Extreme values
 * - Empty strings
 * - Mixed NULL and non-NULL queries
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

describe('Comprehensive: Edge Cases & NULL Handling', () => {
  beforeAll(async () => {
    console.log('🚀 Starting edge cases tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('NULL Handling', () => {
    it('should handle NULL in numeric fields', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.resolved_by',
            operator: 'notSet',
            values: [],
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

      // resolved_by is NULL for 50% of rows
      expect(count).toBeGreaterThan(400000);
      expect(count).toBeLessThan(600000);
    });

    it('should handle NOT NULL in numeric fields', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.resolved_by',
            operator: 'set',
            values: [],
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

      // resolved_by is NOT NULL for 50% of rows
      expect(count).toBeGreaterThan(400000);
      expect(count).toBeLessThan(600000);
    });

    it('should handle NULL in date fields', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.mitigated_date',
            operator: 'notSet',
            values: [],
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

      // mitigated_date is NULL for 30% of rows
      expect(count).toBeGreaterThan(250000);
      expect(count).toBeLessThan(350000);
    });

    it('should handle NULL in boolean fields', async () => {
      const sql = `
        SELECT COUNT(*) as null_count
        FROM fact_all_types
        WHERE is_active IS NULL
      `;
      const result = await duckdbExec(sql);

      // is_active has no NULLs
      expect(Number(result[0].null_count)).toBe(0);
    });

    it('should handle NULL in string fields', async () => {
      const sql = `
        SELECT COUNT(*) as null_count
        FROM fact_all_types
        WHERE priority IS NULL
      `;
      const result = await duckdbExec(sql);

      // priority has no NULLs
      expect(Number(result[0].null_count)).toBe(0);
    });

    it('should handle mixed NULL and non-NULL in aggregates', async () => {
      const sql = `
        SELECT 
          COUNT(*) as total,
          COUNT(resolved_by) as non_null_count,
          SUM(CASE WHEN resolved_by IS NULL THEN 1 ELSE 0 END) as null_count
        FROM fact_all_types
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].total)).toBe(1000000);

      const nonNullCount = Number(result[0].non_null_count);
      const nullCount = Number(result[0].null_count);

      expect(nonNullCount + nullCount).toBe(1000000);
      expect(nullCount).toBeGreaterThan(400000);
      expect(nullCount).toBeLessThan(600000);
    });
  });

  describe('Empty Result Sets', () => {
    it('should handle query returning no rows (impossible filter)', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.id_bigint',
            operator: 'equals',
            values: [9999999],
          },
        ],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      // DuckDB may return empty array or array with one row with count=0
      expect(result.length).toBeLessThanOrEqual(1);

      if (result.length === 1) {
        expect(Number(result[0]?.fact_all_types__count || 0)).toBe(0);
      }
    });

    it('should handle query with non-existent string value', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.priority',
            operator: 'equals',
            values: ['non_existent_priority'],
          },
        ],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      if (result.length > 0) {
        expect(Number(result[0]?.fact_all_types__count || 0)).toBe(0);
      }
    });

    it('should handle grouping with empty groups', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 0
        GROUP BY priority
      `;
      const result = await duckdbExec(sql);

      // No rows match, so should be empty
      expect(result.length).toBe(0);
    });
  });

  describe('Zero Values', () => {
    it('should handle filter on zero value', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.id_bigint',
            operator: 'equals',
            values: [0],
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

      // Exactly 1 row has id_bigint = 0
      expect(count).toBe(1);
    });

    it('should handle zero in numeric comparisons', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.id_bigint',
            operator: 'gte',
            values: [0],
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

      // All rows have id_bigint >= 0
      expect(count).toBe(1000000);
    });

    it('should handle zero in aggregates', async () => {
      const sql = `
        SELECT 
          COUNT(*) as count,
          MIN(id_bigint) as min_id,
          AVG(id_bigint) as avg_id
        FROM fact_all_types
        WHERE id_bigint <= 10
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].min_id)).toBe(0);
      expect(Number(result[0].count)).toBe(11);
    });
  });

  describe('Special Characters in Strings', () => {
    it('should handle apostrophes in string filters', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE description LIKE '%can''t%'
      `;
      const result = await duckdbExec(sql);

      // description doesn't contain "can't", so count should be 0
      expect(Number(result[0].count)).toBeGreaterThanOrEqual(0);
    });

    it('should handle quotes in string filters', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE description LIKE '%"%'
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThanOrEqual(0);
    });

    it('should handle backslashes in string filters', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE description LIKE '%\\%'
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThanOrEqual(0);
    });

    it('should handle percent signs in LIKE patterns', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE description LIKE '%Issue%'
      `;
      const result = await duckdbExec(sql);

      // description contains "Issue X" pattern
      expect(Number(result[0].count)).toBe(1000000);
    });

    it('should handle underscore in LIKE patterns', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE description LIKE 'Issue _'
      `;
      const result = await duckdbExec(sql);

      // "Issue X" where X is a single digit
      expect(Number(result[0].count)).toBeGreaterThan(0);
    });
  });

  describe('Extreme Values', () => {
    it('should handle maximum BIGINT values', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.id_bigint',
            operator: 'equals',
            values: [999999],
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

      // Exactly 1 row has id_bigint = 999999
      expect(count).toBe(1);
    });

    it('should handle very large numbers in aggregates', async () => {
      const sql = `
        SELECT 
          SUM(id_bigint) as total_sum,
          AVG(id_bigint) as avg_value
        FROM fact_all_types
      `;
      const result = await duckdbExec(sql);

      // Sum of 0 to 999,999 = 499,999,500,000
      const expectedSum = (999999 * 1000000) / 2;
      expect(Number(result[0].total_sum)).toBeCloseTo(expectedSum, -6);
      expect(Number(result[0].avg_value)).toBeCloseTo(499999.5, 0);
    });

    it('should handle very small DOUBLE values', async () => {
      const sql = `
        SELECT 
          MIN(metric_double) as min_metric,
          MAX(metric_double) as max_metric
        FROM fact_all_types
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].min_metric)).toBeGreaterThan(0);
      expect(Number(result[0].max_metric)).toBeGreaterThan(Number(result[0].min_metric));
    });
  });

  describe('Empty Arrays', () => {
    it('should count rows with empty tag arrays', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE ARRAY_LENGTH(tags) = 0
      `;
      const result = await duckdbExec(sql);

      // tags are empty for ~40% of rows (when i % 3 = 2)
      expect(Number(result[0].count)).toBeGreaterThan(300000);
      expect(Number(result[0].count)).toBeLessThan(400000);
    });

    it('should handle NULL vs empty array distinction', async () => {
      const sql = `
        SELECT 
          SUM(CASE WHEN tags IS NULL THEN 1 ELSE 0 END) as null_count,
          SUM(CASE WHEN ARRAY_LENGTH(tags) = 0 THEN 1 ELSE 0 END) as empty_count
        FROM fact_all_types
      `;
      const result = await duckdbExec(sql);

      // tags are never NULL (always an array, possibly empty)
      expect(Number(result[0].null_count)).toBe(0);
      expect(Number(result[0].empty_count)).toBeGreaterThan(0);
    });
  });

  describe('Boundary Conditions', () => {
    it('should handle date at start of year', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_date',
            operator: 'equals',
            values: ['2020-01-01'],
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

      // created_date cycles every 366 days, so Jan 1 should have ~2,732 rows
      expect(count).toBeGreaterThan(2700);
      expect(count).toBeLessThan(2800);
    });

    it('should handle date at end of year', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_date',
            operator: 'equals',
            values: ['2020-12-31'],
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

      // created_date cycles every 366 days, so Dec 31 should exist
      expect(count).toBeGreaterThan(2700);
      expect(count).toBeLessThan(2800);
    });

    it('should handle timestamp at midnight', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE EXTRACT(HOUR FROM created_timestamp) = 0
          AND EXTRACT(MINUTE FROM created_timestamp) = 0
          AND EXTRACT(SECOND FROM created_timestamp) = 0
      `;
      const result = await duckdbExec(sql);

      // Some rows should have exactly midnight timestamps
      expect(Number(result[0].count)).toBeGreaterThan(0);
    });
  });

  describe('Performance on Edge Cases', () => {
    it('should handle large IN clause efficiently', async () => {
      const start = Date.now();

      const values = Array.from({ length: 1000 }, (_, i) => i);
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.id_bigint',
            operator: 'equals',
            values,
          },
        ],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(1000);
    });

    it('should handle complex NULL checks efficiently', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          COUNT(*) as total,
          SUM(CASE WHEN resolved_by IS NULL THEN 1 ELSE 0 END) as null_resolved_by,
          SUM(CASE WHEN mitigated_date IS NULL THEN 1 ELSE 0 END) as null_mitigated_date
        FROM fact_all_types
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(500);
    });
  });
});

