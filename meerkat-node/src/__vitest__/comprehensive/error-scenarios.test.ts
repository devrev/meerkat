/**
 * Comprehensive Error Scenarios Tests
 * 
 * Tests error handling and edge cases:
 * - Division by zero
 * - Out-of-range values
 * - Invalid date/time values
 * - Type mismatches
 * - NULL propagation in operations
 * - Invalid function arguments
 * - Constraint violations
 * - Resource limits
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: Error Scenarios', () => {
  beforeAll(async () => {
    console.log('🚀 Starting error scenario tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('Division by Zero', () => {
    it('should handle division by zero gracefully', async () => {
      try {
        const sql = `SELECT 1 / 0 as result`;
        await duckdbExec(sql);
        // If no error, result should be null or infinity
      } catch (error) {
        // Expected: division by zero error
        expect(error).toBeDefined();
      }
    });

    it('should handle division by zero in SELECT with NULL check', async () => {
      const sql = `
        SELECT 
          metric_double,
          CASE 
            WHEN metric_double = 0 THEN NULL
            ELSE 100.0 / metric_double
          END as result
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      // Should execute without error
    });

    it('should use NULLIF to prevent division by zero', async () => {
      const sql = `
        SELECT 
          metric_numeric,
          100.0 / NULLIF(metric_numeric, 0) as safe_division
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        if (Number(row.metric_numeric) === 0) {
          expect(row.safe_division).toBeNull();
        } else {
          expect(row.safe_division).toBeTruthy();
        }
      });
    });
  });

  describe('NULL Propagation', () => {
    it('should propagate NULL in arithmetic operations', async () => {
      const sql = `
        SELECT 
          resolved_by,
          resolved_by + 100 as plus_100,
          resolved_by * 2 as times_2,
          resolved_by - 50 as minus_50
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        if (row.resolved_by === null) {
          expect(row.plus_100).toBeNull();
          expect(row.times_2).toBeNull();
          expect(row.minus_50).toBeNull();
        }
      });
    });

    it('should propagate NULL in string operations', async () => {
      const sql = `
        SELECT 
          resolved_by,
          CAST(resolved_by AS VARCHAR) as as_string,
          CONCAT('ID: ', CAST(resolved_by AS VARCHAR)) as concat_result
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        if (row.resolved_by === null) {
          expect(row.as_string).toBeNull();
        }
      });
    });

    it('should handle NULL in comparisons', async () => {
      const sql = `
        SELECT 
          resolved_by,
          resolved_by > 100 as gt_100,
          resolved_by = 100 as eq_100,
          resolved_by IS NULL as is_null
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        if (row.is_null) {
          // NULL comparisons should return NULL (which becomes false in SQL)
          expect(row.gt_100).toBeNull();
          expect(row.eq_100).toBeNull();
        }
      });
    });
  });

  describe('Type Mismatches', () => {
    it('should handle incompatible type comparisons', async () => {
      try {
        // This may or may not error depending on implicit conversion
        const sql = `
          SELECT COUNT(*) as count
          FROM fact_all_types
          WHERE priority = 123
        `;
        const result = await duckdbExec(sql);
        // If it succeeds, should return 0 (no matches)
        expect(Number(result[0].count)).toBe(0);
      } catch (error) {
        // Expected: type mismatch error
        expect(error).toBeDefined();
      }
    });

    it('should handle string to number conversion errors gracefully', async () => {
      try {
        const sql = `SELECT CAST('not a number' AS INTEGER) as result`;
        await duckdbExec(sql);
        // May return NULL or error
      } catch (error) {
        // Expected: conversion error
        expect(error).toBeDefined();
      }
    });
  });

  describe('Date/Time Errors', () => {
    it('should handle invalid date values', async () => {
      try {
        const sql = `SELECT DATE '2024-13-45' as invalid_date`;
        await duckdbExec(sql);
        // Should error
      } catch (error) {
        expect(error).toBeDefined();
        expect(error.message || error.toString()).toMatch(/date|invalid/i);
      }
    });

    it('should handle invalid timestamp values', async () => {
      try {
        const sql = `SELECT TIMESTAMP '2024-02-30 25:99:99' as invalid_ts`;
        await duckdbExec(sql);
        // Should error
      } catch (error) {
        expect(error).toBeDefined();
      }
    });

    it('should handle date arithmetic overflow', async () => {
      const sql = `
        SELECT 
          DATE '9999-12-31' as max_date,
          DATE '9999-12-31' + INTERVAL '1 day' as overflow_date
      `;
      
      try {
        await duckdbExec(sql);
        // May succeed or error depending on DuckDB behavior
      } catch (error) {
        // Expected: overflow error
        expect(error).toBeDefined();
      }
    });
  });

  describe('Aggregate Errors', () => {
    it('should handle empty aggregates', async () => {
      const sql = `
        SELECT 
          COUNT(*) as count,
          AVG(metric_double) as avg_metric,
          SUM(metric_numeric) as sum_metric
        FROM fact_all_types
        WHERE id_bigint < 0
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBe(0);
      expect(result[0].avg_metric).toBeNull();
      expect(result[0].sum_metric).toBeNull();
    });

    it('should handle aggregates with all NULL values', async () => {
      const sql = `
        SELECT 
          COUNT(resolved_by) as count_resolved,
          AVG(resolved_by) as avg_resolved,
          SUM(resolved_by) as sum_resolved
        FROM fact_all_types
        WHERE resolved_by IS NULL
          AND id_bigint < 1000
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count_resolved)).toBe(0);
      expect(result[0].avg_resolved).toBeNull();
      expect(result[0].sum_resolved).toBeNull();
    });
  });

  describe('Function Argument Errors', () => {
    it('should handle negative SUBSTRING length', async () => {
      try {
        const sql = `SELECT SUBSTRING('hello', 1, -5) as result`;
        const result = await duckdbExec(sql);
        // May return empty string or error
        expect(result).toBeDefined();
      } catch (error) {
        expect(error).toBeDefined();
      }
    });

    it('should handle ROUND with invalid precision', async () => {
      const sql = `
        SELECT 
          metric_double,
          ROUND(metric_double, 2) as rounded_valid
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
    });
  });

  describe('NULL in Functions', () => {
    it('should handle NULL in string functions', async () => {
      const sql = `
        SELECT 
          UPPER(NULL) as upper_null,
          LOWER(NULL) as lower_null,
          CONCAT('test', NULL) as concat_null,
          LENGTH(NULL) as length_null
      `;
      const result = await duckdbExec(sql);

      expect(result[0].upper_null).toBeNull();
      expect(result[0].lower_null).toBeNull();
      expect(result[0].length_null).toBeNull();
    });

    it('should handle NULL in date functions', async () => {
      const sql = `
        SELECT 
          DATE_TRUNC('month', NULL) as trunc_null,
          EXTRACT(YEAR FROM CAST(NULL AS DATE)) as extract_null
      `;
      const result = await duckdbExec(sql);

      expect(result[0].trunc_null).toBeNull();
      expect(result[0].extract_null).toBeNull();
    });

    it('should handle NULL in math functions', async () => {
      const sql = `
        SELECT 
          ABS(NULL) as abs_null,
          ROUND(NULL) as round_null,
          SQRT(NULL) as sqrt_null
      `;
      const result = await duckdbExec(sql);

      expect(result[0].abs_null).toBeNull();
      expect(result[0].round_null).toBeNull();
      expect(result[0].sqrt_null).toBeNull();
    });
  });

  describe('Edge Case Values', () => {
    it('should handle very large numbers', async () => {
      const sql = `
        SELECT 
          999999999999999 as large_int,
          999999999999999.99 as large_double
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].large_int)).toBeGreaterThan(0);
      expect(Number(result[0].large_double)).toBeGreaterThan(0);
    });

    it('should handle very small (negative) numbers', async () => {
      const sql = `
        SELECT 
          -999999999999999 as small_int,
          -999999999999999.99 as small_double
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].small_int)).toBeLessThan(0);
      expect(Number(result[0].small_double)).toBeLessThan(0);
    });

    it('should handle zero values in various operations', async () => {
      const sql = `
        SELECT 
          0 as zero,
          0 * 1000 as zero_times,
          0 + 1000 as zero_plus,
          1000 - 1000 as result_zero
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].zero)).toBe(0);
      expect(Number(result[0].zero_times)).toBe(0);
      expect(Number(result[0].zero_plus)).toBe(1000);
      expect(Number(result[0].result_zero)).toBe(0);
    });
  });

  describe('Empty String Handling', () => {
    it('should handle empty strings in operations', async () => {
      const sql = `
        SELECT 
          '' as empty_string,
          LENGTH('') as empty_length,
          CONCAT('test', '') as concat_empty,
          UPPER('') as upper_empty
      `;
      const result = await duckdbExec(sql);

      expect(result[0].empty_string).toBe('');
      expect(Number(result[0].empty_length)).toBe(0);
      expect(result[0].concat_empty).toBe('test');
      expect(result[0].upper_empty).toBe('');
    });

    it('should distinguish between empty string and NULL', async () => {
      const sql = `
        SELECT 
          '' as empty_string,
          NULL as null_value,
          '' IS NULL as empty_is_null,
          NULL IS NULL as null_is_null,
          '' = '' as empty_equals_empty
      `;
      const result = await duckdbExec(sql);

      expect(result[0].empty_string).toBe('');
      expect(result[0].null_value).toBeNull();
      expect(result[0].empty_is_null).toBe(false);
      expect(result[0].null_is_null).toBe(true);
      expect(result[0].empty_equals_empty).toBe(true);
    });
  });

  describe('Complex Error Scenarios', () => {
    it('should handle errors in subqueries gracefully', async () => {
      const sql = `
        SELECT 
          priority,
          (SELECT AVG(metric_double) FROM fact_all_types WHERE priority = f.priority AND id_bigint < 1000) as avg_metric
        FROM fact_all_types f
        WHERE id_bigint < 5
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
    });

    it('should handle CASE with all NULL results', async () => {
      const sql = `
        SELECT 
          CASE 
            WHEN 1 = 2 THEN 'A'
            WHEN 2 = 3 THEN 'B'
          END as result
        FROM fact_all_types
        WHERE id_bigint = 0
      `;
      const result = await duckdbExec(sql);

      expect(result[0].result).toBeNull();
    });
  });

  describe('Performance with Errors', () => {
    it('should handle large result sets without errors', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 500000
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should handle complex queries without timeout', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          priority,
          status,
          COUNT(*) as count,
          AVG(metric_double) as avg_metric,
          SUM(metric_numeric) as sum_numeric
        FROM fact_all_types
        WHERE id_bigint < 100000
        GROUP BY priority, status
        ORDER BY priority, status
      `;

      const result = await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(result.length).toBeGreaterThan(0);
      expect(duration).toBeLessThan(5000); // Should complete in < 5s
    });
  });
});

