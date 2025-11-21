/**
 * Comprehensive Type Conversions Tests
 * 
 * Tests type conversion and casting:
 * - CAST between different type pairs
 * - Implicit type conversions
 * - Type coercion in comparisons
 * - String to numeric conversions
 * - Numeric to string conversions
 * - Date/timestamp conversions
 * - Array operations (element access)
 * - JSON operations (advanced)
 * - NULL handling in conversions
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: Type Conversions', () => {
  beforeAll(async () => {
    console.log('🚀 Starting type conversion tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('Numeric Type Conversions', () => {
    it('should cast BIGINT to other numeric types', async () => {
      const sql = `
        SELECT 
          id_bigint,
          CAST(id_bigint AS DOUBLE) as as_double,
          CAST(id_bigint AS DECIMAL(18,2)) as as_decimal,
          CAST(id_bigint AS INTEGER) as as_integer
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        const original = Number(row.id_bigint);
        expect(Number(row.as_double)).toBe(original);
        expect(Number(row.as_decimal)).toBe(original);
        expect(Number(row.as_integer)).toBe(original);
      });
    });

    it('should cast DOUBLE to INTEGER (truncation)', async () => {
      const sql = `
        SELECT 
          metric_double,
          CAST(metric_double AS INTEGER) as as_integer,
          CAST(metric_double AS BIGINT) as as_bigint
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        const original = Number(row.metric_double);
        const asInt = Number(row.as_integer);
        const asBigint = Number(row.as_bigint);
        
        // Should truncate decimal part
        expect(Math.floor(original)).toBe(asInt);
        expect(Math.floor(original)).toBe(asBigint);
      });
    });

    it('should perform implicit numeric type coercion', async () => {
      const sql = `
        SELECT 
          id_bigint,
          metric_double,
          id_bigint + metric_double as sum_mixed,
          id_bigint * metric_double as product_mixed
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        const idBigint = Number(row.id_bigint);
        const metricDouble = Number(row.metric_double);
        const sumMixed = Number(row.sum_mixed);
        const productMixed = Number(row.product_mixed);
        
        expect(sumMixed).toBeCloseTo(idBigint + metricDouble, 2);
        expect(productMixed).toBeCloseTo(idBigint * metricDouble, 2);
      });
    });
  });

  describe('String Conversions', () => {
    it('should cast numeric types to VARCHAR', async () => {
      const sql = `
        SELECT 
          id_bigint,
          CAST(id_bigint AS VARCHAR) as id_string,
          CAST(metric_double AS VARCHAR) as metric_string,
          CAST(metric_numeric AS VARCHAR) as numeric_string
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(typeof row.id_string).toBe('string');
        expect(typeof row.metric_string).toBe('string');
        expect(typeof row.numeric_string).toBe('string');
        
        // Should be able to parse back to numbers
        expect(Number(row.id_string)).toBe(Number(row.id_bigint));
      });
    });

    it('should cast VARCHAR to numeric types', async () => {
      const sql = `
        SELECT 
          CAST('123' AS INTEGER) as int_value,
          CAST('456.78' AS DOUBLE) as double_value,
          CAST('999' AS BIGINT) as bigint_value
        FROM fact_all_types
        WHERE id_bigint = 0
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(1);
      expect(Number(result[0].int_value)).toBe(123);
      expect(Number(result[0].double_value)).toBeCloseTo(456.78, 2);
      expect(Number(result[0].bigint_value)).toBe(999);
    });

    it('should handle string to numeric coercion in comparisons', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE CAST(id_bigint AS VARCHAR) = '100'
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBe(1);
    });
  });

  describe('Boolean Conversions', () => {
    it('should cast boolean to INTEGER', async () => {
      const sql = `
        SELECT 
          is_active,
          CAST(is_active AS INTEGER) as as_integer,
          is_deleted,
          CAST(is_deleted AS INTEGER) as deleted_as_int
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        const activeInt = Number(row.as_integer);
        const deletedInt = Number(row.deleted_as_int);
        
        expect([0, 1]).toContain(activeInt);
        expect([0, 1]).toContain(deletedInt);
        
        expect(row.is_active).toBe(activeInt === 1);
        expect(row.is_deleted).toBe(deletedInt === 1);
      });
    });

    it('should cast INTEGER to boolean', async () => {
      const sql = `
        SELECT 
          CAST(0 AS BOOLEAN) as zero_as_bool,
          CAST(1 AS BOOLEAN) as one_as_bool,
          CAST(100 AS BOOLEAN) as hundred_as_bool
        FROM fact_all_types
        WHERE id_bigint = 0
      `;
      const result = await duckdbExec(sql);

      expect(result[0].zero_as_bool).toBe(false);
      expect(result[0].one_as_bool).toBe(true);
      expect(result[0].hundred_as_bool).toBe(true);
    });
  });

  describe('Date/Time Conversions', () => {
    it('should cast DATE to VARCHAR', async () => {
      const sql = `
        SELECT 
          created_date,
          CAST(created_date AS VARCHAR) as date_string
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(typeof row.date_string).toBe('string');
        expect(row.date_string).toMatch(/^\d{4}-\d{2}-\d{2}$/);
      });
    });

    it('should cast TIMESTAMP to DATE', async () => {
      const sql = `
        SELECT 
          created_timestamp,
          CAST(created_timestamp AS DATE) as as_date,
          DATE(created_timestamp) as date_func
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(row.as_date).toBeTruthy();
        expect(row.date_func).toBeTruthy();
      });
    });

    it('should cast DATE to TIMESTAMP', async () => {
      const sql = `
        SELECT 
          created_date,
          CAST(created_date AS TIMESTAMP) as as_timestamp
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(row.as_timestamp).toBeTruthy();
      });
    });

    it('should convert VARCHAR to DATE', async () => {
      const sql = `
        SELECT 
          CAST('2024-06-15' AS DATE) as parsed_date,
          CAST('2024-12-31' AS DATE) as year_end
        FROM fact_all_types
        WHERE id_bigint = 0
      `;
      const result = await duckdbExec(sql);

      expect(result[0].parsed_date).toBeTruthy();
      expect(result[0].year_end).toBeTruthy();
    });
  });

  describe('Array Operations', () => {
    it('should access array elements by index', async () => {
      const sql = `
        SELECT 
          tags,
          tags[1] as first_tag,
          tags[2] as second_tag,
          ARRAY_LENGTH(tags) as tag_count
        FROM fact_all_types
        WHERE ARRAY_LENGTH(tags) > 0
          AND id_bigint < 100
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      result.forEach((row) => {
        expect(Number(row.tag_count)).toBeGreaterThan(0);
      });
    });

    it('should cast array to string representation', async () => {
      const sql = `
        SELECT 
          tags,
          CAST(tags AS VARCHAR) as tags_string
        FROM fact_all_types
        WHERE ARRAY_LENGTH(tags) > 0
          AND id_bigint < 10
        ORDER BY id_bigint
        LIMIT 5
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      result.forEach((row) => {
        expect(typeof row.tags_string).toBe('string');
      });
    });
  });

  describe('JSON Operations', () => {
    it('should extract and cast JSON values', async () => {
      const sql = `
        SELECT 
          metadata_json,
          JSON_EXTRACT_STRING(metadata_json, '$.test') as test_value
        FROM fact_all_types
        WHERE metadata_json != '{}'
          AND id_bigint < 100
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
    });

    it('should cast JSON to VARCHAR', async () => {
      const sql = `
        SELECT 
          metadata_json,
          CAST(metadata_json AS VARCHAR) as json_string,
          LENGTH(CAST(metadata_json AS VARCHAR)) as json_length
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(typeof row.json_string).toBe('string');
        expect(Number(row.json_length)).toBeGreaterThan(0);
      });
    });
  });

  describe('NULL Handling in Conversions', () => {
    it('should handle NULL in CAST operations', async () => {
      const sql = `
        SELECT 
          resolved_by,
          CAST(resolved_by AS VARCHAR) as resolved_by_string,
          CAST(resolved_by AS DOUBLE) as resolved_by_double
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        if (row.resolved_by === null) {
          expect(row.resolved_by_string).toBeNull();
          expect(row.resolved_by_double).toBeNull();
        } else {
          expect(row.resolved_by_string).toBeTruthy();
          expect(row.resolved_by_double).toBeTruthy();
        }
      });
    });

    it('should handle NULL in COALESCE with type conversions', async () => {
      const sql = `
        SELECT 
          resolved_by,
          CAST(COALESCE(resolved_by, 0) AS VARCHAR) as resolved_string
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(row.resolved_string).toBeTruthy();
        expect(typeof row.resolved_string).toBe('string');
      });
    });
  });

  describe('Complex Type Conversions', () => {
    it('should chain multiple type conversions', async () => {
      const sql = `
        SELECT 
          metric_double,
          CAST(CAST(ROUND(metric_double) AS INTEGER) AS VARCHAR) as rounded_string
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(typeof row.rounded_string).toBe('string');
        expect(Number(row.rounded_string)).toBeGreaterThanOrEqual(0);
      });
    });

    it('should use conversions in aggregates', async () => {
      const sql = `
        SELECT 
          priority,
          SUM(CAST(is_active AS INTEGER)) as active_count,
          AVG(CAST(is_deleted AS INTEGER)) as deleted_rate
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
      result.forEach((row) => {
        expect(Number(row.active_count)).toBeGreaterThanOrEqual(0);
        expect(Number(row.deleted_rate)).toBeGreaterThanOrEqual(0);
        expect(Number(row.deleted_rate)).toBeLessThanOrEqual(1);
      });
    });
  });

  describe('Performance', () => {
    it('should execute type conversions efficiently (< 500ms)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          CAST(id_bigint AS VARCHAR) as id_string,
          CAST(metric_double AS INTEGER) as metric_int,
          CAST(created_date AS VARCHAR) as date_string,
          CAST(is_active AS INTEGER) as active_int
        FROM fact_all_types
        WHERE id_bigint < 100000
        LIMIT 1000
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(500);
    });
  });
});

