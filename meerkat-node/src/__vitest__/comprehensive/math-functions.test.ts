/**
 * Comprehensive Mathematical Functions Tests
 * 
 * Tests mathematical operations:
 * - Basic functions (ABS, ROUND, CEIL, FLOOR, TRUNC)
 * - Advanced math (SQRT, POW, LOG, EXP, LN)
 * - Trigonometric functions (SIN, COS, TAN)
 * - Inverse trig (ASIN, ACOS, ATAN, ATAN2)
 * - Modulo and division
 * - Mathematical constants (PI, E)
 * - Mathematical operations with NULL
 * - Edge cases (overflow, underflow)
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: Mathematical Functions', () => {
  beforeAll(async () => {
    console.log('🚀 Starting mathematical functions tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('Basic Math Functions', () => {
    it('should use ABS for absolute values', async () => {
      const sql = `
        SELECT 
          metric_numeric,
          ABS(metric_numeric) as abs_metric,
          ABS(-metric_numeric) as abs_negative
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(Number(row.abs_metric)).toBeGreaterThanOrEqual(0);
        expect(Number(row.abs_negative)).toBeGreaterThanOrEqual(0);
        expect(Number(row.abs_metric)).toBe(Number(row.abs_negative));
      });
    });

    it('should use ROUND with different precisions', async () => {
      const sql = `
        SELECT 
          metric_double,
          ROUND(metric_double) as round_0,
          ROUND(metric_double, 2) as round_2,
          ROUND(metric_double, -1) as round_tens
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(row.round_0).toBeDefined();
        expect(row.round_2).toBeDefined();
        expect(row.round_tens).toBeDefined();
      });
    });

    it('should use CEIL and FLOOR', async () => {
      const sql = `
        SELECT 
          metric_double,
          CEIL(metric_double) as ceiling,
          FLOOR(metric_double) as floor,
          TRUNC(metric_double) as truncate
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        const original = Number(row.metric_double);
        const ceiling = Number(row.ceiling);
        const floor = Number(row.floor);
        const truncate = Number(row.truncate);
        
        expect(ceiling).toBeGreaterThanOrEqual(original);
        expect(floor).toBeLessThanOrEqual(original);
        expect(Math.abs(truncate - original)).toBeLessThan(1);
      });
    });
  });

  describe('Advanced Math Functions', () => {
    it('should use SQRT for square roots', async () => {
      const sql = `
        SELECT 
          metric_double,
          SQRT(metric_double) as sqrt_metric
        FROM fact_all_types
        WHERE metric_double >= 0
          AND id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      result.forEach((row) => {
        const original = Number(row.metric_double);
        const sqrt = Number(row.sqrt_metric);
        expect(sqrt * sqrt).toBeCloseTo(original, 1);
      });
    });

    it('should use POW for exponentiation', async () => {
      const sql = `
        SELECT 
          id_bigint,
          POW(2, id_bigint) as pow_2_n,
          POW(id_bigint, 2) as pow_n_2,
          POW(id_bigint, 0.5) as pow_n_half
        FROM fact_all_types
        WHERE id_bigint BETWEEN 1 AND 5
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
      result.forEach((row, index) => {
        const n = index + 1;
        expect(Number(row.pow_2_n)).toBeCloseTo(Math.pow(2, n), 1);
        expect(Number(row.pow_n_2)).toBeCloseTo(Math.pow(n, 2), 1);
      });
    });

    it('should use LOG and EXP', async () => {
      const sql = `
        SELECT 
          id_bigint,
          LOG(10, id_bigint) as log10,
          LN(id_bigint) as natural_log,
          EXP(1) as e_value
        FROM fact_all_types
        WHERE id_bigint BETWEEN 1 AND 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(Number(row.log10)).toBeGreaterThanOrEqual(0);
        expect(Number(row.natural_log)).toBeGreaterThanOrEqual(0);
        expect(Number(row.e_value)).toBeCloseTo(Math.E, 2);
      });
    });
  });

  describe('Trigonometric Functions', () => {
    it('should use SIN, COS, TAN', async () => {
      const sql = `
        SELECT 
          0 as angle,
          SIN(0) as sin_0,
          COS(0) as cos_0,
          TAN(0) as tan_0
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].sin_0)).toBeCloseTo(0, 5);
      expect(Number(result[0].cos_0)).toBeCloseTo(1, 5);
      expect(Number(result[0].tan_0)).toBeCloseTo(0, 5);
    });

    it('should use PI constant', async () => {
      const sql = `
        SELECT 
          PI() as pi_value,
          SIN(PI() / 2) as sin_90,
          COS(PI()) as cos_180,
          TAN(PI() / 4) as tan_45
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].pi_value)).toBeCloseTo(Math.PI, 5);
      expect(Number(result[0].sin_90)).toBeCloseTo(1, 5);
      expect(Number(result[0].cos_180)).toBeCloseTo(-1, 5);
      expect(Number(result[0].tan_45)).toBeCloseTo(1, 5);
    });

    it('should use inverse trig functions', async () => {
      const sql = `
        SELECT 
          ASIN(0.5) as asin_half,
          ACOS(0.5) as acos_half,
          ATAN(1) as atan_one,
          ATAN2(1, 1) as atan2_one_one
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].asin_half)).toBeCloseTo(Math.asin(0.5), 5);
      expect(Number(result[0].acos_half)).toBeCloseTo(Math.acos(0.5), 5);
      expect(Number(result[0].atan_one)).toBeCloseTo(Math.atan(1), 5);
      expect(Number(result[0].atan2_one_one)).toBeCloseTo(Math.atan2(1, 1), 5);
    });
  });

  describe('Modulo and Division', () => {
    it('should use modulo operator', async () => {
      const sql = `
        SELECT 
          id_bigint,
          id_bigint % 10 as mod_10,
          id_bigint % 5 as mod_5,
          id_bigint % 2 as mod_2
        FROM fact_all_types
        WHERE id_bigint < 20
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(20);
      result.forEach((row) => {
        expect(Number(row.mod_10)).toBeGreaterThanOrEqual(0);
        expect(Number(row.mod_10)).toBeLessThan(10);
        expect(Number(row.mod_5)).toBeGreaterThanOrEqual(0);
        expect(Number(row.mod_5)).toBeLessThan(5);
        expect([0, 1]).toContain(Number(row.mod_2));
      });
    });

    it('should use integer division', async () => {
      const sql = `
        SELECT 
          id_bigint,
          id_bigint / 10 as div_10,
          id_bigint // 10 as int_div_10,
          CAST(id_bigint AS DOUBLE) / 10 as float_div_10
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
    });
  });

  describe('Complex Mathematical Expressions', () => {
    it('should compute distance formula', async () => {
      const sql = `
        SELECT 
          id_bigint,
          metric_double,
          SQRT(POW(id_bigint, 2) + POW(metric_double, 2)) as euclidean_distance
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        const id = Number(row.id_bigint);
        const metric = Number(row.metric_double);
        const expected = Math.sqrt(id * id + metric * metric);
        expect(Number(row.euclidean_distance)).toBeCloseTo(expected, 1);
      });
    });

    it('should compute compound interest formula', async () => {
      const sql = `
        SELECT 
          1000 as principal,
          0.05 as rate,
          10 as years,
          1000 * POW(1 + 0.05, 10) as compound_amount
      `;
      const result = await duckdbExec(sql);

      const expected = 1000 * Math.pow(1.05, 10);
      expect(Number(result[0].compound_amount)).toBeCloseTo(expected, 1);
    });

    it('should use math functions in aggregates', async () => {
      const sql = `
        SELECT 
          priority,
          AVG(metric_double) as avg_metric,
          SQRT(AVG(POW(metric_double - AVG(metric_double) OVER (), 2))) as std_dev_approx
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
      result.forEach((row) => {
        expect(Number(row.avg_metric)).toBeGreaterThan(0);
      });
    });
  });

  describe('NULL Handling', () => {
    it('should handle NULL in math functions', async () => {
      const sql = `
        SELECT 
          ABS(NULL) as abs_null,
          SQRT(NULL) as sqrt_null,
          POW(NULL, 2) as pow_null,
          LOG(NULL) as log_null
      `;
      const result = await duckdbExec(sql);

      expect(result[0].abs_null).toBeNull();
      expect(result[0].sqrt_null).toBeNull();
      expect(result[0].pow_null).toBeNull();
      expect(result[0].log_null).toBeNull();
    });
  });

  describe('Edge Cases', () => {
    it('should handle very large exponents', async () => {
      const sql = `
        SELECT 
          POW(2, 10) as pow_2_10,
          POW(2, 20) as pow_2_20
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].pow_2_10)).toBe(1024);
      expect(Number(result[0].pow_2_20)).toBe(1048576);
    });

    it('should handle square root of zero', async () => {
      const sql = `SELECT SQRT(0) as sqrt_zero`;
      const result = await duckdbExec(sql);

      expect(Number(result[0].sqrt_zero)).toBe(0);
    });

    it('should handle logarithm edge cases', async () => {
      const sql = `
        SELECT 
          LOG(10, 1) as log_1,
          LOG(10, 10) as log_10,
          LOG(10, 100) as log_100
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].log_1)).toBe(0);
      expect(Number(result[0].log_10)).toBeCloseTo(1, 5);
      expect(Number(result[0].log_100)).toBeCloseTo(2, 5);
    });
  });

  describe('Performance', () => {
    it('should execute math functions efficiently (< 500ms)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          id_bigint,
          SQRT(metric_double) as sqrt_val,
          POW(metric_double, 2) as pow_val,
          ABS(metric_numeric) as abs_val,
          ROUND(metric_double, 2) as round_val
        FROM fact_all_types
        WHERE id_bigint < 50000
        LIMIT 1000
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(500);
    });
  });
});

