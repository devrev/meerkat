/**
 * Advanced NULL Handling Tests
 * 
 * Tests comprehensive NULL behavior:
 * - COALESCE with multiple arguments
 * - NULLIF detailed scenarios
 * - NULL in arithmetic operations
 * - NULL in comparisons (three-valued logic)
 * - NULL in aggregates
 * - NULL in JOINs
 * - NULL in window functions
 * - IS DISTINCT FROM / IS NOT DISTINCT FROM
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: Advanced NULL Handling', () => {
  beforeAll(async () => {
    console.log('🚀 Starting advanced NULL handling tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('COALESCE with Multiple Arguments', () => {
    it('should use COALESCE with 3+ arguments', async () => {
      const sql = `
        SELECT 
          id_bigint,
          resolved_by,
          mitigated_date,
          COALESCE(resolved_by, CAST(id_bigint AS BIGINT), 0) as resolved_value,
          COALESCE(mitigated_date, created_date, DATE '2020-01-01') as effective_date
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        // resolved_value should never be NULL
        expect(row.resolved_value).not.toBeNull();
        expect(row.effective_date).not.toBeNull();
      });
    });

    it('should chain COALESCE for complex fallback logic', async () => {
      const sql = `
        SELECT 
          id_bigint,
          COALESCE(
            CASE WHEN resolved_by > 100 THEN resolved_by END,
            CASE WHEN id_bigint > 50 THEN id_bigint END,
            0
          ) as complex_coalesce
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(row.complex_coalesce).not.toBeNull();
      });
    });
  });

  describe('NULLIF Detailed Scenarios', () => {
    it('should use NULLIF to convert specific values to NULL', async () => {
      const sql = `
        SELECT 
          priority,
          NULLIF(priority, 'low') as non_low_priority,
          NULLIF(priority, 'high') as non_high_priority
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        if (row.priority === 'low') {
          expect(row.non_low_priority).toBeNull();
        }
        if (row.priority === 'high') {
          expect(row.non_high_priority).toBeNull();
        }
      });
    });

    it('should use NULLIF for zero-division protection', async () => {
      const sql = `
        SELECT 
          id_bigint,
          metric_numeric,
          100.0 / NULLIF(metric_numeric, 0) as safe_division,
          CASE 
            WHEN NULLIF(metric_numeric, 0) IS NULL THEN 'Division by zero'
            ELSE 'Valid'
          END as division_status
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        if (Number(row.metric_numeric) === 0) {
          expect(row.safe_division).toBeNull();
          expect(row.division_status).toBe('Division by zero');
        } else {
          expect(row.safe_division).not.toBeNull();
          expect(row.division_status).toBe('Valid');
        }
      });
    });

    it('should combine NULLIF and COALESCE', async () => {
      const sql = `
        SELECT 
          priority,
          COALESCE(NULLIF(priority, 'low'), 'not-low') as priority_or_default
        FROM fact_all_types
        WHERE id_bigint < 50
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        if (row.priority === 'low') {
          expect(row.priority_or_default).toBe('not-low');
        } else {
          expect(row.priority_or_default).toBe(row.priority);
        }
      });
    });
  });

  describe('NULL in Aggregates', () => {
    it('should show difference between COUNT(*) and COUNT(column)', async () => {
      const sql = `
        SELECT 
          COUNT(*) as count_all,
          COUNT(resolved_by) as count_resolved,
          COUNT(DISTINCT resolved_by) as count_distinct_resolved
        FROM fact_all_types
        WHERE id_bigint < 1000
      `;
      const result = await duckdbExec(sql);

      const countAll = Number(result[0].count_all);
      const countResolved = Number(result[0].count_resolved);
      const countDistinct = Number(result[0].count_distinct_resolved);

      expect(countAll).toBe(1000);
      // count_resolved should be less than count_all if there are NULLs
      expect(countResolved).toBeLessThanOrEqual(countAll);
      expect(countDistinct).toBeLessThanOrEqual(countResolved);
    });

    it('should handle NULL in AVG, SUM, MIN, MAX', async () => {
      const sql = `
        SELECT 
          AVG(resolved_by) as avg_resolved,
          SUM(resolved_by) as sum_resolved,
          MIN(resolved_by) as min_resolved,
          MAX(resolved_by) as max_resolved,
          COUNT(resolved_by) as count_resolved
        FROM fact_all_types
        WHERE id_bigint < 1000
      `;
      const result = await duckdbExec(sql);

      // Aggregates should ignore NULL values
      const countResolved = Number(result[0].count_resolved);
      
      if (countResolved > 0) {
        expect(result[0].avg_resolved).not.toBeNull();
        expect(result[0].sum_resolved).not.toBeNull();
        expect(result[0].min_resolved).not.toBeNull();
        expect(result[0].max_resolved).not.toBeNull();
      } else {
        expect(result[0].avg_resolved).toBeNull();
        expect(result[0].sum_resolved).toBeNull();
        expect(result[0].min_resolved).toBeNull();
        expect(result[0].max_resolved).toBeNull();
      }
    });

    it('should use FILTER clause with NULL handling', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as total_count,
          COUNT(*) FILTER (WHERE resolved_by IS NOT NULL) as resolved_count,
          COUNT(*) FILTER (WHERE resolved_by IS NULL) as unresolved_count
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
      result.forEach((row) => {
        const total = Number(row.total_count);
        const resolved = Number(row.resolved_count);
        const unresolved = Number(row.unresolved_count);
        
        expect(total).toBe(resolved + unresolved);
      });
    });
  });

  describe('NULL in Window Functions', () => {
    it('should handle NULL in window function ordering', async () => {
      const sql = `
        SELECT 
          id_bigint,
          resolved_by,
          ROW_NUMBER() OVER (ORDER BY resolved_by NULLS FIRST) as row_num_nulls_first,
          ROW_NUMBER() OVER (ORDER BY resolved_by NULLS LAST) as row_num_nulls_last
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(Number(row.row_num_nulls_first)).toBeGreaterThan(0);
        expect(Number(row.row_num_nulls_last)).toBeGreaterThan(0);
      });
    });

    it('should handle NULL in window function aggregates', async () => {
      const sql = `
        SELECT 
          id_bigint,
          resolved_by,
          AVG(resolved_by) OVER (ORDER BY id_bigint ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as moving_avg
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      // moving_avg will be NULL if all values in window are NULL
    });
  });

  describe('IS DISTINCT FROM / IS NOT DISTINCT FROM', () => {
    it('should use IS DISTINCT FROM for NULL-safe comparison', async () => {
      const sql = `
        SELECT 
          f1.id_bigint as id1,
          f2.id_bigint as id2,
          f1.resolved_by as resolved1,
          f2.resolved_by as resolved2,
          f1.resolved_by IS DISTINCT FROM f2.resolved_by as is_different
        FROM fact_all_types f1
        CROSS JOIN fact_all_types f2
        WHERE f1.id_bigint < 5
          AND f2.id_bigint BETWEEN 5 AND 9
        ORDER BY f1.id_bigint, f2.id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        // IS DISTINCT FROM treats NULL as equal to NULL
        if (row.resolved1 === null && row.resolved2 === null) {
          expect(row.is_different).toBe(false);
        } else if (row.resolved1 === null || row.resolved2 === null) {
          expect(row.is_different).toBe(true);
        }
      });
    });

    it('should use IS NOT DISTINCT FROM for NULL-aware equality', async () => {
      const sql = `
        SELECT 
          COUNT(*) as count
        FROM fact_all_types f1
        INNER JOIN fact_all_types f2 
          ON f1.resolved_by IS NOT DISTINCT FROM f2.resolved_by
        WHERE f1.id_bigint < 10
          AND f2.id_bigint BETWEEN 10 AND 19
      `;
      const result = await duckdbExec(sql);

      // Should match rows where both have NULL or both have same value
      expect(Number(result[0].count)).toBeGreaterThan(0);
    });
  });

  describe('Three-Valued Logic', () => {
    it('should demonstrate three-valued logic in WHERE', async () => {
      const sql = `
        SELECT 
          COUNT(*) FILTER (WHERE resolved_by > 100) as count_true,
          COUNT(*) FILTER (WHERE resolved_by <= 100) as count_false,
          COUNT(*) FILTER (WHERE NOT (resolved_by > 100 OR resolved_by <= 100)) as count_unknown,
          COUNT(*) as total
        FROM fact_all_types
        WHERE id_bigint < 1000
      `;
      const result = await duckdbExec(sql);

      const countTrue = Number(result[0].count_true);
      const countFalse = Number(result[0].count_false);
      const countUnknown = Number(result[0].count_unknown);
      const total = Number(result[0].total);

      // total = true + false + unknown (NULL cases)
      expect(total).toBe(countTrue + countFalse + countUnknown);
    });
  });
});

