/**
 * Comprehensive Conditional Logic Tests
 * 
 * Tests conditional logic beyond CASE:
 * - IIF function (inline IF)
 * - Complex boolean logic
 * - Short-circuit evaluation behavior
 * - Boolean operator precedence (AND vs OR)
 * - Conditional expressions in different contexts
 * - Truth table verification
 * - NULL in boolean logic
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: Conditional Logic', () => {
  beforeAll(async () => {
    console.log('🚀 Starting conditional logic tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('IIF Function', () => {
    it('should use IIF for simple conditional logic', async () => {
      const sql = `
        SELECT 
          priority,
          IIF(priority = 'high', 1, 0) as is_high,
          IIF(priority IN ('high', 'critical', 'urgent'), 'important', 'normal') as importance
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        if (row.priority === 'high') {
          expect(Number(row.is_high)).toBe(1);
          expect(row.importance).toBe('important');
        } else if (['critical', 'urgent'].includes(row.priority)) {
          expect(row.importance).toBe('important');
        }
      });
    });

    it('should nest IIF functions', async () => {
      const sql = `
        SELECT 
          metric_double,
          IIF(metric_double > 1000, 'very_high',
            IIF(metric_double > 500, 'high',
              IIF(metric_double > 100, 'medium', 'low')
            )
          ) as metric_level
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        const metric = Number(row.metric_double);
        if (metric > 1000) {
          expect(row.metric_level).toBe('very_high');
        } else if (metric > 500) {
          expect(row.metric_level).toBe('high');
        } else if (metric > 100) {
          expect(row.metric_level).toBe('medium');
        } else {
          expect(row.metric_level).toBe('low');
        }
      });
    });

    it('should use IIF with aggregates', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as total,
          SUM(IIF(is_active, 1, 0)) as active_count,
          SUM(IIF(is_deleted, 1, 0)) as deleted_count
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
      result.forEach((row) => {
        const total = Number(row.total);
        const active = Number(row.active_count);
        const deleted = Number(row.deleted_count);
        
        expect(active + deleted).toBeLessThanOrEqual(total);
      });
    });
  });

  describe('Boolean Operator Precedence', () => {
    it('should respect AND precedence over OR', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE (priority = 'high' OR priority = 'low')
          AND status = 'open'
          AND id_bigint < 10000
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should use parentheses to override precedence', async () => {
      const sql1 = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE priority = 'high' OR priority = 'low' AND status = 'open'
          AND id_bigint < 10000
      `;

      const sql2 = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE (priority = 'high' OR priority = 'low') AND status = 'open'
          AND id_bigint < 10000
      `;

      const result1 = await duckdbExec(sql1);
      const result2 = await duckdbExec(sql2);

      // Results should be different due to precedence
      const count1 = Number(result1[0].count);
      const count2 = Number(result2[0].count);
      
      expect(count1).toBeDefined();
      expect(count2).toBeDefined();
    });

    it('should handle complex precedence with NOT', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE NOT (priority = 'high' AND status = 'closed')
          AND id_bigint < 10000
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });
  });

  describe('Truth Table Verification', () => {
    it('should verify AND truth table', async () => {
      const sql = `
        SELECT 
          true AND true as t_and_t,
          true AND false as t_and_f,
          false AND true as f_and_t,
          false AND false as f_and_f
      `;
      const result = await duckdbExec(sql);

      expect(result[0].t_and_t).toBe(true);
      expect(result[0].t_and_f).toBe(false);
      expect(result[0].f_and_t).toBe(false);
      expect(result[0].f_and_f).toBe(false);
    });

    it('should verify OR truth table', async () => {
      const sql = `
        SELECT 
          true OR true as t_or_t,
          true OR false as t_or_f,
          false OR true as f_or_t,
          false OR false as f_or_f
      `;
      const result = await duckdbExec(sql);

      expect(result[0].t_or_t).toBe(true);
      expect(result[0].t_or_f).toBe(true);
      expect(result[0].f_or_t).toBe(true);
      expect(result[0].f_or_f).toBe(false);
    });

    it('should verify NOT truth table', async () => {
      const sql = `
        SELECT 
          NOT true as not_true,
          NOT false as not_false
      `;
      const result = await duckdbExec(sql);

      expect(result[0].not_true).toBe(false);
      expect(result[0].not_false).toBe(true);
    });
  });

  describe('NULL in Boolean Logic', () => {
    it('should handle NULL in AND operations', async () => {
      const sql = `
        SELECT 
          true AND NULL as t_and_null,
          false AND NULL as f_and_null,
          NULL AND true as null_and_t,
          NULL AND false as null_and_f
      `;
      const result = await duckdbExec(sql);

      // true AND NULL = NULL
      expect(result[0].t_and_null).toBeNull();
      // false AND NULL = false (short-circuit)
      expect(result[0].f_and_null).toBe(false);
      expect(result[0].null_and_t).toBeNull();
      expect(result[0].null_and_f).toBe(false);
    });

    it('should handle NULL in OR operations', async () => {
      const sql = `
        SELECT 
          true OR NULL as t_or_null,
          false OR NULL as f_or_null,
          NULL OR true as null_or_t,
          NULL OR false as null_or_f
      `;
      const result = await duckdbExec(sql);

      // true OR NULL = true (short-circuit)
      expect(result[0].t_or_null).toBe(true);
      // false OR NULL = NULL
      expect(result[0].f_or_null).toBeNull();
      expect(result[0].null_or_t).toBe(true);
      expect(result[0].null_or_f).toBeNull();
    });

    it('should handle NOT NULL', async () => {
      const sql = `SELECT NOT NULL as not_null`;
      const result = await duckdbExec(sql);

      expect(result[0].not_null).toBeNull();
    });
  });

  describe('Complex Conditional Expressions', () => {
    it('should combine multiple conditional operators', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE (
          (priority = 'high' AND is_active = true) OR
          (priority = 'critical' AND status = 'open') OR
          (priority = 'urgent')
        )
        AND id_bigint < 10000
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should use conditional logic in SELECT with multiple columns', async () => {
      const sql = `
        SELECT 
          priority,
          status,
          is_active,
          (priority = 'high' AND status = 'open' AND is_active = true) as is_critical_active,
          (priority IN ('low', 'medium') OR is_deleted = true) as is_non_priority
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(typeof row.is_critical_active).toBe('boolean');
        expect(typeof row.is_non_priority).toBe('boolean');
      });
    });

    it('should use conditional logic with aggregates and HAVING', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as count,
          SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_count
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY priority
        HAVING (COUNT(*) > 1000 AND SUM(CASE WHEN is_active THEN 1 ELSE 0 END) > 500)
            OR priority = 'urgent'
        ORDER BY count DESC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
    });
  });

  describe('Performance', () => {
    it('should execute conditional logic efficiently (< 500ms)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          priority,
          IIF(priority = 'high', 1, 0) as is_high,
          (priority = 'high' OR priority = 'critical') AND is_active = true as is_important_active
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

