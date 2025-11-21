/**
 * Comprehensive CASE Expression Tests
 * 
 * Tests CASE WHEN expressions in different contexts:
 * - Simple CASE expressions
 * - Searched CASE expressions
 * - CASE in SELECT
 * - CASE in WHERE
 * - CASE in ORDER BY
 * - CASE in GROUP BY
 * - CASE with aggregates
 * - Nested CASE expressions
 * - CASE with NULL handling
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: CASE Expressions', () => {
  beforeAll(async () => {
    console.log('🚀 Starting CASE expression tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('Simple CASE Expressions', () => {
    it('should use simple CASE for value mapping', async () => {
      const sql = `
        SELECT 
          priority,
          CASE priority
            WHEN 'low' THEN 1
            WHEN 'medium' THEN 2
            WHEN 'high' THEN 3
            WHEN 'critical' THEN 4
            WHEN 'urgent' THEN 5
          END as priority_level,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 1000
        GROUP BY priority, priority_level
        ORDER BY priority_level
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row, index) => {
        expect(Number(row.priority_level)).toBe(index + 1);
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use CASE with ELSE clause', async () => {
      const sql = `
        SELECT 
          CASE status
            WHEN 'open' THEN 'Active'
            WHEN 'in_progress' THEN 'Active'
            ELSE 'Inactive'
          END as status_category,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 1000
        GROUP BY status_category
        ORDER BY status_category
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(2); // Active and Inactive

      const categories = result.map((r) => r.status_category);
      expect(categories).toContain('Active');
      expect(categories).toContain('Inactive');
    });
  });

  describe('Searched CASE Expressions', () => {
    it('should use searched CASE with conditions', async () => {
      const sql = `
        SELECT 
          id_bigint,
          CASE 
            WHEN id_bigint < 100 THEN 'Small'
            WHEN id_bigint < 1000 THEN 'Medium'
            ELSE 'Large'
          END as size_category,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 5000
        GROUP BY size_category, id_bigint
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      result.forEach((row) => {
        const id = Number(row.id_bigint);
        if (id < 100) {
          expect(row.size_category).toBe('Small');
        } else if (id < 1000) {
          expect(row.size_category).toBe('Medium');
        } else {
          expect(row.size_category).toBe('Large');
        }
      });
    });

    it('should use CASE with complex conditions', async () => {
      const sql = `
        SELECT 
          CASE 
            WHEN priority = 'high' AND is_active = true THEN 'High Priority Active'
            WHEN priority = 'high' AND is_active = false THEN 'High Priority Inactive'
            WHEN is_active = true THEN 'Active'
            ELSE 'Inactive'
          END as category,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY category
        ORDER BY category
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(row.category).toBeTruthy();
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use CASE with multiple conditions', async () => {
      const sql = `
        SELECT 
          CASE 
            WHEN metric_double > 1000 THEN 'Very High'
            WHEN metric_double > 500 THEN 'High'
            WHEN metric_double > 100 THEN 'Medium'
            WHEN metric_double > 0 THEN 'Low'
            ELSE 'Zero or Negative'
          END as metric_category,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY metric_category
        ORDER BY metric_category
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
    });
  });

  describe('CASE in SELECT', () => {
    it('should use multiple CASE expressions in SELECT', async () => {
      const sql = `
        SELECT 
          priority,
          CASE WHEN is_active THEN 'Active' ELSE 'Inactive' END as status,
          CASE 
            WHEN metric_double > 500 THEN 'High'
            ELSE 'Low'
          END as metric_level,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 1000
        GROUP BY priority, status, metric_level
        ORDER BY priority, status, metric_level
        LIMIT 20
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(['Active', 'Inactive']).toContain(row.status);
        expect(['High', 'Low']).toContain(row.metric_level);
      });
    });

    it('should use CASE for derived columns', async () => {
      const sql = `
        SELECT 
          id_bigint,
          priority,
          CASE 
            WHEN priority IN ('high', 'critical', 'urgent') THEN metric_double * 2
            ELSE metric_double
          END as adjusted_metric
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      result.forEach((row) => {
        expect(Number(row.adjusted_metric)).toBeGreaterThan(0);
      });
    });
  });

  describe('CASE in WHERE', () => {
    it('should use CASE in WHERE clause', async () => {
      const sql = `
        SELECT 
          priority,
          status,
          COUNT(*) as count
        FROM fact_all_types
        WHERE CASE 
          WHEN priority = 'high' THEN is_active = true
          WHEN priority = 'low' THEN is_active = false
          ELSE true
        END
        AND id_bigint < 10000
        GROUP BY priority, status
        ORDER BY priority, status
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
    });

    it('should filter using CASE result', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE (CASE 
          WHEN priority = 'high' THEN 1
          WHEN priority = 'medium' THEN 2
          ELSE 3
        END) < 3
        AND id_bigint < 10000
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });
  });

  describe('CASE in ORDER BY', () => {
    it('should order by CASE expression', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 1000
        GROUP BY priority
        ORDER BY 
          CASE priority
            WHEN 'urgent' THEN 1
            WHEN 'critical' THEN 2
            WHEN 'high' THEN 3
            WHEN 'medium' THEN 4
            WHEN 'low' THEN 5
          END
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      const priorities = result.map((r) => r.priority);
      expect(priorities[0]).toBe('urgent');
      expect(priorities[1]).toBe('critical');
      expect(priorities[2]).toBe('high');
      expect(priorities[3]).toBe('medium');
      expect(priorities[4]).toBe('low');
    });

    it('should order by complex CASE', async () => {
      const sql = `
        SELECT 
          status,
          priority,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 1000
        GROUP BY status, priority
        ORDER BY 
          CASE 
            WHEN status = 'open' THEN 1
            WHEN status = 'in_progress' THEN 2
            ELSE 3
          END,
          priority
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
    });
  });

  describe('CASE in GROUP BY', () => {
    it('should group by CASE expression', async () => {
      const sql = `
        SELECT 
          CASE 
            WHEN id_bigint < 1000 THEN 'First Thousand'
            WHEN id_bigint < 10000 THEN 'First Ten Thousand'
            ELSE 'Rest'
          END as id_range,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 20000
        GROUP BY id_range
        ORDER BY id_range
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);

      const firstThousand = result.find((r) => r.id_range === 'First Thousand');
      expect(Number(firstThousand?.count)).toBe(1000);
    });
  });

  describe('CASE with Aggregates', () => {
    it('should use CASE inside aggregate function', async () => {
      const sql = `
        SELECT 
          priority,
          COUNT(CASE WHEN is_active THEN 1 END) as active_count,
          COUNT(CASE WHEN NOT is_active THEN 1 END) as inactive_count,
          COUNT(*) as total_count
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        const activeCount = Number(row.active_count);
        const inactiveCount = Number(row.inactive_count);
        const totalCount = Number(row.total_count);

        expect(activeCount + inactiveCount).toBe(totalCount);
      });
    });

    it('should use SUM with CASE for conditional aggregation', async () => {
      const sql = `
        SELECT 
          status,
          SUM(CASE WHEN priority = 'high' THEN 1 ELSE 0 END) as high_priority_count,
          SUM(CASE WHEN priority = 'low' THEN 1 ELSE 0 END) as low_priority_count,
          COUNT(*) as total
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY status
        ORDER BY status
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(Number(row.total)).toBeGreaterThan(0);
        expect(Number(row.high_priority_count)).toBeGreaterThanOrEqual(0);
        expect(Number(row.low_priority_count)).toBeGreaterThanOrEqual(0);
      });
    });

    it('should use AVG with CASE', async () => {
      const sql = `
        SELECT 
          priority,
          AVG(CASE WHEN is_active THEN metric_double ELSE NULL END) as avg_active_metric,
          AVG(CASE WHEN NOT is_active THEN metric_double ELSE NULL END) as avg_inactive_metric
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.avg_active_metric)).toBeGreaterThan(0);
        expect(Number(row.avg_inactive_metric)).toBeGreaterThan(0);
      });
    });
  });

  describe('Nested CASE Expressions', () => {
    it('should use nested CASE expressions', async () => {
      const sql = `
        SELECT 
          CASE 
            WHEN priority = 'high' THEN
              CASE 
                WHEN is_active THEN 'High Active'
                ELSE 'High Inactive'
              END
            WHEN priority = 'low' THEN
              CASE 
                WHEN is_active THEN 'Low Active'
                ELSE 'Low Inactive'
              END
            ELSE 'Other'
          END as combined_status,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY combined_status
        ORDER BY combined_status
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(row.combined_status).toBeTruthy();
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use deeply nested CASE', async () => {
      const sql = `
        SELECT 
          CASE 
            WHEN metric_double > 500 THEN
              CASE 
                WHEN priority = 'high' THEN 'High Metric, High Priority'
                ELSE 'High Metric, Other Priority'
              END
            ELSE
              CASE 
                WHEN priority = 'high' THEN 'Low Metric, High Priority'
                ELSE 'Low Metric, Other Priority'
              END
          END as category,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY category
        ORDER BY category
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(4);
    });
  });

  describe('CASE with NULL Handling', () => {
    it('should use CASE to handle NULL values', async () => {
      const sql = `
        SELECT 
          CASE 
            WHEN resolved_by IS NULL THEN 'Unresolved'
            ELSE 'Resolved'
          END as resolution_status,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY resolution_status
        ORDER BY resolution_status
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(2);

      const statuses = result.map((r) => r.resolution_status);
      expect(statuses).toContain('Unresolved');
      expect(statuses).toContain('Resolved');
    });

    it('should use CASE with COALESCE', async () => {
      const sql = `
        SELECT 
          CASE 
            WHEN COALESCE(resolved_by, 0) > 0 THEN 'Has Resolver'
            ELSE 'No Resolver'
          END as resolver_status,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY resolver_status
        ORDER BY resolver_status
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(2);
    });

    it('should return NULL from CASE when no conditions match and no ELSE', async () => {
      const sql = `
        SELECT 
          id_bigint,
          CASE 
            WHEN priority = 'nonexistent' THEN 'Match'
          END as result
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      result.forEach((row) => {
        expect(row.result).toBeNull();
      });
    });
  });

  describe('CASE with JOINs', () => {
    it('should use CASE in JOIN with dimension tables', async () => {
      const sql = `
        SELECT 
          CASE 
            WHEN u.user_segment = 'enterprise' THEN 'Premium'
            WHEN u.user_segment = 'pro' THEN 'Standard'
            ELSE 'Basic'
          END as tier,
          COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        WHERE f.id_bigint < 10000
        GROUP BY tier
        ORDER BY tier
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);

      const tiers = result.map((r) => r.tier);
      expect(tiers).toContain('Premium');
      expect(tiers).toContain('Standard');
      expect(tiers).toContain('Basic');
    });
  });

  describe('Performance', () => {
    it('should execute CASE expressions efficiently (< 500ms)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          CASE 
            WHEN priority = 'high' THEN 'Important'
            WHEN priority = 'low' THEN 'Not Important'
            ELSE 'Medium Importance'
          END as importance,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 100000
        GROUP BY importance
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(500);
    });

    it('should execute complex nested CASE efficiently (< 1s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          CASE 
            WHEN metric_double > 500 THEN
              CASE 
                WHEN is_active THEN 'High Active'
                ELSE 'High Inactive'
              END
            ELSE
              CASE 
                WHEN is_active THEN 'Low Active'
                ELSE 'Low Inactive'
              END
          END as category,
          AVG(metric_double) as avg_metric,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 100000
        GROUP BY category
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(1000);
    });
  });
});

