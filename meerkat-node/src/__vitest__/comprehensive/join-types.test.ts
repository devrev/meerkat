/**
 * Comprehensive JOIN Types Tests
 * 
 * Tests different JOIN types and scenarios:
 * - LEFT JOIN / LEFT OUTER JOIN
 * - RIGHT JOIN / RIGHT OUTER JOIN
 * - FULL OUTER JOIN
 * - CROSS JOIN
 * - Self-joins
 * - JOIN with NULL keys
 * - JOIN with different data types
 * - Multiple JOIN chains
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: JOIN Types', () => {
  beforeAll(async () => {
    console.log('🚀 Starting JOIN types tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('LEFT JOIN', () => {
    it('should perform LEFT JOIN keeping all left table rows', async () => {
      const sql = `
        SELECT 
          f.user_id,
          u.user_name,
          u.user_segment
        FROM fact_all_types f
        LEFT JOIN dim_user u ON f.user_id = u.user_id
        WHERE f.id_bigint < 100
        ORDER BY f.id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(100);

      // All rows should have user_id from fact table
      result.forEach((row) => {
        expect(row.user_id).toBeTruthy();
        // user_name and user_segment should also be populated (since all match)
        expect(row.user_name).toBeTruthy();
        expect(row.user_segment).toBeTruthy();
      });
    });

    it('should show NULL for unmatched RIGHT table in LEFT JOIN', async () => {
      // Create a temporary fact row with non-existent user
      await duckdbExec(`
        CREATE TEMPORARY TABLE fact_with_missing AS
        SELECT * FROM fact_all_types WHERE id_bigint < 10
        UNION ALL
        SELECT 
          999999 as id_bigint,
          'nonexistent_user' as user_id,
          'nonexistent_part' as part_id,
          NULL as resolved_by,
          100 as metric_numeric,
          100.0 as metric_double,
          true as is_active,
          false as is_deleted,
          false as flag_boolean,
          'high' as priority,
          'open' as status,
          'Test' as description,
          DATE '2020-01-01' as record_date,
          DATE '2020-01-01' as created_date,
          NULL as mitigated_date,
          DATE '2020-01-01' as partition_record_date,
          TIMESTAMP '2020-01-01 00:00:00' as created_timestamp,
          TIMESTAMP '2020-01-01 00:00:00' as identified_timestamp,
          TIMESTAMP '2020-01-01 00:00:00' as deployment_time,
          ARRAY[]::VARCHAR[] as tags,
          ARRAY[]::VARCHAR[] as part_ids,
          '{}' as metadata_json
      `);

      const sql = `
        SELECT 
          f.user_id,
          u.user_name
        FROM fact_with_missing f
        LEFT JOIN dim_user u ON f.user_id = u.user_id
        WHERE f.id_bigint = 999999
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(1);
      expect(result[0].user_id).toBe('nonexistent_user');
      expect(result[0].user_name).toBeNull();

      await duckdbExec('DROP TABLE fact_with_missing');
    });

    it('should aggregate with LEFT JOIN', async () => {
      const sql = `
        SELECT 
          u.user_segment,
          COUNT(f.id_bigint) as fact_count,
          COUNT(u.user_id) as matched_count
        FROM dim_user u
        LEFT JOIN fact_all_types f ON u.user_id = f.user_id AND f.id_bigint < 1000
        GROUP BY u.user_segment
        ORDER BY u.user_segment
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);

      result.forEach((row) => {
        expect(row.user_segment).toBeTruthy();
        // matched_count should equal 10000 (all users exist)
        expect(Number(row.matched_count)).toBe(10000);
      });
    });
  });

  describe('RIGHT JOIN', () => {
    it('should perform RIGHT JOIN keeping all right table rows', async () => {
      const sql = `
        SELECT 
          f.id_bigint,
          u.user_id,
          u.user_name
        FROM fact_all_types f
        RIGHT JOIN dim_user u ON f.user_id = u.user_id
        WHERE u.user_id IN ('user_0', 'user_1', 'user_2')
        ORDER BY u.user_id, f.id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      // All rows should have user_id and user_name from dim_user
      result.forEach((row) => {
        expect(row.user_id).toBeTruthy();
        expect(row.user_name).toBeTruthy();
      });
    });

    it('should show NULL for unmatched LEFT table in RIGHT JOIN', async () => {
      await duckdbExec(`
        CREATE TEMPORARY TABLE dim_user_extra AS
        SELECT * FROM dim_user
        UNION ALL
        SELECT 
          'user_99999' as user_id,
          'Extra User' as user_name,
          'test@example.com' as user_email,
          'free' as user_segment,
          'engineering' as user_department,
          DATE '2020-01-01' as user_created_date,
          true as is_active_user
      `);

      const sql = `
        SELECT 
          f.id_bigint,
          u.user_id,
          u.user_name
        FROM fact_all_types f
        RIGHT JOIN dim_user_extra u ON f.user_id = u.user_id
        WHERE u.user_id = 'user_99999'
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      result.forEach((row) => {
        expect(row.user_id).toBe('user_99999');
        expect(row.user_name).toBe('Extra User');
        expect(row.id_bigint).toBeNull();
      });

      await duckdbExec('DROP TABLE dim_user_extra');
    });
  });

  describe('FULL OUTER JOIN', () => {
    it('should perform FULL OUTER JOIN keeping all rows from both tables', async () => {
      const sql = `
        SELECT 
          COUNT(*) as total_rows,
          SUM(CASE WHEN f.id_bigint IS NOT NULL THEN 1 ELSE 0 END) as fact_rows,
          SUM(CASE WHEN u.user_id IS NOT NULL THEN 1 ELSE 0 END) as user_rows
        FROM (SELECT * FROM fact_all_types WHERE id_bigint < 10) f
        FULL OUTER JOIN dim_user u ON f.user_id = u.user_id
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(1);

      const totalRows = Number(result[0].total_rows);
      const factRows = Number(result[0].fact_rows);
      const userRows = Number(result[0].user_rows);

      // Should have all 10K users plus some fact rows
      expect(totalRows).toBeGreaterThan(10000);
      expect(userRows).toBe(10000);
      expect(factRows).toBeGreaterThan(0);
    });
  });

  describe('CROSS JOIN', () => {
    it('should perform CROSS JOIN creating Cartesian product', async () => {
      const sql = `
        SELECT COUNT(*) as total
        FROM (SELECT DISTINCT priority FROM fact_all_types) f
        CROSS JOIN (SELECT DISTINCT user_segment FROM dim_user) u
      `;
      const result = await duckdbExec(sql);

      // 5 priorities × 3 segments = 15
      expect(Number(result[0].total)).toBe(15);
    });

    it('should use CROSS JOIN for combinations', async () => {
      const sql = `
        SELECT 
          f.priority,
          u.user_segment,
          COUNT(*) as combo_count
        FROM fact_all_types f
        CROSS JOIN (SELECT DISTINCT user_segment FROM dim_user) u
        WHERE f.id_bigint < 100
        GROUP BY f.priority, u.user_segment
        ORDER BY f.priority, u.user_segment
      `;
      const result = await duckdbExec(sql);

      // 5 priorities × 3 segments = 15 combinations
      expect(result.length).toBe(15);

      // Each combination should have the same count (100 rows / 15 combos ~ 6-7 per combo)
      result.forEach((row) => {
        expect(Number(row.combo_count)).toBeGreaterThan(0);
      });
    });
  });

  describe('Self-Joins', () => {
    it('should perform self-join to find related records', async () => {
      const sql = `
        SELECT 
          f1.id_bigint as id1,
          f2.id_bigint as id2,
          f1.priority,
          f1.status
        FROM fact_all_types f1
        INNER JOIN fact_all_types f2 
          ON f1.priority = f2.priority 
          AND f1.status = f2.status 
          AND f1.id_bigint < f2.id_bigint
        WHERE f1.id_bigint < 10
        ORDER BY f1.id_bigint, f2.id_bigint
        LIMIT 20
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        // id1 should be less than id2
        expect(Number(row.id1)).toBeLessThan(Number(row.id2));
      });
    });

    it('should self-join to find previous/next records', async () => {
      const sql = `
        SELECT 
          curr.id_bigint as current_id,
          prev.id_bigint as previous_id,
          curr.priority as current_priority,
          prev.priority as previous_priority
        FROM fact_all_types curr
        LEFT JOIN fact_all_types prev 
          ON prev.id_bigint = curr.id_bigint - 1
        WHERE curr.id_bigint BETWEEN 5 AND 10
        ORDER BY curr.id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(6); // ids 5-10

      result.forEach((row, index) => {
        expect(Number(row.current_id)).toBe(5 + index);
        if (Number(row.current_id) > 5) {
          expect(Number(row.previous_id)).toBe(Number(row.current_id) - 1);
        }
      });
    });
  });

  describe('JOIN with NULL Keys', () => {
    it('should handle NULL keys in JOIN (NULLs do not match)', async () => {
      const sql = `
        SELECT 
          COUNT(*) as null_resolved_count
        FROM fact_all_types f
        LEFT JOIN fact_all_types f2 ON f.resolved_by = f2.id_bigint
        WHERE f.resolved_by IS NULL
          AND f.id_bigint < 100
      `;
      const result = await duckdbExec(sql);

      // resolved_by is NULL for some rows, and NULL != NULL in SQL
      expect(Number(result[0].null_resolved_count)).toBeGreaterThan(0);
    });

    it('should use IS NOT DISTINCT FROM for NULL-aware JOIN', async () => {
      const sql = `
        SELECT 
          f1.id_bigint,
          f1.resolved_by,
          f2.id_bigint as matched_id
        FROM fact_all_types f1
        LEFT JOIN fact_all_types f2 
          ON f1.resolved_by IS NOT DISTINCT FROM f2.id_bigint
        WHERE f1.id_bigint < 10
        ORDER BY f1.id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
    });
  });

  describe('JOIN Chains', () => {
    it('should chain multiple JOINs', async () => {
      const sql = `
        SELECT 
          f.id_bigint,
          u.user_segment,
          p.product_category,
          COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        INNER JOIN dim_part p ON f.part_id = p.part_id
        LEFT JOIN dim_user u2 ON u.user_segment = u2.user_segment AND u2.user_id = 'user_0'
        WHERE f.id_bigint < 100
        GROUP BY f.id_bigint, u.user_segment, p.product_category
        ORDER BY f.id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      result.forEach((row) => {
        expect(row.user_segment).toBeTruthy();
        expect(row.product_category).toBeTruthy();
      });
    });

    it('should mix different JOIN types in chain', async () => {
      const sql = `
        SELECT 
          COUNT(*) as total_rows,
          COUNT(DISTINCT f.id_bigint) as fact_count,
          COUNT(DISTINCT u.user_id) as user_count,
          COUNT(DISTINCT p.part_id) as part_count
        FROM fact_all_types f
        LEFT JOIN dim_user u ON f.user_id = u.user_id
        RIGHT JOIN dim_part p ON f.part_id = p.part_id
        WHERE f.id_bigint < 100 OR f.id_bigint IS NULL
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(1);
      expect(Number(result[0].total_rows)).toBeGreaterThan(0);
    });
  });

  describe('JOIN with Different Conditions', () => {
    it('should JOIN with inequality condition', async () => {
      const sql = `
        SELECT 
          f1.id_bigint as id1,
          f2.id_bigint as id2,
          f1.priority
        FROM fact_all_types f1
        INNER JOIN fact_all_types f2 
          ON f1.priority = f2.priority 
          AND f1.id_bigint < f2.id_bigint
        WHERE f1.id_bigint < 5
        ORDER BY f1.id_bigint, f2.id_bigint
        LIMIT 20
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(Number(row.id1)).toBeLessThan(Number(row.id2));
      });
    });

    it('should JOIN with computed expressions', async () => {
      const sql = `
        SELECT 
          f.id_bigint,
          u.user_id,
          u.user_segment
        FROM fact_all_types f
        INNER JOIN dim_user u 
          ON SUBSTRING(f.user_id, 1, 4) = SUBSTRING(u.user_id, 1, 4)
        WHERE f.id_bigint < 10
        ORDER BY f.id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
    });

    it('should JOIN with BETWEEN condition', async () => {
      const sql = `
        SELECT COUNT(*) as match_count
        FROM fact_all_types f1
        INNER JOIN fact_all_types f2 
          ON f1.id_bigint BETWEEN f2.id_bigint - 5 AND f2.id_bigint + 5
        WHERE f1.id_bigint < 10
          AND f2.id_bigint < 10
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].match_count)).toBeGreaterThan(0);
    });
  });

  describe('JOIN with Aggregates', () => {
    it('should aggregate before JOIN', async () => {
      const sql = `
        SELECT 
          agg.priority,
          agg.count,
          u.user_segment,
          COUNT(*) as user_count
        FROM (
          SELECT priority, COUNT(*) as count
          FROM fact_all_types
          WHERE id_bigint < 1000
          GROUP BY priority
        ) agg
        CROSS JOIN (SELECT DISTINCT user_segment FROM dim_user) u
        GROUP BY agg.priority, agg.count, u.user_segment
        ORDER BY agg.priority, u.user_segment
      `;
      const result = await duckdbExec(sql);

      // 5 priorities × 3 segments = 15
      expect(result.length).toBe(15);
    });

    it('should aggregate after JOIN', async () => {
      const sql = `
        SELECT 
          u.user_segment,
          p.product_category,
          COUNT(*) as fact_count,
          AVG(f.metric_double) as avg_metric
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        INNER JOIN dim_part p ON f.part_id = p.part_id
        WHERE f.id_bigint < 10000
        GROUP BY u.user_segment, p.product_category
        ORDER BY u.user_segment, p.product_category
      `;
      const result = await duckdbExec(sql);

      // 3 segments × 5 categories = 15
      expect(result.length).toBe(15);

      result.forEach((row) => {
        expect(Number(row.fact_count)).toBeGreaterThan(0);
        expect(Number(row.avg_metric)).toBeGreaterThan(0);
      });
    });
  });

  describe('Performance', () => {
    it('should execute LEFT JOIN efficiently (< 1s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          u.user_segment,
          COUNT(*) as count
        FROM fact_all_types f
        LEFT JOIN dim_user u ON f.user_id = u.user_id
        WHERE f.id_bigint < 100000
        GROUP BY u.user_segment
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(1000);
    });

    it('should execute multiple JOINs efficiently (< 2s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          u.user_segment,
          p.product_category,
          COUNT(*) as count
        FROM fact_all_types f
        LEFT JOIN dim_user u ON f.user_id = u.user_id
        LEFT JOIN dim_part p ON f.part_id = p.part_id
        WHERE f.id_bigint < 50000
        GROUP BY u.user_segment, p.product_category
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(2000);
    });
  });
});

