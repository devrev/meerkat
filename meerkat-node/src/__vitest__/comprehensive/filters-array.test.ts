/**
 * Comprehensive Array Filter Tests
 * 
 * Tests VARCHAR[] array field filtering with complex scenarios including:
 * - Array membership (element IN array)
 * - Array contains (array contains element)
 * - Array overlap (arrays have common elements)
 * - Empty array handling
 * - NULL array handling
 * - Complex nested queries with arrays
 * - Array + other filter combinations
 * - Performance testing on 1M+ rows
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

describe('Comprehensive: Array Filters', () => {
  beforeAll(async () => {
    console.log('🚀 Starting Vitest test suite...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('Array Contains Element', () => {
    it('should filter array contains single tag: "backend"', async () => {
      // Test if tags array contains 'backend'
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE list_contains(tags, 'backend')
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // tags includes 'backend' when i % 3 = 0, so 33% of 1M
      expect(count).toBeGreaterThan(330000);
      expect(count).toBeLessThan(340000);
    });

    it('should filter array contains single tag: "frontend"', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE list_contains(tags, 'frontend')
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // tags includes 'frontend' when i % 3 = 1, so 33% of 1M
      expect(count).toBeGreaterThan(330000);
      expect(count).toBeLessThan(340000);
    });

    it('should filter array contains single tag: "api"', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE list_contains(tags, 'api')
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // tags includes 'api' when i % 3 = 2, so 33% of 1M
      expect(count).toBeGreaterThan(330000);
      expect(count).toBeLessThan(340000);
    });

    it('should filter array contains tag: "urgent"', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE list_contains(tags, 'urgent')
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // tags includes 'urgent' when i % 10 = 0, so 10% of 1M
      expect(count).toBeGreaterThan(98000);
      expect(count).toBeLessThan(102000);
    });
  });

  describe('Array Contains Multiple Elements', () => {
    it('should filter array contains "backend" AND "urgent"', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE list_contains(tags, 'backend')
          AND list_contains(tags, 'urgent')
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // Intersection: i % 3 = 0 AND i % 10 = 0
      // This means i % 30 = 0 (LCM of 3 and 10)
      // So 1M / 30 = 33,333
      expect(count).toBeGreaterThan(33000);
      expect(count).toBeLessThan(34000);
    });

    it('should filter array contains "frontend" OR "api"', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE list_contains(tags, 'frontend')
           OR list_contains(tags, 'api')
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // Union: (i % 3 = 1) OR (i % 3 = 2)
      // This is 66% of 1M = 666,666
      expect(count).toBeGreaterThan(665000);
      expect(count).toBeLessThan(670000);
    });
  });

  describe('Array NOT Contains Element', () => {
    it('should filter array does NOT contain "urgent"', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE NOT list_contains(tags, 'urgent')
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // 90% of rows don't have 'urgent'
      expect(count).toBeGreaterThan(898000);
      expect(count).toBeLessThan(902000);
    });

    it('should filter array does NOT contain "backend"', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE NOT list_contains(tags, 'backend')
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // 67% don't have 'backend'
      expect(count).toBeGreaterThan(660000);
      expect(count).toBeLessThan(670000);
    });
  });

  describe('Array Length/Size Queries', () => {
    it('should filter by array length = 1', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE array_length(tags) = 1
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // Arrays with exactly 1 element (not multiples of 3 or 10)
      // Should be majority of rows
      expect(count).toBeGreaterThan(500000);
      expect(count).toBeLessThan(700000);
    });

    it('should filter by array length = 2', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE array_length(tags) = 2
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // Arrays with exactly 2 elements (multiples of 3 or 10, but not both)
      expect(count).toBeGreaterThan(200000);
      expect(count).toBeLessThan(400000);
    });

    it('should filter by array length >= 2', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE array_length(tags) >= 2
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // At least 2 elements
      expect(count).toBeGreaterThan(300000);
      expect(count).toBeLessThan(500000);
    });
  });

  describe('Empty and NULL Array Handling', () => {
    it('should handle empty arrays', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE array_length(tags) = 0
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // Our schema doesn't create empty arrays, all have at least 1 element
      expect(count).toBe(0);
    });

    it('should filter non-NULL arrays', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE tags IS NOT NULL
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // All arrays are non-NULL in our schema
      expect(count).toBe(1000000);
    });

    it('should filter NULL arrays', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE tags IS NULL
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // No NULL arrays in our schema
      expect(count).toBe(0);
    });
  });

  describe('Complex Array Combinations', () => {
    it('should filter array contains + boolean filter', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE list_contains(tags, 'urgent')
          AND is_active = true
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // urgent (10%) AND is_active=true (50%)
      // Intersection: rows where i % 10 = 0 AND i % 2 = 0
      // i % 10 = 0 means multiples of 10, which are all even
      // So result is 10% of 1M = 100K
      expect(count).toBeGreaterThan(98000);
      expect(count).toBeLessThan(102000);
    });

    it('should filter array contains + string filter', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE list_contains(tags, 'backend')
          AND priority = 'high'
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // backend (33%) AND priority='high' (20%)
      // Expected: ~6.6% of 1M = 66K
      expect(count).toBeGreaterThan(65000);
      expect(count).toBeLessThan(68000);
    });

    it('should filter array contains + date range', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE list_contains(tags, 'urgent')
          AND record_date >= '2020-01-01'
          AND record_date < '2020-02-01'
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // urgent (10%) AND January dates (31/1460)
      // Expected: ~2K
      expect(count).toBeGreaterThan(2000);
      expect(count).toBeLessThan(2200);
    });

    it('should filter array contains + numeric range', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE list_contains(tags, 'frontend')
          AND id_bigint BETWEEN 100000 AND 200000
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // frontend (33%) AND id in range (10%)
      // Expected: ~33K
      expect(count).toBeGreaterThan(32000);
      expect(count).toBeLessThan(35000);
    });
  });

  describe('Multiple Array Conditions', () => {
    it('should filter array contains at least one of multiple tags', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE list_contains(tags, 'backend')
           OR list_contains(tags, 'frontend')
           OR list_contains(tags, 'api')
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // All rows have at least one of these primary tags
      expect(count).toBe(1000000);
    });

    it('should filter array contains ALL of specific tags', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE list_contains(tags, 'backend')
          AND list_contains(tags, 'urgent')
          AND is_deleted = false
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // backend AND urgent AND not deleted
      // i % 30 = 0 (backend+urgent) AND i % 10 != 0 (not deleted)
      // Contradiction: i % 30 = 0 implies i % 10 = 0, so AND is_deleted=false excludes all
      // Wait, is_deleted = (i % 10 = 0), so rows where i%30=0 have is_deleted=true
      // Therefore, result should be 0
      expect(count).toBe(0);
    });

    it('should filter complex nested array conditions', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE (
          list_contains(tags, 'backend') OR list_contains(tags, 'frontend')
        ) AND NOT list_contains(tags, 'urgent')
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // (backend OR frontend) AND NOT urgent
      // (i%3=0 OR i%3=1) AND i%10!=0
      // 66% of rows, minus those divisible by 10
      // 66% - (66% of 10%) ≈ 60% = 600K
      expect(count).toBeGreaterThan(590000);
      expect(count).toBeLessThan(610000);
    });
  });

  describe('Array Aggregations and Grouping', () => {
    it('should count distinct array lengths', async () => {
      const sql = `
        SELECT array_length(tags) as tag_count, COUNT(*) as row_count
        FROM fact_all_types
        GROUP BY array_length(tags)
        ORDER BY tag_count
      `;

      const result = await duckdbExec(sql);

      // Should have multiple distinct array lengths
      expect(result.length).toBeGreaterThan(0);
      expect(result.length).toBeLessThan(5);

      // Total should be 1M
      const total = result.reduce((sum, row) => sum + Number(row.row_count || 0), 0);
      expect(total).toBe(1000000);
    });

    it('should find most common tags by unnesting', async () => {
      const sql = `
        SELECT unnest(tags) as tag, COUNT(*) as tag_count
        FROM fact_all_types
        GROUP BY tag
        ORDER BY tag_count DESC
        LIMIT 10
      `;

      const result = await duckdbExec(sql);

      // Should return tag statistics
      expect(result.length).toBeGreaterThan(0);
      expect(result.length).toBeLessThan(11);

      // Top tag should be one of the primary tags
      const topTag = result[0]?.tag;
      expect(['backend', 'frontend', 'api']).toContain(topTag);
    });
  });

  describe('Array Performance Tests', () => {
    it('should execute array contains query quickly (< 500ms on 1M rows)', async () => {
      const { result, duration } = await measureExecutionTime(async () => {
        const sql = `
          SELECT COUNT(*) AS count
          FROM fact_all_types
          WHERE list_contains(tags, 'urgent')
        `;
        return duckdbExec(sql);
      });

      const count = Number(result[0]?.count || 0);

      expect(count).toBeGreaterThan(0);
      expect(duration).toBeLessThan(500);
    });

    it('should execute complex array query quickly (< 1000ms)', async () => {
      const { result, duration } = await measureExecutionTime(async () => {
        const sql = `
          SELECT COUNT(*) AS count
          FROM fact_all_types
          WHERE (list_contains(tags, 'backend') OR list_contains(tags, 'frontend'))
            AND list_contains(tags, 'urgent')
            AND is_active = true
            AND priority IN ('high', 'critical')
        `;
        return duckdbExec(sql);
      });

      const count = Number(result[0]?.count || 0);

      expect(count).toBeGreaterThan(0);
      expect(duration).toBeLessThan(1000);
    });
  });

  describe('Array Edge Cases', () => {
    it('should handle searching for non-existent tag', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE list_contains(tags, 'nonexistent_tag')
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // Should return 0
      expect(count).toBe(0);
    });

    it('should handle case-sensitive tag search', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE list_contains(tags, 'URGENT')
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // Tags are lowercase, so uppercase search returns 0
      expect(count).toBe(0);
    });

    it('should handle array operations with empty string', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE list_contains(tags, '')
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // No empty strings in tags
      expect(count).toBe(0);
    });
  });

  describe('Advanced Array Operations', () => {
    it('should count rows by number of tags', async () => {
      const sql = `
        SELECT 
          CASE 
            WHEN array_length(tags) = 1 THEN '1_tag'
            WHEN array_length(tags) = 2 THEN '2_tags'
            WHEN array_length(tags) >= 3 THEN '3+_tags'
            ELSE 'no_tags'
          END as tag_group,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY tag_group
        ORDER BY tag_group
      `;

      const result = await duckdbExec(sql);

      // Should have multiple groups
      expect(result.length).toBeGreaterThan(0);

      // Total should be 1M
      const total = result.reduce((sum, row) => sum + Number(row.count || 0), 0);
      expect(total).toBe(1000000);
    });

    it('should filter by array overlap with custom list', async () => {
      const sql = `
        SELECT COUNT(*) AS count
        FROM fact_all_types
        WHERE EXISTS (
          SELECT 1 
          FROM unnest(tags) as tag
          WHERE tag IN ('backend', 'urgent')
        )
      `;

      const result = await duckdbExec(sql);
      const count = Number(result[0]?.count || 0);

      // Rows with 'backend' OR 'urgent'
      // backend: 33%, urgent: 10%, overlap: 3.3%
      // Union: 33% + 10% - 3.3% = 39.7%
      expect(count).toBeGreaterThan(390000);
      expect(count).toBeLessThan(410000);
    });
  });
});

