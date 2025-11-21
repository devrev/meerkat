/**
 * Comprehensive JSON Field Tests
 * 
 * Tests JSON field extraction and filtering:
 * - JSON path extraction
 * - JSON field filtering
 * - JSON_EXTRACT_STRING function
 * - Casting JSON to other types
 * - Aggregating over JSON fields
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: JSON Fields', () => {
  beforeAll(async () => {
    console.log('🚀 Starting JSON field tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('JSON Extraction', () => {
    it('should extract JSON field using JSON_EXTRACT_STRING', async () => {
      const sql = `
        SELECT 
          JSON_EXTRACT_STRING(metadata_json, '$.source') as source,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY JSON_EXTRACT_STRING(metadata_json, '$.source')
        ORDER BY source
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(row.source).toBeTruthy();
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should extract nested JSON field', async () => {
      const sql = `
        SELECT 
          JSON_EXTRACT_STRING(metadata_json, '$.reported_by') as reported_by,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY JSON_EXTRACT_STRING(metadata_json, '$.reported_by')
        ORDER BY count DESC
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(row.reported_by).toBeTruthy();
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should extract JSON field and cast to numeric', async () => {
      const sql = `
        SELECT 
          JSON_EXTRACT_STRING(metadata_json, '$.severity') as severity,
          CAST(JSON_EXTRACT_STRING(metadata_json, '$.severity') AS INTEGER) as severity_int,
          COUNT(*) as count
        FROM fact_all_types
        WHERE JSON_EXTRACT_STRING(metadata_json, '$.severity') IS NOT NULL
        GROUP BY JSON_EXTRACT_STRING(metadata_json, '$.severity')
        ORDER BY severity_int
        LIMIT 5
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(row.severity).toBeTruthy();
        expect(Number(row.severity_int)).toBeGreaterThanOrEqual(1);
        expect(Number(row.severity_int)).toBeLessThanOrEqual(5);
      });
    });
  });

  describe('JSON Filtering', () => {
    it('should filter by JSON field value', async () => {
      const sql = `
        SELECT 
          COUNT(*) as count
        FROM fact_all_types
        WHERE JSON_EXTRACT_STRING(metadata_json, '$.source') = 'web'
      `;
      const result = await duckdbExec(sql);

      // source cycles through 'web', 'mobile', 'api'
      // 'web' is 33% of rows
      expect(Number(result[0].count)).toBeGreaterThan(300000);
      expect(Number(result[0].count)).toBeLessThan(400000);
    });

    it('should filter by JSON numeric field', async () => {
      const sql = `
        SELECT 
          COUNT(*) as count
        FROM fact_all_types
        WHERE CAST(JSON_EXTRACT_STRING(metadata_json, '$.severity') AS INTEGER) >= 3
      `;
      const result = await duckdbExec(sql);

      // severity 3, 4, 5 = 60% of rows
      expect(Number(result[0].count)).toBeGreaterThan(550000);
      expect(Number(result[0].count)).toBeLessThan(650000);
    });

    it('should group by JSON field', async () => {
      const sql = `
        SELECT 
          JSON_EXTRACT_STRING(metadata_json, '$.source') as source,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY JSON_EXTRACT_STRING(metadata_json, '$.source')
        ORDER BY source
      `;
      const result = await duckdbExec(sql);

      // Should have 3 sources: api, mobile, web
      expect(result.length).toBe(3);

      const sources = result.map((r) => r.source);
      expect(sources).toContain('api');
      expect(sources).toContain('mobile');
      expect(sources).toContain('web');

      // Each source should have ~333K rows
      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(300000);
        expect(Number(row.count)).toBeLessThan(370000);
      });
    });
  });

  describe('JSON with Aggregates', () => {
    it('should aggregate by JSON field', async () => {
      const sql = `
        SELECT 
          JSON_EXTRACT_STRING(metadata_json, '$.source') as source,
          COUNT(*) as count,
          AVG(metric_double) as avg_metric,
          SUM(id_bigint) as total_ids
        FROM fact_all_types
        GROUP BY JSON_EXTRACT_STRING(metadata_json, '$.source')
        ORDER BY source
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);

      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(0);
        expect(Number(row.avg_metric)).toBeGreaterThan(0);
        expect(Number(row.total_ids)).toBeGreaterThan(0);
      });
    });

    it('should use JSON field in complex aggregation', async () => {
      const sql = `
        SELECT 
          JSON_EXTRACT_STRING(metadata_json, '$.source') as source,
          priority,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY JSON_EXTRACT_STRING(metadata_json, '$.source'), priority
        ORDER BY source, priority
      `;
      const result = await duckdbExec(sql);

      // 3 sources * 5 priorities = 15 combinations
      expect(result.length).toBe(15);

      result.forEach((row) => {
        expect(row.source).toMatch(/^(api|mobile|web)$/);
        expect(row.priority).toMatch(/^(low|medium|high|critical|urgent)$/);
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });
  });

  describe('JSON with JOINs', () => {
    it('should join with JSON field extraction', async () => {
      const sql = `
        SELECT 
          JSON_EXTRACT_STRING(f.metadata_json, '$.source') as source,
          u.user_segment,
          COUNT(*) as count
        FROM fact_all_types f
        INNER JOIN dim_user u ON f.user_id = u.user_id
        GROUP BY JSON_EXTRACT_STRING(f.metadata_json, '$.source'), u.user_segment
        ORDER BY source, user_segment
      `;
      const result = await duckdbExec(sql);

      // 3 sources * 3 segments = 9 combinations
      expect(result.length).toBe(9);

      result.forEach((row) => {
        expect(row.source).toMatch(/^(api|mobile|web)$/);
        expect(row.user_segment).toMatch(/^(enterprise|pro|free)$/);
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });
  });

  describe('JSON NULL Handling', () => {
    it('should handle NULL JSON fields', async () => {
      const sql = `
        SELECT 
          COUNT(*) as total,
          SUM(CASE WHEN metadata_json IS NULL THEN 1 ELSE 0 END) as null_count,
          SUM(CASE WHEN metadata_json IS NOT NULL THEN 1 ELSE 0 END) as non_null_count
        FROM fact_all_types
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].total)).toBe(1000000);
      
      // metadata_json is never NULL (always populated)
      expect(Number(result[0].null_count)).toBe(0);
      expect(Number(result[0].non_null_count)).toBe(1000000);
    });

    it('should handle missing JSON keys', async () => {
      const sql = `
        SELECT 
          COUNT(*) as total,
          COUNT(JSON_EXTRACT_STRING(metadata_json, '$.nonexistent_key')) as with_key
        FROM fact_all_types
        LIMIT 1
      `;
      const result = await duckdbExec(sql);

      // Nonexistent keys return NULL, COUNT ignores NULLs
      expect(Number(result[0].total)).toBe(1000000);
      expect(Number(result[0].with_key)).toBe(0);
    });
  });

  describe('JSON String Operations', () => {
    it('should use LIKE on JSON extracted strings', async () => {
      const sql = `
        SELECT 
          COUNT(*) as count
        FROM fact_all_types
        WHERE JSON_EXTRACT_STRING(metadata_json, '$.reported_by') LIKE '%user_%'
      `;
      const result = await duckdbExec(sql);

      // All reported_by values follow 'user_X' pattern
      expect(Number(result[0].count)).toBe(1000000);
    });

    it('should concatenate JSON fields', async () => {
      const sql = `
        SELECT 
          CONCAT(
            JSON_EXTRACT_STRING(metadata_json, '$.source'),
            '-',
            priority
          ) as combined,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY 
          CONCAT(
            JSON_EXTRACT_STRING(metadata_json, '$.source'),
            '-',
            priority
          )
        ORDER BY combined
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(row.combined).toContain('-');
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });
  });

  describe('Performance', () => {
    it('should extract JSON fields quickly (< 500ms)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          JSON_EXTRACT_STRING(metadata_json, '$.source') as source,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY JSON_EXTRACT_STRING(metadata_json, '$.source')
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(500);
    });

    it('should filter by JSON field quickly (< 500ms)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          COUNT(*) as count,
          AVG(metric_double) as avg_metric
        FROM fact_all_types
        WHERE JSON_EXTRACT_STRING(metadata_json, '$.source') = 'web'
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(500);
    });
  });
});

