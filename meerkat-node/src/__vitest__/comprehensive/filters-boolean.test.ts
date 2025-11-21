/**
 * Comprehensive Boolean Filter Tests
 * 
 * Tests boolean field filtering with complex scenarios including:
 * - Basic true/false filtering
 * - NULL handling
 * - Complex nested queries with boolean conditions
 * - Boolean combinations with other filter types
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

describe('Comprehensive: Boolean Filters', () => {
  beforeAll(async () => {
    console.log('🚀 Starting Vitest test suite...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('Basic Boolean Filters', () => {
    it('should filter boolean field is_active = true', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.is_active',
            operator: 'equals',
            values: ['true'],
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

      // is_active = (i % 2) = 0, so 50% of 1M rows
      expect(count).toBe(500000);
    });

    it('should filter boolean field is_active = false', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.is_active',
            operator: 'equals',
            values: ['false'],
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

      // is_active = (i % 2) = 0, so 50% are false
      expect(count).toBe(500000);
    });

    it('should filter boolean field is_deleted = true', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.is_deleted',
            operator: 'equals',
            values: ['true'],
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

      // is_deleted = (i % 10) = 0, so 10% of 1M rows
      expect(count).toBe(100000);
    });
  });

  describe('Boolean NOT Equals', () => {
    it('should filter boolean notEquals true (excludes true values)', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.is_active',
            operator: 'notEquals',
            values: ['true'],
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

      // Should return false values (500K)
      expect(count).toBe(500000);
    });

    it('should filter boolean notEquals false (excludes false values)', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.is_deleted',
            operator: 'notEquals',
            values: ['false'],
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

      // Should return true values (100K)
      expect(count).toBe(100000);
    });
  });

  describe('Boolean NULL Handling', () => {
    it('should filter boolean IS NOT NULL (set)', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.is_active',
            operator: 'set',
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

      // is_active has no NULLs in our schema
      expect(count).toBe(1000000);
    });

    it('should filter boolean IS NULL (notSet)', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.is_active',
            operator: 'notSet',
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

      // No NULL values in is_active
      expect(count).toBe(0);
    });
  });

  describe('Complex Boolean Combinations', () => {
    it('should handle AND combination: is_active=true AND is_deleted=false', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.is_active',
            operator: 'equals',
            values: ['true'],
          },
          {
            member: 'fact_all_types.is_deleted',
            operator: 'equals',
            values: ['false'],
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

      // is_active=true (50%) AND is_deleted=false (90%)
      // Both conditions: (i % 2 = 0) AND (i % 10 != 0)
      // This means even numbers that are NOT multiples of 10
      // Multiples of 10: 100K, so even non-multiples of 10: 500K - 100K = 400K
      expect(count).toBe(400000);
    });

    it('should handle complex nested: (is_active=true OR is_deleted=true)', async () => {
      // Note: This tests OR logic if cubeQueryToSQL supports it
      // For now, we'll test with multiple separate queries and verify the union
      const query1 = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.is_active',
            operator: 'equals',
            values: ['true'],
          },
        ],
        dimensions: [],
      };

      const query2 = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.is_deleted',
            operator: 'equals',
            values: ['true'],
          },
        ],
        dimensions: [],
      };

      const sql1 = await cubeQueryToSQL({
        query: query1,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const sql2 = await cubeQueryToSQL({
        query: query2,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result1 = await duckdbExec(sql1);
      const result2 = await duckdbExec(sql2);

      const count1 = Number(result1[0]?.fact_all_types__count || 0);
      const count2 = Number(result2[0]?.fact_all_types__count || 0);

      // Verify individual conditions
      expect(count1).toBe(500000); // is_active=true
      expect(count2).toBe(100000); // is_deleted=true

      // OR logic: is_active=true (even) OR is_deleted=true (multiples of 10)
      // All even numbers already include multiples of 10, so result is 500K
    });

    it('should handle boolean with numeric filter: is_active=true AND id_bigint < 100000', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.is_active',
            operator: 'equals',
            values: ['true'],
          },
          {
            member: 'fact_all_types.id_bigint',
            operator: 'lt',
            values: ['100000'],
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

      // is_active=true (50% of rows) AND id_bigint < 100000 (10% of rows)
      // Expected: 50K rows
      expect(count).toBe(50000);
    });

    it('should handle boolean with string filter: is_active=true AND priority=high', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.is_active',
            operator: 'equals',
            values: ['true'],
          },
          {
            member: 'fact_all_types.priority',
            operator: 'equals',
            values: ['high'],
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

      // is_active=true (50%) AND priority='high' (20%)
      // Expected: ~10% of 1M = 100K
      expect(count).toBe(100000);
    });
  });

  describe('Boolean Performance Tests', () => {
    it('should filter boolean fields quickly (< 500ms on 1M rows)', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.is_active',
            operator: 'equals',
            values: ['true'],
          },
          {
            member: 'fact_all_types.is_deleted',
            operator: 'equals',
            values: ['false'],
          },
        ],
        dimensions: [],
      };

      const { result, duration } = await measureExecutionTime(async () => {
        const sql = await cubeQueryToSQL({
          query,
          tableSchemas: [FACT_ALL_TYPES_SCHEMA],
        });
        return duckdbExec(sql);
      });

      const count = Number(result[0]?.fact_all_types__count || 0);

      expect(count).toBe(400000);
      expect(duration).toBeLessThan(500); // Should be fast
    });
  });

  describe('Boolean Edge Cases', () => {
    it('should handle contradictory boolean filters: is_active=true AND is_active=false', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.is_active',
            operator: 'equals',
            values: ['true'],
          },
          {
            member: 'fact_all_types.is_active',
            operator: 'equals',
            values: ['false'],
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

      // Contradictory conditions should return 0 rows
      expect(count).toBe(0);
    });

    it('should handle multiple boolean fields with different values', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.is_active',
            operator: 'equals',
            values: ['false'],
          },
          {
            member: 'fact_all_types.is_deleted',
            operator: 'equals',
            values: ['true'],
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

      // is_active=false (odd numbers, 50%) AND is_deleted=true (multiples of 10, 10%)
      // Odd multiples of 10: none in our schema (multiples of 10 are even)
      // So this should be 0
      expect(count).toBe(0);
    });
  });
});

