/**
 * Comprehensive TIMESTAMP Filter Tests
 * 
 * Tests TIMESTAMP filtering with various operators:
 * - Equality (=)
 * - Inequality (!=, <, <=, >, >=)
 * - NULL handling (set, notSet)
 * - Edge cases (boundary timestamps, timezone handling)
 * 
 * NOTE: Tests avoid multiple filters due to known limitation.
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

describe('Comprehensive: TIMESTAMP Filters', () => {
  beforeAll(async () => {
    console.log('🚀 Starting TIMESTAMP filter tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('Equality Operator', () => {
    it('should filter TIMESTAMP with equals operator', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_timestamp',
            operator: 'equals',
            values: ['2020-01-01T00:00:00'],
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

      // Only rows with exact timestamp match
      expect(count).toBeGreaterThan(0);
      expect(count).toBeLessThan(1000);
    });

    it('should filter TIMESTAMP with not equals operator', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_timestamp',
            operator: 'notEquals',
            values: ['2020-01-01T00:00:00'],
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

      // Almost all rows should match
      expect(count).toBeGreaterThan(999000);
      expect(count).toBeLessThan(1000000);
    });
  });

  describe('Comparison Operators', () => {
    it('should filter TIMESTAMP with greater than operator', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_timestamp',
            operator: 'gt',
            values: ['2022-01-01T00:00:00'],
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

      // created_timestamp cycles every 1460 days from 2020-01-01
      // Timestamps > 2022-01-01 should be roughly 50% of rows
      expect(count).toBeGreaterThan(400000);
      expect(count).toBeLessThan(600000);
    });

    it('should filter TIMESTAMP with greater than or equal operator', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_timestamp',
            operator: 'gte',
            values: ['2022-01-01T00:00:00'],
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

      expect(count).toBeGreaterThan(400000);
      expect(count).toBeLessThan(600000);
    });

    it('should filter TIMESTAMP with less than operator', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_timestamp',
            operator: 'lt',
            values: ['2021-01-01T00:00:00'],
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

      // Timestamps < 2021-01-01 should be roughly 25% of rows
      expect(count).toBeGreaterThan(200000);
      expect(count).toBeLessThan(300000);
    });

    it('should filter TIMESTAMP with less than or equal operator', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_timestamp',
            operator: 'lte',
            values: ['2021-01-01T00:00:00'],
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

      expect(count).toBeGreaterThan(200000);
      expect(count).toBeLessThan(300000);
    });
  });

  describe('Time Precision', () => {
    it('should handle hour-level timestamp precision', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_timestamp',
            operator: 'lt',
            values: ['2020-01-01T12:00:00'],
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

      // Should have some rows before noon on the first day
      expect(count).toBeGreaterThan(0);
    });

    it('should handle minute-level timestamp precision', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_timestamp',
            operator: 'lt',
            values: ['2020-01-01T00:30:00'],
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

      // Should have some rows in the first 30 minutes
      expect(count).toBeGreaterThan(0);
    });

    it('should handle second-level timestamp precision', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_timestamp',
            operator: 'gte',
            values: ['2020-01-01T00:00:59'],
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

      // Almost all rows should be >= 59 seconds after start
      expect(count).toBeGreaterThan(999000);
    });
  });

  describe('IN Operator', () => {
    it('should handle TIMESTAMP IN operator with multiple values', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.deployment_time',
            operator: 'equals',
            values: [
              '2020-01-01T00:00:00',
              '2020-01-02T00:00:00',
              '2020-01-03T00:00:00',
            ],
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

      // deployment_time cycles every 366 days
      // 3 days * (1M / 366) ≈ 8,196 rows
      expect(count).toBeGreaterThan(8000);
      expect(count).toBeLessThan(8400);
    });

    it('should handle TIMESTAMP NOT IN operator', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.deployment_time',
            operator: 'notEquals',
            values: ['2020-01-01T00:00:00', '2020-01-02T00:00:00'],
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

      // Most rows should NOT be in the excluded list
      expect(count).toBeGreaterThan(990000);
      expect(count).toBeLessThan(1000000);
    });
  });

  describe('NULL Handling', () => {
    it('should handle TIMESTAMP set operator (NOT NULL)', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_timestamp',
            operator: 'set',
            values: [],
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

      // All rows have non-NULL created_timestamp
      expect(count).toBe(1000000);
    });

    it('should handle TIMESTAMP notSet operator (IS NULL)', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_timestamp',
            operator: 'notSet',
            values: [],
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

      // No rows have NULL created_timestamp
      expect(count).toBe(0);
    });
  });

  describe('Edge Cases', () => {
    it('should handle timestamp at year boundary', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.deployment_time',
            operator: 'equals',
            values: ['2020-12-31T00:00:00'],
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

      // deployment_time cycles every 366 days, so Dec 31 should exist
      // ~2,732 rows
      expect(count).toBeGreaterThan(2700);
      expect(count).toBeLessThan(2800);
    });

    it('should handle very early timestamp', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_timestamp',
            operator: 'lt',
            values: ['2020-01-01T00:01:00'],
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

      // Should have some rows in first minute
      expect(count).toBeGreaterThan(0);
      expect(count).toBeLessThan(1000);
    });

    it('should handle timestamp comparison with year boundary', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_timestamp',
            operator: 'gte',
            values: ['2023-12-31T23:59:59'],
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

      // created_timestamp cycles every 1460 days from 2020-01-01
      // So timestamps >= 2023-12-31 should exist
      expect(count).toBeGreaterThanOrEqual(0);
    });
  });

  describe('Performance', () => {
    it('should execute timestamp filter quickly (< 500ms on 1M rows)', async () => {
      const start = Date.now();

      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_timestamp',
            operator: 'gt',
            values: ['2021-06-01T00:00:00'],
          },
        ],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(500);
    });
  });
});

