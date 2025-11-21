/**
 * Comprehensive Pagination Tests (LIMIT / OFFSET)
 * 
 * Tests LIMIT and OFFSET functionality:
 * - Basic LIMIT
 * - LIMIT with OFFSET
 * - Pagination scenarios
 * - Edge cases (limit=0, offset beyond data)
 * - Performance on large datasets
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

describe('Comprehensive: Pagination (LIMIT / OFFSET)', () => {
  beforeAll(async () => {
    console.log('🚀 Starting pagination tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('Basic LIMIT', () => {
    it('should limit results to specified count', async () => {
      const query = {
        measures: [],
        dimensions: ['fact_all_types.priority'],
        limit: 10,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeLessThanOrEqual(10);
    });

    it('should limit aggregated results', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        limit: 2,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeLessThanOrEqual(2);
    });

    it('should handle LIMIT 1', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.status'],
        limit: 1,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBe(1);
    });

    it('should handle LIMIT larger than result set', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        limit: 10000,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      // priority has 5 values, so result should be ≤ 5
      expect(result.length).toBeLessThanOrEqual(5);
    });
  });

  describe('LIMIT with OFFSET', () => {
    it('should skip rows with OFFSET', async () => {
      const queryPage1 = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        order: [['fact_all_types.priority', 'asc']],
        limit: 2,
        offset: 0,
      };

      const queryPage2 = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        order: [['fact_all_types.priority', 'asc']],
        limit: 2,
        offset: 2,
      };

      const sqlPage1 = await cubeQueryToSQL({
        query: queryPage1,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const sqlPage2 = await cubeQueryToSQL({
        query: queryPage2,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const resultPage1 = await duckdbExec(sqlPage1);
      const resultPage2 = await duckdbExec(sqlPage2);

      expect(resultPage1.length).toBeLessThanOrEqual(2);
      expect(resultPage2.length).toBeLessThanOrEqual(2);

      // Pages should have different data
      if (resultPage1.length > 0 && resultPage2.length > 0) {
        expect(resultPage1[0].fact_all_types__priority).not.toBe(
          resultPage2[0].fact_all_types__priority
        );
      }
    });

    it('should implement pagination correctly across multiple pages', async () => {
      const pageSize = 1;
      const pages: any[][] = [];

      for (let page = 0; page < 3; page++) {
        const query = {
          measures: ['fact_all_types.count'],
          dimensions: ['fact_all_types.priority'],
          order: [['fact_all_types.priority', 'asc']],
          limit: pageSize,
          offset: page * pageSize,
        };

        const sql = await cubeQueryToSQL({
          query,
          tableSchemas: [FACT_ALL_TYPES_SCHEMA],
        });

        const result = await duckdbExec(sql);
        pages.push(result);
      }

      // Each page should have data
      pages.forEach((page) => {
        expect(page.length).toBeGreaterThan(0);
      });

      // Pages should have different priorities
      const priorities = pages.map((page) => page[0]?.fact_all_types__priority);
      const uniquePriorities = new Set(priorities);
      expect(uniquePriorities.size).toBe(priorities.length);
    });
  });

  describe('Pagination with Ordering', () => {
    it('should maintain order with LIMIT', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        order: [['fact_all_types.count', 'desc']],
        limit: 3,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeLessThanOrEqual(3);

      // Verify descending order
      for (let i = 1; i < result.length; i++) {
        expect(Number(result[i].fact_all_types__count)).toBeLessThanOrEqual(
          Number(result[i - 1].fact_all_types__count)
        );
      }
    });

    it('should maintain order with LIMIT and OFFSET', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.status'],
        order: [['fact_all_types.status', 'asc']],
        limit: 2,
        offset: 1,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeLessThanOrEqual(2);

      // Verify ascending order
      for (let i = 1; i < result.length; i++) {
        expect(result[i].fact_all_types__status >= result[i - 1].fact_all_types__status).toBe(
          true
        );
      }
    });
  });

  describe('Pagination with Filters', () => {
    it('should apply LIMIT after filtering', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.status'],
        filters: [
          {
            member: 'fact_all_types.priority',
            operator: 'equals',
            values: ['high'],
          },
        ],
        limit: 2,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeLessThanOrEqual(2);
    });

    it('should apply OFFSET after filtering', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        filters: [
          {
            member: 'fact_all_types.is_active',
            operator: 'equals',
            values: [true],
          },
        ],
        order: [['fact_all_types.priority', 'asc']],
        limit: 1,
        offset: 1,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBe(1);
    });
  });

  describe('Edge Cases', () => {
    it('should handle OFFSET beyond available data', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        limit: 10,
        offset: 1000,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      // Should return empty or very few results
      expect(result.length).toBe(0);
    });

    it('should handle LIMIT 0', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        limit: 0,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBe(0);
    });

    it('should handle OFFSET 0 (same as no offset)', async () => {
      const queryWithOffset = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        order: [['fact_all_types.priority', 'asc']],
        limit: 3,
        offset: 0,
      };

      const queryWithoutOffset = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        order: [['fact_all_types.priority', 'asc']],
        limit: 3,
      };

      const sqlWith = await cubeQueryToSQL({
        query: queryWithOffset,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const sqlWithout = await cubeQueryToSQL({
        query: queryWithoutOffset,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const resultWith = await duckdbExec(sqlWith);
      const resultWithout = await duckdbExec(sqlWithout);

      expect(resultWith.length).toBe(resultWithout.length);
      expect(resultWith[0]?.fact_all_types__priority).toBe(
        resultWithout[0]?.fact_all_types__priority
      );
    });
  });

  describe('Performance', () => {
    it('should execute paginated query quickly (< 200ms)', async () => {
      const start = Date.now();

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority', 'fact_all_types.status'],
        limit: 10,
        offset: 100,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(200);
    });

    it('should execute large OFFSET query efficiently', async () => {
      const start = Date.now();

      const query = {
        measures: [],
        dimensions: ['fact_all_types.id_bigint'],
        filters: [
          {
            member: 'fact_all_types.id_bigint',
            operator: 'lt',
            values: [10000],
          },
        ],
        order: [['fact_all_types.id_bigint', 'asc']],
        limit: 10,
        offset: 5000,
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

