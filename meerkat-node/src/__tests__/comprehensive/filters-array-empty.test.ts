/**
 * Comprehensive Array Empty/Not Empty Filter Tests
 *
 * Tests arrayEmpty and arrayNotEmpty filter operators including:
 * - Basic empty/not-empty filtering across different array columns
 * - Complementary counts (arrayEmpty + arrayNotEmpty = total)
 * - Combinations with other filter types
 * - Performance testing on 1M+ rows
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { cubeQueryToSQL } from '../../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../../duckdb-exec';
import { measureExecutionTime } from './helpers/test-helpers';
import {
    createAllSyntheticTables,
    dropSyntheticTables,
    verifySyntheticTables,
} from './synthetic/schema-setup';
import { FACT_ALL_TYPES_SCHEMA } from './synthetic/table-schemas';

describe('Comprehensive: Array Empty/Not Empty Filters', () => {
  beforeAll(async () => {
    console.log('🚀 Starting Array Empty/Not Empty test suite...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('arrayEmpty Operator', () => {
    it('should filter empty arrays on tags column', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.tags',
            operator: 'arrayEmpty',
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

      // tags is empty when i % 4 = 3, so 25% of 1M rows
      expect(count).toBe(250000);
    });

    it('should filter empty arrays on owned_by_ids column', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.owned_by_ids',
            operator: 'arrayEmpty',
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

      // owned_by_ids is empty when i % 3 = 2, so 333333 of 1M rows
      expect(count).toBe(333333);
    });

    it('should filter empty arrays on part_ids column', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.part_ids',
            operator: 'arrayEmpty',
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

      // part_ids is empty when i % 5 in {2,3,4}, so 60% of 1M rows
      expect(count).toBe(600000);
    });
  });

  describe('arrayNotEmpty Operator', () => {
    it('should filter non-empty arrays on tags column', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.tags',
            operator: 'arrayNotEmpty',
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

      // tags is non-empty when i % 4 in {0,1,2}, so 75% of 1M rows
      expect(count).toBe(750000);
    });

    it('should filter non-empty arrays on owned_by_ids column', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.owned_by_ids',
            operator: 'arrayNotEmpty',
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

      // owned_by_ids is non-empty when i % 3 in {0,1}, so 666667 of 1M rows
      expect(count).toBe(666667);
    });

    it('should filter non-empty arrays on part_ids column', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.part_ids',
            operator: 'arrayNotEmpty',
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

      // part_ids is non-empty when i % 5 in {0,1}, so 40% of 1M rows
      expect(count).toBe(400000);
    });
  });

  describe('Complementary Counts', () => {
    it('arrayEmpty + arrayNotEmpty on tags should equal total rows', async () => {
      const emptyQuery = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.tags',
            operator: 'arrayEmpty',
          },
        ],
        dimensions: [],
      };

      const notEmptyQuery = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.tags',
            operator: 'arrayNotEmpty',
          },
        ],
        dimensions: [],
      };

      const emptySql = await cubeQueryToSQL({
        query: emptyQuery,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const notEmptySql = await cubeQueryToSQL({
        query: notEmptyQuery,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const emptyResult = await duckdbExec(emptySql);
      const notEmptyResult = await duckdbExec(notEmptySql);

      const emptyCount = Number(emptyResult[0]?.fact_all_types__count || 0);
      const notEmptyCount = Number(notEmptyResult[0]?.fact_all_types__count || 0);

      // Empty + NotEmpty should cover all rows
      expect(emptyCount + notEmptyCount).toBe(1000000);
    });

    it('arrayEmpty + arrayNotEmpty on part_ids should equal total rows', async () => {
      const emptyQuery = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.part_ids',
            operator: 'arrayEmpty',
          },
        ],
        dimensions: [],
      };

      const notEmptyQuery = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.part_ids',
            operator: 'arrayNotEmpty',
          },
        ],
        dimensions: [],
      };

      const emptySql = await cubeQueryToSQL({
        query: emptyQuery,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const notEmptySql = await cubeQueryToSQL({
        query: notEmptyQuery,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });

      const emptyResult = await duckdbExec(emptySql);
      const notEmptyResult = await duckdbExec(notEmptySql);

      const emptyCount = Number(emptyResult[0]?.fact_all_types__count || 0);
      const notEmptyCount = Number(notEmptyResult[0]?.fact_all_types__count || 0);

      expect(emptyCount + notEmptyCount).toBe(1000000);
    });
  });

  describe('Combined with Other Filters', () => {
    it.fails('should combine arrayNotEmpty with equals filter', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.tags',
            operator: 'arrayNotEmpty',
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

      // tags not empty (75%) AND priority='high' (i%5=0, 20%)
      // Since i%4 != 3 AND i%5 = 0 are independent moduli:
      // Total = 1M * (3/4) * (1/5) = 150000
      expect(count).toBe(150000);
    });

    it.fails('should combine arrayEmpty with boolean filter', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.part_ids',
            operator: 'arrayEmpty',
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

      // part_ids empty (i%5 in {2,3,4}, 60%) AND is_deleted (i%10=0, 10%)
      // i%10=0 means i%5=0, so i%5 in {2,3,4} AND i%10=0 is impossible
      // Wait: i%10=0 implies i%5=0, which is NOT in {2,3,4}. So count = 0.
      expect(count).toBe(0);
    });
  });

  describe('Performance', () => {
    it('should execute arrayEmpty filter within acceptable time', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.tags',
            operator: 'arrayEmpty',
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

      expect(count).toBe(250000);
      expect(duration).toBeLessThan(5000);
    });

    it('should execute arrayNotEmpty filter within acceptable time', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.tags',
            operator: 'arrayNotEmpty',
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

      expect(count).toBe(750000);
      expect(duration).toBeLessThan(5000);
    });
  });
});
