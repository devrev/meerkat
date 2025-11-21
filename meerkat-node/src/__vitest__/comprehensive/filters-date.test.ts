/**
 * Comprehensive Date Filter Tests
 * 
 * Tests DATE field filtering with complex scenarios including:
 * - Date comparisons (=, !=, <, <=, >, >=)
 * - Date ranges (BETWEEN)
 * - Date IN lists
 * - NULL handling
 * - Complex nested queries with date ranges
 * - Date + other filter combinations
 * - Performance testing on 1M+ rows
 * - Year/month/day boundary conditions
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { cubeQueryToSQL } from '../../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../../duckdb-exec';
import { measureExecutionTime } from '../helpers/test-helpers';
import {
    createAllSyntheticTables,
    dropSyntheticTables,
    verifySyntheticTables,
} from '../synthetic/schema-setup';
import { FACT_ALL_TYPES_SCHEMA } from '../synthetic/table-schemas';

describe('Comprehensive: Date Filters', () => {
  beforeAll(async () => {
    console.log('🚀 Starting Vitest test suite...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('Basic Date Equality', () => {
    it('should filter date equals specific date', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'equals',
            values: ['2020-01-01'],
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

      // record_date = '2020-01-01' + (i % 1460) days, cycles every 4 years
      // Each date appears 1M / 1460 ≈ 685 times
      expect(count).toBeGreaterThan(680);
      expect(count).toBeLessThan(690);
    });

    it('should filter date notEquals specific date', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'notEquals',
            values: ['2020-01-01'],
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

      // Should exclude ~685 rows
      expect(count).toBeGreaterThan(999310);
      expect(count).toBeLessThan(999320);
    });
  });

  describe('Date Comparison Operators', () => {
    it('should filter date < specific date', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'lt',
            values: ['2020-01-31'],
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

      // Dates from 2020-01-01 to 2020-01-30 (30 days)
      // 30 days * 685 ≈ 20,548
      expect(count).toBeGreaterThan(20000);
      expect(count).toBeLessThan(21000);
    });

    it('should filter date <= specific date', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'lte',
            values: ['2020-01-31'],
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

      // Dates from 2020-01-01 to 2020-01-31 (31 days)
      expect(count).toBeGreaterThan(21000);
      expect(count).toBeLessThan(22000);
    });

    it('should filter date > specific date', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'gt',
            values: ['2023-12-01'],
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

      // Dates after 2023-12-01 in the 4-year cycle
      // Should be a small portion
      expect(count).toBeGreaterThan(0);
      expect(count).toBeLessThan(50000);
    });

    it('should filter date >= specific date', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_date',
            operator: 'gte',
            values: ['2020-12-01'],
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

      // created_date cycles every 365 days
      // From Dec 1 to Dec 31 (31 days) out of 365
      // 31/365 * 1M ≈ 84,932
      expect(count).toBeGreaterThan(80000);
      expect(count).toBeLessThan(90000);
    });
  });

  describe('Date Range Queries (BETWEEN)', () => {
    it('should filter date BETWEEN start and end', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'gte',
            values: ['2020-06-01'],
          },
          {
            member: 'fact_all_types.record_date',
            operator: 'lte',
            values: ['2020-06-30'],
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

      // June has 30 days, 30 * 685 ≈ 20,548
      expect(count).toBeGreaterThan(20000);
      expect(count).toBeLessThan(21000);
    });

    it('should filter date range spanning full year', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_date',
            operator: 'gte',
            values: ['2020-01-01'],
          },
          {
            member: 'fact_all_types.created_date',
            operator: 'lt',
            values: ['2021-01-01'],
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

      // created_date cycles every 365 days, so full year = all rows
      expect(count).toBe(1000000);
    });

    it('should filter date range with narrow window (1 week)', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'gte',
            values: ['2020-03-01'],
          },
          {
            member: 'fact_all_types.record_date',
            operator: 'lt',
            values: ['2020-03-08'],
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

      // 7 days * 685 ≈ 4,795
      expect(count).toBeGreaterThan(4700);
      expect(count).toBeLessThan(4900);
    });
  });

  describe('Date IN Lists', () => {
    it('should filter date IN with multiple specific dates', async () => {
      const dates = ['2020-01-01', '2020-01-15', '2020-02-01'];
      
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'in',
            values: dates,
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

      // 3 dates * 685 ≈ 2,055
      expect(count).toBeGreaterThan(2000);
      expect(count).toBeLessThan(2100);
    });

    it('should filter date NOT IN with exclusion list', async () => {
      const excludeDates = ['2020-01-01'];
      
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'notIn',
            values: excludeDates,
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

      // Should exclude ~685 rows
      expect(count).toBeGreaterThan(999310);
      expect(count).toBeLessThan(999320);
    });
  });

  describe('Date NULL Handling', () => {
    it('should filter date IS NOT NULL', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
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

      // All dates are set in our schema
      expect(count).toBe(1000000);
    });

    it('should filter date IS NULL', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
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

      // No NULL dates
      expect(count).toBe(0);
    });
  });

  describe('Complex Date Combinations', () => {
    it('should handle date range + boolean filter', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'gte',
            values: ['2020-01-01'],
          },
          {
            member: 'fact_all_types.record_date',
            operator: 'lt',
            values: ['2020-02-01'],
          },
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

      // January (31 days) * 685 * 50% (is_active) ≈ 10,608
      expect(count).toBeGreaterThan(10000);
      expect(count).toBeLessThan(11000);
    });

    it('should handle date range + string filter', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_date',
            operator: 'gte',
            values: ['2020-06-01'],
          },
          {
            member: 'fact_all_types.created_date',
            operator: 'lt',
            values: ['2020-07-01'],
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

      // June (30 days / 365) * 1M * 20% (priority=high) ≈ 16,438
      expect(count).toBeGreaterThan(16000);
      expect(count).toBeLessThan(17000);
    });

    it('should handle date range + numeric filter', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'gte',
            values: ['2020-01-01'],
          },
          {
            member: 'fact_all_types.record_date',
            operator: 'lt',
            values: ['2020-01-31'],
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

      // Complex intersection - should be small subset
      expect(count).toBeGreaterThan(0);
      expect(count).toBeLessThan(5000);
    });

    it('should handle overlapping date ranges on different fields', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'gte',
            values: ['2020-06-01'],
          },
          {
            member: 'fact_all_types.record_date',
            operator: 'lt',
            values: ['2020-07-01'],
          },
          {
            member: 'fact_all_types.created_date',
            operator: 'gte',
            values: ['2020-06-15'],
          },
          {
            member: 'fact_all_types.created_date',
            operator: 'lt',
            values: ['2020-07-15'],
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

      // Overlapping ranges - complex calculation
      expect(count).toBeGreaterThan(0);
      expect(count).toBeLessThan(30000);
    });
  });

  describe('Date Boundary Conditions', () => {
    it('should handle leap year date: 2020-02-29', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'equals',
            values: ['2020-02-29'],
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

      // 2020 is a leap year, so this date exists
      expect(count).toBeGreaterThan(680);
      expect(count).toBeLessThan(690);
    });

    it('should handle year boundary: 2020-12-31', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.created_date',
            operator: 'equals',
            values: ['2020-12-31'],
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

      // created_date cycles every 366 days (fixed to include Dec 31)
      // 1M / 366 ≈ 2,732 rows per date
      expect(count).toBeGreaterThan(2730);
      expect(count).toBeLessThan(2740);
    });

    it('should handle month boundary transitions correctly', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'gte',
            values: ['2020-01-31'],
          },
          {
            member: 'fact_all_types.record_date',
            operator: 'lte',
            values: ['2020-02-01'],
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

      // 2 days
      expect(count).toBeGreaterThan(1360);
      expect(count).toBeLessThan(1380);
    });
  });

  describe('Date Performance Tests', () => {
    it('should execute date range query quickly (< 500ms on 1M rows)', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'gte',
            values: ['2020-01-01'],
          },
          {
            member: 'fact_all_types.record_date',
            operator: 'lt',
            values: ['2020-12-31'],
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

      expect(count).toBeGreaterThan(0);
      expect(duration).toBeLessThan(500);
    });
  });

  describe('Date Edge Cases', () => {
    it('should handle empty date range (start > end)', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'gte',
            values: ['2020-12-31'],
          },
          {
            member: 'fact_all_types.record_date',
            operator: 'lt',
            values: ['2020-01-01'],
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

      // Invalid range should return 0
      expect(count).toBe(0);
    });

    it('should handle single-day range (same start and end)', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.record_date',
            operator: 'gte',
            values: ['2020-05-15'],
          },
          {
            member: 'fact_all_types.record_date',
            operator: 'lte',
            values: ['2020-05-15'],
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

      // Single day
      expect(count).toBeGreaterThan(680);
      expect(count).toBeLessThan(690);
    });
  });
});

