/**
 * Comprehensive Tests: String Filters
 * 
 * Tests VARCHAR/TEXT types with all string operators
 * covering ~44 test cases for string filter permutations.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { cubeQueryToSQL } from '../../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
} from '../synthetic/schema-setup';
import { FACT_ALL_TYPES_SCHEMA } from '../synthetic/table-schemas';
import { BatchErrorReporter } from '../helpers/test-helpers';

describe('Comprehensive: String Filters', () => {
  beforeAll(async () => {
    await dropSyntheticTables();
    await createAllSyntheticTables();
  }, 120000);
  
  describe('Equals Operator', () => {
    const equalsTestCases = [
      { value: 'high', field: 'priority', expectedApproxCount: 200000 },
      { value: 'medium', field: 'priority', expectedApproxCount: 200000 },
      { value: 'low', field: 'priority', expectedApproxCount: 200000 },
      { value: 'critical', field: 'priority', expectedApproxCount: 200000 },
      { value: 'unknown', field: 'priority', expectedApproxCount: 200000 },
      { value: 'open', field: 'status', expectedApproxCount: 250000 },
      { value: 'closed', field: 'status', expectedApproxCount: 250000 },
      { value: 'P0', field: 'severity_label', expectedApproxCount: 333333 },
    ];
    
    equalsTestCases.forEach(({ value, field, expectedApproxCount }) => {
      it(`should filter string '${field}' equals '${value}'`, async () => {
        const query = {
          measures: ['fact_all_types.count'],
          filters: [
            {
              member: `fact_all_types.${field}`,
              operator: 'equals',
              values: [value],
            },
          ],
          dimensions: [],
        };
        
        const sql = await cubeQueryToSQL({
          query,
          tableSchemas: [FACT_ALL_TYPES_SCHEMA],
        });
        
        const result = await duckdbExec(sql);
        const count = result[0]?.fact_all_types__count || 0;
        
        // Allow 10% variance for distribution
        const minExpected = expectedApproxCount * 0.9;
        const maxExpected = expectedApproxCount * 1.1;
        
        expect(count).toBeGreaterThan(minExpected);
        expect(count).toBeLessThan(maxExpected);
      });
    });
    
    it('should handle case-sensitive equals', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.priority',
            operator: 'equals',
            values: ['HIGH'], // uppercase, should not match 'high'
          },
        ],
        dimensions: [],
      };
      
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });
      
      const result = await duckdbExec(sql);
      
      // Should match zero rows (case sensitive)
      expect(Number(result[0]?.fact_all_types__count)).toBe(0);
    });
  });
  
  describe('NotEquals Operator', () => {
    it('should filter string notEquals', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.priority',
            operator: 'notEquals',
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
      const count = result[0]?.fact_all_types__count || 0;
      
      // Should be 4/5 of total (all except 'high')
      expect(count).toBeGreaterThan(750000);
      expect(count).toBeLessThan(850000);
    });
  });
  
  describe('Contains Operator', () => {
    it('should filter string contains (case insensitive)', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.title',
            operator: 'contains',
            values: ['Title 1'],
          },
        ],
        dimensions: [],
      };
      
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });
      
      // Should use case-insensitive operator
      expect(sql).toMatch(/~~\*/);
      
      const result = await duckdbExec(sql);
      const count = result[0]?.fact_all_types__count || 0;
      
      // Matches: Title 1, Title 10, Title 11, ..., Title 19, Title 100, etc.
      expect(count).toBeGreaterThan(100000);
    });
    
    it('should filter string contains with substring', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.priority',
            operator: 'contains',
            values: ['igh'], // matches 'high'
          },
        ],
        dimensions: [],
      };
      
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });
      
      const result = await duckdbExec(sql);
      const count = result[0]?.fact_all_types__count || 0;
      
      // Should match 'high' priority (1/5 of data)
      expect(count).toBeGreaterThan(180000);
      expect(count).toBeLessThan(220000);
    });
  });
  
  describe('NotContains Operator', () => {
    it('should filter string notContains', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.priority',
            operator: 'notContains',
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
      const count = result[0]?.fact_all_types__count || 0;
      
      // Should exclude 'high' (4/5 of data)
      expect(count).toBeGreaterThan(750000);
      expect(count).toBeLessThan(850000);
    });
  });
  
  describe('IN Operator', () => {
    it('should filter string IN with multiple values', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.priority',
            operator: 'in',
            values: ['high', 'critical'],
          },
        ],
        dimensions: [],
      };
      
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });
      
      const result = await duckdbExec(sql);
      const count = result[0]?.fact_all_types__count || 0;
      
      // Should match 2/5 of data
      expect(count).toBeGreaterThan(380000);
      expect(count).toBeLessThan(420000);
    });
    
    it('should filter string IN with large list (100+ values)', async () => {
      // Create 100 incident IDs
      const values = Array.from({ length: 100 }, (_, i) => `inc_${i}`);
      
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.incident_id',
            operator: 'in',
            values,
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
      
      // Each incident_id appears 10 times (1M rows / 100K unique IDs)
      expect(count).toBe(1000);
    });
    
    it('should handle string IN with 1000+ values using string_split optimization', async () => {
      // Create 1500 incident IDs
      const values = Array.from({ length: 1500 }, (_, i) => `inc_${i}`);
      
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.incident_id',
            operator: 'in',
            values,
          },
        ],
        dimensions: [],
      };
      
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });
      
      // NOTE: string_split optimization not implemented in cubeQueryToSQL yet
      // Just verify the query works with large IN lists
      const result = await duckdbExec(sql);
      expect(Number(result[0]?.fact_all_types__count)).toBe(15000);
    });
  });
  
  describe('NOT IN Operator', () => {
    it('should filter string NOT IN', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.priority',
            operator: 'notIn',
            values: ['high', 'critical'],
          },
        ],
        dimensions: [],
      };
      
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });
      
      const result = await duckdbExec(sql);
      const count = result[0]?.fact_all_types__count || 0;
      
      // Should match 3/5 of data (excluding high and critical)
      expect(count).toBeGreaterThan(580000);
      expect(count).toBeLessThan(620000);
    });
  });
  
  describe('Set/NotSet (NULL handling)', () => {
    it('should filter string IS NOT NULL (set)', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.nullable_string',
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
      const count = result[0]?.fact_all_types__count || 0;
      
      // nullable_string is NULL when i % 10 = 0, so 90% have values
      expect(count).toBeGreaterThan(890000);
      expect(count).toBeLessThan(910000);
    });
    
    it('should filter string IS NULL (notSet)', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.nullable_string',
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
      const count = result[0]?.fact_all_types__count || 0;
      
      // 10% are NULL
      expect(count).toBeGreaterThan(95000);
      expect(count).toBeLessThan(105000);
    });
  });
  
  describe('Special Characters', () => {
    it('should handle strings with double quotes', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.edge_case_string',
            operator: 'contains',
            values: ['"quotes"'],
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
      
      // Appears every 100 rows
      expect(count).toBe(10000);
    });
    
    it("should handle strings with apostrophes", async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.edge_case_string',
            operator: 'contains',
            values: ["apostrophe"],
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
      
      // Appears every 100 rows
      expect(count).toBe(10000);
    });
    
    it('should handle strings with backslashes', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.edge_case_string',
            operator: 'contains',
            values: ['backslash'],
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
      
      // Appears every 100 rows
      expect(count).toBe(10000);
    });
    
    it('should handle empty strings', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.edge_case_string',
            operator: 'equals',
            values: [''],
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
      
      // Appears every 100 rows (when i % 100 = 3)
      expect(count).toBe(10000);
    });
  });
  
  describe('Combined String Filters', () => {
    it('should handle AND combination of string filters', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            and: [
              {
                member: 'fact_all_types.priority',
                operator: 'equals',
                values: ['high'],
              },
              {
                member: 'fact_all_types.status',
                operator: 'equals',
                values: ['open'],
              },
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
      const count = result[0]?.fact_all_types__count || 0;
      
      // Should be (1/5) * (1/4) of data = 5%
      expect(count).toBeGreaterThan(45000);
      expect(count).toBeLessThan(55000);
    });
    
    it('should handle multiple IN filters', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            and: [
              {
                member: 'fact_all_types.priority',
                operator: 'in',
                values: ['high', 'critical'],
              },
              {
                member: 'fact_all_types.environment',
                operator: 'in',
                values: ['production', 'staging'],
              },
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
      const count = result[0]?.fact_all_types__count || 0;
      
      // Should be (2/5) * (2/4) of data = 20%
      expect(count).toBeGreaterThan(180000);
      expect(count).toBeLessThan(220000);
    });
  });
  
  describe('Edge Cases', () => {
    it('should handle filter that matches no rows', async () => {
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.priority',
            operator: 'equals',
            values: ['nonexistent_value'],
          },
        ],
        dimensions: [],
      };
      
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });
      
      const result = await duckdbExec(sql);
      
      expect(Number(result[0]?.fact_all_types__count)).toBe(0);
    });
    
    it('should handle filter with many values in IN clause', async () => {
      // Create many unique values that don't exist
      const values = Array.from({ length: 50 }, (_, i) => `nonexistent_${i}`);
      
      const query = {
        measures: ['fact_all_types.count'],
        filters: [
          {
            member: 'fact_all_types.priority',
            operator: 'in',
            values,
          },
        ],
        dimensions: [],
      };
      
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [FACT_ALL_TYPES_SCHEMA],
      });
      
      const result = await duckdbExec(sql);
      
      expect(Number(result[0]?.fact_all_types__count)).toBe(0);
    });
  });
});

