/**
 * Delimiter Safety Tests
 * 
 * CRITICAL: These tests will FAIL if the __ delimiter is changed to .
 * 
 * Each test generates SQL, executes it, and deep equals the result.
 * If MEERKAT_OUTPUT_DELIMITER is changed from '__' to '.':
 * - The SQL execution will fail with parser errors, OR
 * - The result keys will be different and deep equals will fail
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { cubeQueryToSQL } from '../../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../../duckdb-exec';
import { TableSchema } from '@devrev/meerkat-core';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
} from './synthetic/schema-setup';

describe('Delimiter Safety Tests', () => {
  beforeAll(async () => {
    await dropSyntheticTables();
    await createAllSyntheticTables();
  }, 120000);

  it('table.column dimension with filter', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
      dimensions: [{ name: 'priority', sql: 'fact_all_types.priority', type: 'string' }],
    };

    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        filters: [{ member: 'fact_all_types.priority', operator: 'equals', values: ['high'] }],
      },
      tableSchemas: [schema],
    });

    const result = await duckdbExec(sql);

    expect(result).toEqual([
      { fact_all_types__count: expect.anything(), fact_all_types__priority: 'high' }
    ]);
  });

  it('multiple qualified columns', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
      dimensions: [
        { name: 'priority', sql: 'fact_all_types.priority', type: 'string' },
        { name: 'status', sql: 'fact_all_types.status', type: 'string' },
      ],
    };

    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority', 'fact_all_types.status'],
        filters: [{ member: 'fact_all_types.priority', operator: 'equals', values: ['high'] }],
        limit: 1,
      },
      tableSchemas: [schema],
    });

    const result = await duckdbExec(sql);

    expect(result).toEqual([
      { 
        fact_all_types__count: expect.anything(), 
        fact_all_types__priority: 'high',
        fact_all_types__status: expect.any(String),
      }
    ]);
  });

  it('CASE WHEN expression with filter', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
      dimensions: [{
        name: 'priority_category',
        sql: `CASE WHEN fact_all_types.priority = 'high' THEN 'urgent' ELSE 'normal' END`,
        type: 'string',
      }],
    };

    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority_category'],
        filters: [{ member: 'fact_all_types.priority_category', operator: 'equals', values: ['urgent'] }],
      },
      tableSchemas: [schema],
    });

    const result = await duckdbExec(sql);

    expect(result).toEqual([
      { fact_all_types__count: expect.anything(), fact_all_types__priority_category: 'urgent' }
    ]);
  });

  it('nested CASE WHEN with filter', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
      dimensions: [
        { name: 'priority', sql: 'fact_all_types.priority', type: 'string' },
        {
          name: 'severity',
          sql: `CASE WHEN fact_all_types.priority = 'high' THEN 'critical' ELSE 'normal' END`,
          type: 'string',
        },
      ],
    };

    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority', 'fact_all_types.severity'],
        filters: [{ member: 'fact_all_types.priority', operator: 'equals', values: ['high'] }],
      },
      tableSchemas: [schema],
    });

    const result = await duckdbExec(sql);

    expect(result).toEqual([
      { 
        fact_all_types__count: expect.anything(), 
        fact_all_types__priority: 'high',
        fact_all_types__severity: 'critical',
      }
    ]);
  });

  it('COALESCE with filter', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
      dimensions: [
        { name: 'priority', sql: 'fact_all_types.priority', type: 'string' },
        {
          name: 'safe_priority',
          sql: "COALESCE(fact_all_types.priority, 'unknown')",
          type: 'string',
        },
      ],
    };

    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority', 'fact_all_types.safe_priority'],
        filters: [{ member: 'fact_all_types.priority', operator: 'equals', values: ['high'] }],
      },
      tableSchemas: [schema],
    });

    const result = await duckdbExec(sql);

    expect(result).toEqual([
      { 
        fact_all_types__count: expect.anything(), 
        fact_all_types__priority: 'high',
        fact_all_types__safe_priority: 'high',
      }
    ]);
  });

  it('CONCAT with filter', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
      dimensions: [{
        name: 'combined',
        sql: "CONCAT(fact_all_types.priority, '-', fact_all_types.status)",
        type: 'string',
      }],
    };

    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.combined'],
        filters: [{ member: 'fact_all_types.combined', operator: 'equals', values: ['high-open'] }],
      },
      tableSchemas: [schema],
    });

    const result = await duckdbExec(sql);

    expect(result).toEqual([
      { fact_all_types__count: expect.anything(), fact_all_types__combined: 'high-open' }
    ]);
  });

  it('SUM aggregate with filter', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'total', sql: 'SUM(fact_all_types.metric_bigint)', type: 'number' }],
      dimensions: [{ name: 'priority', sql: 'fact_all_types.priority', type: 'string' }],
    };

    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.total'],
        dimensions: ['fact_all_types.priority'],
        filters: [{ member: 'fact_all_types.priority', operator: 'equals', values: ['high'] }],
      },
      tableSchemas: [schema],
    });

    const result = await duckdbExec(sql);

    expect(result).toEqual([
      { fact_all_types__total: expect.anything(), fact_all_types__priority: 'high' }
    ]);
  });

  it('AVG aggregate with filter', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'avg_metric', sql: 'AVG(fact_all_types.metric_double)', type: 'number' }],
      dimensions: [{ name: 'status', sql: 'fact_all_types.status', type: 'string' }],
    };

    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.avg_metric'],
        dimensions: ['fact_all_types.status'],
        filters: [{ member: 'fact_all_types.status', operator: 'equals', values: ['open'] }],
      },
      tableSchemas: [schema],
    });

    const result = await duckdbExec(sql);

    expect(result).toEqual([
      { fact_all_types__avg_metric: expect.anything(), fact_all_types__status: 'open' }
    ]);
  });

  it('COUNT DISTINCT with filter', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'unique_users', sql: 'COUNT(DISTINCT fact_all_types.user_id)', type: 'number' }],
      dimensions: [{ name: 'priority', sql: 'fact_all_types.priority', type: 'string' }],
    };

    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.unique_users'],
        dimensions: ['fact_all_types.priority'],
        filters: [{ member: 'fact_all_types.priority', operator: 'equals', values: ['high'] }],
      },
      tableSchemas: [schema],
    });

    const result = await duckdbExec(sql);

    expect(result).toEqual([
      { fact_all_types__unique_users: expect.anything(), fact_all_types__priority: 'high' }
    ]);
  });

  it('HAVING filter on measure', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
      dimensions: [{ name: 'priority', sql: 'fact_all_types.priority', type: 'string' }],
    };

    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        filters: [{ member: 'fact_all_types.count', operator: 'gt', values: ['100000'] }],
      },
      tableSchemas: [schema],
    });

    const result = await duckdbExec(sql);

    expect(result.length).toBeGreaterThan(0);
    expect(result[0]).toEqual({
      fact_all_types__count: expect.anything(),
      fact_all_types__priority: expect.any(String),
    });
  });

  it('CAST expression with filter', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
      dimensions: [
        { name: 'priority', sql: 'fact_all_types.priority', type: 'string' },
        {
          name: 'metric_str',
          sql: 'CAST(fact_all_types.metric_bigint AS VARCHAR)',
          type: 'string',
        },
      ],
    };

    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority', 'fact_all_types.metric_str'],
        filters: [{ member: 'fact_all_types.priority', operator: 'equals', values: ['high'] }],
        limit: 1,
      },
      tableSchemas: [schema],
    });

    const result = await duckdbExec(sql);

    expect(result).toEqual([
      { 
        fact_all_types__count: expect.anything(), 
        fact_all_types__priority: 'high',
        fact_all_types__metric_str: expect.any(String),
      }
    ]);
  });

  it('UPPER string function with filter', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
      dimensions: [{
        name: 'priority_upper',
        sql: 'UPPER(fact_all_types.priority)',
        type: 'string',
      }],
    };

    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority_upper'],
        filters: [{ member: 'fact_all_types.priority_upper', operator: 'equals', values: ['HIGH'] }],
      },
      tableSchemas: [schema],
    });

    const result = await duckdbExec(sql);

    expect(result).toEqual([
      { fact_all_types__count: expect.anything(), fact_all_types__priority_upper: 'HIGH' }
    ]);
  });

  it('json_extract_path_text with filter', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
      dimensions: [{
        name: 'json_source',
        sql: "json_extract_path_text(fact_all_types.metadata_json, 'source')",
        type: 'string',
      }],
    };

    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.json_source'],
        filters: [{ member: 'fact_all_types.json_source', operator: 'equals', values: ['web'] }],
      },
      tableSchemas: [schema],
    });

    const result = await duckdbExec(sql);

    expect(result).toEqual([
      { fact_all_types__count: expect.anything(), fact_all_types__json_source: 'web' }
    ]);
  });

  it('DATE_TRUNC with filter', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
      dimensions: [
        { name: 'priority', sql: 'fact_all_types.priority', type: 'string' },
        {
          name: 'created_month',
          sql: "DATE_TRUNC('month', fact_all_types.created_date)",
          type: 'time',
        },
      ],
    };

    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority', 'fact_all_types.created_month'],
        filters: [{ member: 'fact_all_types.priority', operator: 'equals', values: ['high'] }],
        limit: 1,
      },
      tableSchemas: [schema],
    });

    const result = await duckdbExec(sql);

    expect(result).toEqual([
      { 
        fact_all_types__count: expect.anything(), 
        fact_all_types__priority: 'high',
        fact_all_types__created_month: expect.anything(),
      }
    ]);
  });

  it('EXTRACT with filter', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
      dimensions: [
        { name: 'priority', sql: 'fact_all_types.priority', type: 'string' },
        {
          name: 'created_year',
          sql: 'EXTRACT(YEAR FROM fact_all_types.created_date)',
          type: 'number',
        },
      ],
    };

    const sql = await cubeQueryToSQL({
      query: {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority', 'fact_all_types.created_year'],
        filters: [{ member: 'fact_all_types.priority', operator: 'equals', values: ['high'] }],
        limit: 1,
      },
      tableSchemas: [schema],
    });

    const result = await duckdbExec(sql);

    expect(result).toEqual([
      { 
        fact_all_types__count: expect.anything(), 
        fact_all_types__priority: 'high',
        fact_all_types__created_year: expect.anything(),
      }
    ]);
  });
});
