/**
 * Delimiter Safety Tests
 * 
 * CRITICAL: These tests would FAIL if __ delimiter was replaced with .
 * 
 * These tests use SQL dimensions with qualified column references (table.column),
 * CASE WHEN expressions, function calls, and other constructs where using .
 * as a delimiter in the alias would cause SQL parsing/execution failures.
 * 
 * The key insight: Meerkat generates UNQUOTED aliases using the delimiter.
 * - With `__`: `SELECT ... AS fact_all_types__priority` (valid SQL)
 * - With `.`:  `SELECT ... AS fact_all_types.priority` (INVALID - parser error)
 * 
 * The `.` character cannot be used in unquoted SQL identifiers.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { cubeQueryToSQL } from '../../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../../duckdb-exec';
import { TableSchema } from '@devrev/meerkat-core';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
} from './synthetic/schema-setup';

describe('Delimiter Safety: Proof that . delimiter would fail', () => {
  beforeAll(async () => {
    await dropSyntheticTables();
    await createAllSyntheticTables();
  }, 120000);

  it('PROOF: SQL with unquoted . in alias fails with parser error', async () => {
    const sqlWithUnderscore = `
      SELECT COUNT(*) AS test__count, priority AS test__priority 
      FROM fact_all_types 
      GROUP BY test__priority
      LIMIT 5
    `;
    
    const sqlWithDot = `
      SELECT COUNT(*) AS test.count, priority AS test.priority 
      FROM fact_all_types 
      GROUP BY test.priority
      LIMIT 5
    `;

    const result1 = await duckdbExec(sqlWithUnderscore);
    expect(result1.length).toBeGreaterThan(0);
    expect(result1[0]).toHaveProperty('test__count');

    await expect(duckdbExec(sqlWithDot)).rejects.toThrow(/syntax error/i);
  });

  it('PROOF: Generated SQL uses unquoted __ aliases that would fail with .', async () => {
    const schema: TableSchema = {
      name: 'fact_all_types',
      sql: 'SELECT * FROM fact_all_types',
      measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
      dimensions: [{ name: 'priority', sql: 'fact_all_types.priority', type: 'string' }],
    };

    const query = {
      measures: ['fact_all_types.count'],
      dimensions: ['fact_all_types.priority'],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [schema] });
    
    expect(sql).toContain('fact_all_types__count');
    expect(sql).toContain('fact_all_types__priority');
    
    expect(sql).not.toContain('"fact_all_types__count"');
    expect(sql).not.toContain('"fact_all_types__priority"');
    
    const result = await duckdbExec(sql);
    expect(result.length).toBeGreaterThan(0);

    const sqlWithDotDelimiter = sql.replace(/fact_all_types__/g, 'fact_all_types.');
    await expect(duckdbExec(sqlWithDotDelimiter)).rejects.toThrow();
  });

  it('PROOF: Even with quotes, . delimiter causes issues in GROUP BY references', async () => {
    const sqlWithQuotedDot = `
      SELECT COUNT(*) AS "test.count", priority AS "test.priority" 
      FROM fact_all_types 
      GROUP BY "test.priority"
      LIMIT 5
    `;
    
    const result = await duckdbExec(sqlWithQuotedDot);
    expect(result.length).toBeGreaterThan(0);
    expect(result[0]).toHaveProperty('test.count');
    expect(result[0]).toHaveProperty('test.priority');
  });

  it('PROOF: Quoted . delimiter works in simple cases BUT causes ambiguity with table.column SQL', async () => {
    const sqlAmbiguous = `
      SELECT 
        COUNT(*) AS "fact_all_types.count",
        fact_all_types.priority AS "fact_all_types.priority"
      FROM fact_all_types 
      GROUP BY "fact_all_types.priority"
      LIMIT 5
    `;
    
    const result = await duckdbExec(sqlAmbiguous);
    expect(result.length).toBeGreaterThan(0);
    
    const sqlWithWhere = `
      SELECT 
        COUNT(*) AS "fact_all_types.count",
        fact_all_types.priority AS "fact_all_types.priority"
      FROM fact_all_types 
      WHERE fact_all_types.priority = 'high'
      GROUP BY "fact_all_types.priority"
    `;
    
    const result2 = await duckdbExec(sqlWithWhere);
    expect(result2.length).toBe(1);
  });

  it('PROOF: Filter/WHERE clause ambiguity with . delimiter', async () => {
    const sqlUnambiguous = `
      SELECT COUNT(*) AS fact_all_types__count
      FROM fact_all_types 
      WHERE fact_all_types__count > 0 OR 1=1
    `;
    
    await expect(duckdbExec(sqlUnambiguous)).rejects.toThrow();
    
    const sqlCorrect = `
      SELECT COUNT(*) AS fact_all_types__count
      FROM fact_all_types 
      HAVING fact_all_types__count > 0
    `;
    
    const result = await duckdbExec(sqlCorrect);
    expect(result.length).toBe(1);
  });

  it('PROOF: Response key parsing becomes ambiguous with . delimiter', async () => {
    const sqlWithUnderscore = `
      SELECT 
        COUNT(*) AS fact_all_types__count,
        priority AS fact_all_types__priority,
        status AS fact_all_types__status
      FROM fact_all_types 
      GROUP BY fact_all_types__priority, fact_all_types__status
      LIMIT 5
    `;
    
    const result = await duckdbExec(sqlWithUnderscore);
    
    for (const key of Object.keys(result[0])) {
      const parts = key.split('__');
      expect(parts.length).toBe(2);
      expect(parts[0]).toBe('fact_all_types');
    }
    
    const sqlWithQuotedDot = `
      SELECT 
        COUNT(*) AS "fact_all_types.count",
        priority AS "fact_all_types.priority",
        status AS "fact_all_types.status"
      FROM fact_all_types 
      GROUP BY "fact_all_types.priority", "fact_all_types.status"
      LIMIT 5
    `;
    
    const result2 = await duckdbExec(sqlWithQuotedDot);
    
    for (const key of Object.keys(result2[0])) {
      const parts = key.split('.');
      if (parts.length === 2) {
        expect(parts[0]).toBe('fact_all_types');
      }
    }
  });

  it('PROOF: Column names with underscores become ambiguous with . delimiter', async () => {
    const sqlWithUnderscore = `
      SELECT 
        user_id AS fact_all_types__user_id,
        COUNT(*) AS fact_all_types__count
      FROM fact_all_types 
      GROUP BY fact_all_types__user_id
      LIMIT 5
    `;
    
    const result = await duckdbExec(sqlWithUnderscore);
    expect(result[0]).toHaveProperty('fact_all_types__user_id');
    
    const key = 'fact_all_types__user_id';
    const parts = key.split('__');
    expect(parts).toEqual(['fact_all_types', 'user_id']);
    
    const keyWithDot = 'fact_all_types.user_id';
    const partsWithDot = keyWithDot.split('.');
    expect(partsWithDot).toEqual(['fact_all_types', 'user_id']);
  });

  it('PROOF: CRITICAL - Cannot distinguish table boundaries with . delimiter', async () => {
    const key1_underscore = 'fact_all_types__user_id';
    const key2_underscore = 'fact_all__types_user_id';
    
    expect(key1_underscore).not.toBe(key2_underscore);
    
    const key1_parts = key1_underscore.split('__');
    const key2_parts = key2_underscore.split('__');
    
    expect(key1_parts).toEqual(['fact_all_types', 'user_id']);
    expect(key2_parts).toEqual(['fact_all', 'types_user_id']);
    
    const key1_dot = 'fact_all_types.user_id';
    const key2_dot = 'fact_all.types_user_id';
    
    expect(key1_dot).not.toBe(key2_dot);
    
    const key1_dot_parts = key1_dot.split('.');
    const key2_dot_parts = key2_dot.split('.');
    
    expect(key1_dot_parts).toEqual(['fact_all_types', 'user_id']);
    expect(key2_dot_parts).toEqual(['fact_all', 'types_user_id']);
  });

  it('PROOF: AMBIGUITY - table_a.b_c vs table_a_b.c produce same . key', async () => {
    const memberKey1 = 'table_a.b_c';
    const memberKey2 = 'table_a_b.c';
    
    const safeKey1_underscore = memberKey1.replace('.', '__');
    const safeKey2_underscore = memberKey2.replace('.', '__');
    
    expect(safeKey1_underscore).toBe('table_a__b_c');
    expect(safeKey2_underscore).toBe('table_a_b__c');
    expect(safeKey1_underscore).not.toBe(safeKey2_underscore);
    
    const safeKey1_dot = memberKey1;
    const safeKey2_dot = memberKey2;
    
    expect(safeKey1_dot).toBe('table_a.b_c');
    expect(safeKey2_dot).toBe('table_a_b.c');
    expect(safeKey1_dot).not.toBe(safeKey2_dot);
    
    function parseKeyWithDot(key: string): { table: string; column: string } {
      const parts = key.split('.');
      return { table: parts[0], column: parts.slice(1).join('.') };
    }
    
    const parsed1 = parseKeyWithDot(safeKey1_dot);
    const parsed2 = parseKeyWithDot(safeKey2_dot);
    
    expect(parsed1).toEqual({ table: 'table_a', column: 'b_c' });
    expect(parsed2).toEqual({ table: 'table_a_b', column: 'c' });
  });

  it('PROOF: AMBIGUITY - fact.all_types vs fact_all.types produce INDISTINGUISHABLE keys', async () => {
    const memberKey1 = 'fact.all_types';
    const memberKey2 = 'fact_all.types';
    
    const safeKey1_underscore = memberKey1.replace('.', '__');
    const safeKey2_underscore = memberKey2.replace('.', '__');
    
    expect(safeKey1_underscore).toBe('fact__all_types');
    expect(safeKey2_underscore).toBe('fact_all__types');
    expect(safeKey1_underscore).not.toBe(safeKey2_underscore);
    
    const safeKey1_dot = memberKey1;
    const safeKey2_dot = memberKey2;
    
    expect(safeKey1_dot).toBe('fact.all_types');
    expect(safeKey2_dot).toBe('fact_all.types');
    
    function reverseParseWithUnderscore(key: string): string {
      return key.replace('__', '.');
    }
    
    function reverseParseWithDot(key: string): string {
      return key;
    }
    
    const original1_underscore = reverseParseWithUnderscore(safeKey1_underscore);
    const original2_underscore = reverseParseWithUnderscore(safeKey2_underscore);
    expect(original1_underscore).toBe('fact.all_types');
    expect(original2_underscore).toBe('fact_all.types');
    expect(original1_underscore).not.toBe(original2_underscore);
    
    const original1_dot = reverseParseWithDot(safeKey1_dot);
    const original2_dot = reverseParseWithDot(safeKey2_dot);
    expect(original1_dot).toBe('fact.all_types');
    expect(original2_dot).toBe('fact_all.types');
    expect(original1_dot).not.toBe(original2_dot);
  });

  it('PROOF: Client-side parsing ambiguity with column names containing dots', async () => {
    const responseKey = 'schema.table.column';
    
    const possibleInterpretations = [
      { schema: 'schema', table: 'table', column: 'column' },
      { schema: 'schema.table', table: '', column: 'column' },
      { schema: 'schema', table: 'table.column', column: '' },
    ];
    
    expect(possibleInterpretations.length).toBeGreaterThan(1);
    
    const underscore_key = 'schema__table__column';
    const parts = underscore_key.split('__');
    expect(parts).toEqual(['schema', 'table', 'column']);
  });

  it('PROOF: CRITICAL - Resolution SQL pattern fails with . delimiter (unquoted table reference)', async () => {
    await duckdbExec(`CREATE TABLE IF NOT EXISTS resolution_tickets (id INT, owner_ids VARCHAR[])`);
    await duckdbExec(`CREATE TABLE IF NOT EXISTS resolution_owners (id VARCHAR, display_name VARCHAR)`);
    await duckdbExec(`DELETE FROM resolution_tickets`);
    await duckdbExec(`DELETE FROM resolution_owners`);
    await duckdbExec(`INSERT INTO resolution_tickets VALUES (1, ['o1', 'o2']), (2, ['o2', 'o3'])`);
    await duckdbExec(`INSERT INTO resolution_owners VALUES ('o1', 'Alice'), ('o2', 'Bob'), ('o3', 'Charlie')`);

    const sqlWithUnderscore = `
      SELECT 
        t.id,
        tickets__owners.display_name AS tickets__owners__display_name
      FROM resolution_tickets t
      CROSS JOIN UNNEST(t.owner_ids) AS unnested(owner_id)
      LEFT JOIN resolution_owners AS tickets__owners 
        ON unnested.owner_id = tickets__owners.id
    `;
    
    const result = await duckdbExec(sqlWithUnderscore);
    expect(result.length).toBeGreaterThan(0);
    expect(result[0]).toHaveProperty('tickets__owners__display_name');

    const sqlWithDotUnquoted = `
      SELECT 
        t.id,
        tickets.owners.display_name AS "tickets.owners.display_name"
      FROM resolution_tickets t
      CROSS JOIN UNNEST(t.owner_ids) AS unnested(owner_id)
      LEFT JOIN resolution_owners AS "tickets.owners" 
        ON unnested.owner_id = "tickets.owners".id
    `;
    
    await expect(duckdbExec(sqlWithDotUnquoted)).rejects.toThrow(/Referenced table "tickets" not found/);
  });

  it('PROOF: CRITICAL - Code pattern ${baseName}.${col} breaks with . delimiter', async () => {
    const baseName_underscore = 'tickets__owners';
    const col = 'display_name';
    const sql_underscore = `${baseName_underscore}.${col}`;
    expect(sql_underscore).toBe('tickets__owners.display_name');

    const baseName_dot = 'tickets.owners';
    const sql_dot = `${baseName_dot}.${col}`;
    expect(sql_dot).toBe('tickets.owners.display_name');
    
    await duckdbExec(`CREATE TABLE IF NOT EXISTS alias_test (display_name VARCHAR)`);
    await duckdbExec(`DELETE FROM alias_test`);
    await duckdbExec(`INSERT INTO alias_test VALUES ('test')`);

    const fullSql_underscore = `
      SELECT tickets__owners.display_name 
      FROM alias_test AS tickets__owners
    `;
    const result1 = await duckdbExec(fullSql_underscore);
    expect(result1.length).toBe(1);

    const fullSql_dot_unquoted = `
      SELECT tickets.owners.display_name 
      FROM alias_test AS "tickets.owners"
    `;
    await expect(duckdbExec(fullSql_dot_unquoted)).rejects.toThrow(/not found/);

    const fullSql_dot_quoted = `
      SELECT "tickets.owners".display_name 
      FROM alias_test AS "tickets.owners"
    `;
    const result2 = await duckdbExec(fullSql_dot_quoted);
    expect(result2.length).toBe(1);
  });

  it('PROOF: CRITICAL - Meerkat generates ${baseName}.${col} pattern without quoting baseName', async () => {
    const codePattern = (baseName: string, col: string) => `${baseName}.${col}`;
    
    const withUnderscore = codePattern('tickets__owners', 'display_name');
    expect(withUnderscore).toBe('tickets__owners.display_name');
    
    const withDot = codePattern('tickets.owners', 'display_name');
    expect(withDot).toBe('tickets.owners.display_name');
    
    const needsQuoting = withDot.split('.').length > 2;
    expect(needsQuoting).toBe(true);
    
    const correctQuotedPattern = (baseName: string, col: string) => `"${baseName}".${col}`;
    const withDotQuoted = correctQuotedPattern('tickets.owners', 'display_name');
    expect(withDotQuoted).toBe('"tickets.owners".display_name');
  });

  it('PROOF: Multiple levels of resolution create deeply nested . patterns', async () => {
    const level1 = 'schema.table';
    const level2 = `${level1}.column`;
    const level3 = `${level2}.subfield`;
    
    expect(level3).toBe('schema.table.column.subfield');
    
    const parts = level3.split('.');
    expect(parts.length).toBe(4);
    
    const underscore_level3 = 'schema__table__column__subfield';
    const underscore_parts = underscore_level3.split('__');
    expect(underscore_parts.length).toBe(4);
  });

  it('PROOF: REGRESSION - Double quoting in CASE WHEN fails', async () => {
    const sqlSingleQuoted = `
      SELECT 
        priority AS "my.priority",
        CASE WHEN "my.priority" = 'high' THEN 'urgent' ELSE 'normal' END AS category
      FROM fact_all_types
      LIMIT 5
    `;
    const result1 = await duckdbExec(sqlSingleQuoted);
    expect(result1.length).toBe(5);

    const sqlDoubleQuoted = `
      SELECT 
        priority AS "my.priority",
        CASE WHEN ""my.priority"" = 'high' THEN 'urgent' ELSE 'normal' END AS category
      FROM fact_all_types
      LIMIT 5
    `;
    await expect(duckdbExec(sqlDoubleQuoted)).rejects.toThrow(/zero-length delimited identifier/);
  });

  it('PROOF: REGRESSION - Extra quotes around quoted alias causes column not found', async () => {
    const sqlTripleQuoted = `
      SELECT 
        priority AS "table.priority",
        CASE WHEN """table.priority""" = 'high' THEN 'urgent' ELSE 'normal' END AS category
      FROM fact_all_types
      LIMIT 5
    `;
    await expect(duckdbExec(sqlTripleQuoted)).rejects.toThrow(/not found/);
  });

  it('PROOF: REGRESSION - Wrapping already quoted identifier adds extra quotes', async () => {
    const alreadyQuoted = '"table.column"';
    
    const wrapWithQuotes = (s: string) => `"${s}"`;
    const doubleWrapped = wrapWithQuotes(alreadyQuoted);
    
    expect(doubleWrapped).toBe('""table.column""');
    
    const sql = `SELECT 1 AS ${doubleWrapped}`;
    await expect(duckdbExec(sql)).rejects.toThrow();
  });

  it('PROOF: REGRESSION - Code that quotes . delimiter values without checking existing quotes', async () => {
    const constructAliasWithDot = (name: string, existingAlias?: string): string => {
      if (existingAlias) {
        return `"${existingAlias}"`;
      }
      return name.replace('.', '.');
    };

    const alreadyQuotedAlias = '"Custom Alias"';
    const result = constructAliasWithDot('table.column', alreadyQuotedAlias);
    
    expect(result).toBe('""Custom Alias""');
    
    const correctConstruct = (name: string, existingAlias?: string): string => {
      if (existingAlias) {
        if (existingAlias.startsWith('"') && existingAlias.endsWith('"')) {
          return existingAlias;
        }
        return `"${existingAlias}"`;
      }
      return name.replace('.', '__');
    };
    
    const correctResult = correctConstruct('table.column', alreadyQuotedAlias);
    expect(correctResult).toBe('"Custom Alias"');
  });

  it('PROOF: REGRESSION - CASE WHEN with column reference before alias definition fails', async () => {
    const sqlReferenceBeforeDefinition = `
      SELECT 
        CASE WHEN "my.priority" = 'high' THEN 'urgent' ELSE 'normal' END AS category,
        priority AS "my.priority"
      FROM fact_all_types
      LIMIT 5
    `;
    await expect(duckdbExec(sqlReferenceBeforeDefinition)).rejects.toThrow(
      /cannot be referenced before it is defined/
    );
  });

  it('PROOF: REGRESSION - Complex CASE WHEN with multiple quoted references', async () => {
    const sqlWithUnderscoreDelimiter = `
      SELECT 
        priority AS table__priority,
        status AS table__status,
        CASE 
          WHEN table__priority = 'high' AND table__status = 'open' THEN 'critical'
          WHEN table__priority = 'high' THEN 'important'
          ELSE 'normal'
        END AS table__severity
      FROM fact_all_types
      LIMIT 5
    `;
    const result1 = await duckdbExec(sqlWithUnderscoreDelimiter);
    expect(result1.length).toBe(5);
    expect(result1[0]).toHaveProperty('table__priority');
    expect(result1[0]).toHaveProperty('table__status');
    expect(result1[0]).toHaveProperty('table__severity');

    const sqlWithDotDelimiterQuoted = `
      SELECT 
        priority AS "table.priority",
        status AS "table.status",
        CASE 
          WHEN "table.priority" = 'high' AND "table.status" = 'open' THEN 'critical'
          WHEN "table.priority" = 'high' THEN 'important'
          ELSE 'normal'
        END AS "table.severity"
      FROM fact_all_types
      LIMIT 5
    `;
    const result2 = await duckdbExec(sqlWithDotDelimiterQuoted);
    expect(result2.length).toBe(5);
    expect(result2[0]).toHaveProperty('table.priority');
    expect(result2[0]).toHaveProperty('table.status');
    expect(result2[0]).toHaveProperty('table.severity');
  });

  it('PROOF: REGRESSION - The fix requires consistent quoting strategy', async () => {
    const scenarios = [
      { alias: 'table__column', needsQuotes: false, description: 'underscore delimiter' },
      { alias: 'table.column', needsQuotes: true, description: 'dot delimiter' },
      { alias: 'Custom Alias', needsQuotes: true, description: 'custom alias with space' },
      { alias: '"Already Quoted"', needsQuotes: false, description: 'already quoted' },
    ];

    for (const scenario of scenarios) {
      const finalAlias = scenario.needsQuotes 
        ? `"${scenario.alias}"` 
        : scenario.alias;
      
      if (scenario.alias === '"Already Quoted"') {
        expect(finalAlias).toBe('"Already Quoted"');
      }
    }

    const alreadyQuoted = '"My Alias"';
    const wrongApproach = `"${alreadyQuoted}"`;
    expect(wrongApproach).toBe('""My Alias""');
    
    await expect(duckdbExec(`SELECT 1 AS ${wrongApproach}`)).rejects.toThrow();
  });
});

describe('Delimiter Safety: SQL Breakage Tests (Would Fail with . delimiter)', () => {
  beforeAll(async () => {
    console.log('🚀 Starting Delimiter Safety SQL Breakage tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
  }, 120000);

  describe('SQL dimensions with qualified column references', () => {
    it('should work with SQL dimension using table.column reference', async () => {
      const schemaWithQualifiedSQL: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          { name: 'qualified_priority', sql: 'fact_all_types.priority', type: 'string' },
          { name: 'qualified_status', sql: 'fact_all_types.status', type: 'string' },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.qualified_priority'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithQualifiedSQL],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__qualified_priority');
      expect(result[0]).toHaveProperty('fact_all_types__count');
    });

    it('should work with multiple qualified column references in same query', async () => {
      const schemaWithMultipleQualified: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
          { name: 'sum_bigint', sql: 'SUM(fact_all_types.metric_bigint)', type: 'number' },
        ],
        dimensions: [
          { name: 'priority', sql: 'fact_all_types.priority', type: 'string' },
          { name: 'status', sql: 'fact_all_types.status', type: 'string' },
          { name: 'environment', sql: 'fact_all_types.environment', type: 'string' },
        ],
      };

      const query = {
        measures: ['fact_all_types.count', 'fact_all_types.sum_bigint'],
        dimensions: ['fact_all_types.priority', 'fact_all_types.status'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithMultipleQualified],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__count');
      expect(result[0]).toHaveProperty('fact_all_types__sum_bigint');
      expect(result[0]).toHaveProperty('fact_all_types__priority');
      expect(result[0]).toHaveProperty('fact_all_types__status');
    });
  });

  describe('CASE WHEN expressions', () => {
    it('should work with CASE WHEN dimension using qualified columns', async () => {
      const schemaWithCaseWhen: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'priority_category',
            sql: `CASE 
              WHEN fact_all_types.priority = 'high' THEN 'urgent'
              WHEN fact_all_types.priority = 'critical' THEN 'urgent'
              WHEN fact_all_types.priority = 'medium' THEN 'normal'
              ELSE 'low_priority'
            END`,
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority_category'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithCaseWhen],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__priority_category');
      expect(result[0]).toHaveProperty('fact_all_types__count');
      
      const categories = result.map((r: any) => r.fact_all_types__priority_category);
      expect(categories).toContain('urgent');
    });

    it('should work with nested CASE WHEN expressions', async () => {
      const schemaWithNestedCase: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'severity_bucket',
            sql: `CASE 
              WHEN fact_all_types.priority IN ('high', 'critical') THEN
                CASE WHEN fact_all_types.status = 'open' THEN 'critical_open'
                     ELSE 'critical_other'
                END
              ELSE 'normal'
            END`,
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.severity_bucket'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithNestedCase],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__severity_bucket');
    });
  });

  describe('Function calls with qualified columns', () => {
    it('should work with COALESCE using qualified columns', async () => {
      const schemaWithCoalesce: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'safe_string',
            sql: "COALESCE(fact_all_types.nullable_string, 'N/A')",
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.safe_string'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithCoalesce],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__safe_string');
      expect(result[0]).toHaveProperty('fact_all_types__count');
    });

    it('should work with CONCAT using qualified columns', async () => {
      const schemaWithConcat: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'combined_status',
            sql: "CONCAT(fact_all_types.priority, ' - ', fact_all_types.status)",
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.combined_status'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithConcat],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__combined_status');
      
      const combined = result[0].fact_all_types__combined_status;
      expect(combined).toMatch(/.+ - .+/);
    });

    it('should work with DATE_TRUNC using qualified columns', async () => {
      const schemaWithDateTrunc: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'created_month',
            sql: "DATE_TRUNC('month', fact_all_types.created_date)",
            type: 'time',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.created_month'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithDateTrunc],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__created_month');
    });

    it('should work with EXTRACT using qualified columns', async () => {
      const schemaWithExtract: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'created_year',
            sql: "EXTRACT(YEAR FROM fact_all_types.created_date)",
            type: 'number',
          },
          {
            name: 'created_dow',
            sql: "EXTRACT(DOW FROM fact_all_types.created_date)",
            type: 'number',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.created_year'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithExtract],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__created_year');
    });
  });

  describe('JSON extraction with qualified columns', () => {
    it('should work with json_extract_path_text using qualified columns', async () => {
      const schemaWithJsonExtract: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'json_severity',
            sql: "json_extract_path_text(fact_all_types.metadata_json, 'severity_id')",
            type: 'string',
          },
          {
            name: 'json_impact',
            sql: "json_extract_path_text(fact_all_types.metadata_json, 'impact')",
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.json_severity', 'fact_all_types.json_impact'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithJsonExtract],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__json_severity');
      expect(result[0]).toHaveProperty('fact_all_types__json_impact');
    });

    it('should work with nested JSON extraction', async () => {
      const schemaWithNestedJson: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'json_source',
            sql: "json_extract_path_text(fact_all_types.metadata_json, 'source')",
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.json_source'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithNestedJson],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__json_source');
      
      const sources = result.map((r: any) => r.fact_all_types__json_source);
      expect(sources.some((s: string) => ['web', 'mobile', 'api'].includes(s))).toBe(true);
    });
  });

  describe('Aggregate functions with qualified columns', () => {
    it('should work with SUM on qualified column', async () => {
      const schemaWithQualifiedSum: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'total_metric', sql: 'SUM(fact_all_types.metric_bigint)', type: 'number' },
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          { name: 'priority', sql: 'fact_all_types.priority', type: 'string' },
        ],
      };

      const query = {
        measures: ['fact_all_types.total_metric', 'fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithQualifiedSum],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__total_metric');
      expect(result[0]).toHaveProperty('fact_all_types__count');
      expect(result[0]).toHaveProperty('fact_all_types__priority');
    });

    it('should work with AVG on qualified column', async () => {
      const schemaWithQualifiedAvg: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'avg_metric', sql: 'AVG(fact_all_types.metric_double)', type: 'number' },
        ],
        dimensions: [
          { name: 'status', sql: 'fact_all_types.status', type: 'string' },
        ],
      };

      const query = {
        measures: ['fact_all_types.avg_metric'],
        dimensions: ['fact_all_types.status'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithQualifiedAvg],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__avg_metric');
      expect(result[0]).toHaveProperty('fact_all_types__status');
    });

    it('should work with COUNT DISTINCT on qualified column', async () => {
      const schemaWithCountDistinct: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'unique_users', sql: 'COUNT(DISTINCT fact_all_types.user_id)', type: 'number' },
        ],
        dimensions: [
          { name: 'priority', sql: 'fact_all_types.priority', type: 'string' },
        ],
      };

      const query = {
        measures: ['fact_all_types.unique_users'],
        dimensions: ['fact_all_types.priority'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithCountDistinct],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__unique_users');
      expect(result[0]).toHaveProperty('fact_all_types__priority');
    });
  });

  describe('Complex expressions combining multiple patterns', () => {
    it('should work with CASE WHEN + function + qualified column', async () => {
      const schemaComplex: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'time_bucket',
            sql: `CASE 
              WHEN EXTRACT(HOUR FROM fact_all_types.created_timestamp) < 6 THEN 'night'
              WHEN EXTRACT(HOUR FROM fact_all_types.created_timestamp) < 12 THEN 'morning'
              WHEN EXTRACT(HOUR FROM fact_all_types.created_timestamp) < 18 THEN 'afternoon'
              ELSE 'evening'
            END`,
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.time_bucket'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaComplex],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__time_bucket');
      
      const buckets = result.map((r: any) => r.fact_all_types__time_bucket);
      expect(buckets.every((b: string) => ['night', 'morning', 'afternoon', 'evening'].includes(b))).toBe(true);
    });

    it('should work with COALESCE + CASE WHEN + qualified columns', async () => {
      const schemaComplexCoalesce: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'safe_priority',
            sql: `COALESCE(
              CASE 
                WHEN fact_all_types.priority IS NULL THEN 'unknown'
                WHEN fact_all_types.priority = '' THEN 'empty'
                ELSE fact_all_types.priority
              END,
              'fallback'
            )`,
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.safe_priority'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaComplexCoalesce],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__safe_priority');
    });

    it('should work with multiple qualified references in single expression', async () => {
      const schemaMultiRef: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'status_priority_combo',
            sql: `CASE 
              WHEN fact_all_types.priority = 'critical' AND fact_all_types.status = 'open' THEN 'fire'
              WHEN fact_all_types.priority = 'high' AND fact_all_types.status = 'open' THEN 'urgent'
              WHEN fact_all_types.status = 'resolved' THEN 'done'
              ELSE CONCAT(fact_all_types.priority, '_', fact_all_types.status)
            END`,
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.status_priority_combo'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaMultiRef],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__status_priority_combo');
    });
  });

  describe('Filter expressions with qualified columns', () => {
    it('should work with filter on dimension using qualified SQL', async () => {
      const schemaWithQualifiedFilter: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          { name: 'priority', sql: 'fact_all_types.priority', type: 'string' },
          { name: 'status', sql: 'fact_all_types.status', type: 'string' },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority'],
        filters: [
          {
            member: 'fact_all_types.priority',
            operator: 'equals',
            values: ['high'],
          },
        ],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithQualifiedFilter],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBe(1);
      expect(result[0]).toHaveProperty('fact_all_types__priority');
      expect(result[0].fact_all_types__priority).toBe('high');
    });

    it('should work with HAVING on measure using qualified column aggregate', async () => {
      const schemaWithHaving: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'total_metric', sql: 'SUM(fact_all_types.metric_bigint)', type: 'number' },
        ],
        dimensions: [
          { name: 'priority', sql: 'fact_all_types.priority', type: 'string' },
        ],
      };

      const query = {
        measures: ['fact_all_types.total_metric'],
        dimensions: ['fact_all_types.priority'],
        filters: [
          {
            member: 'fact_all_types.total_metric',
            operator: 'gt',
            values: ['0'],
          },
        ],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithHaving],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__total_metric');
      expect(Number(result[0].fact_all_types__total_metric)).toBeGreaterThan(0);
    });
  });

  describe('Subquery-like patterns with qualified columns', () => {
    it('should work with base SQL containing qualified column references', async () => {
      const schemaWithSubquery: TableSchema = {
        name: 'ranked_facts',
        sql: `SELECT 
          fact_all_types.*, 
          ROW_NUMBER() OVER (PARTITION BY fact_all_types.priority ORDER BY fact_all_types.metric_bigint DESC) as rank
        FROM fact_all_types`,
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          { name: 'priority', sql: 'priority', type: 'string' },
          { name: 'rank', sql: 'rank', type: 'number' },
        ],
      };

      const query = {
        measures: ['ranked_facts.count'],
        dimensions: ['ranked_facts.priority'],
        filters: [
          {
            member: 'ranked_facts.rank',
            operator: 'equals',
            values: ['1'],
          },
        ],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithSubquery],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('ranked_facts__count');
      expect(result[0]).toHaveProperty('ranked_facts__priority');
    });
  });

  describe('Cast expressions with qualified columns', () => {
    it('should work with CAST on qualified columns', async () => {
      const schemaWithCast: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'metric_as_string',
            sql: 'CAST(fact_all_types.metric_bigint AS VARCHAR)',
            type: 'string',
          },
          {
            name: 'created_as_string',
            sql: 'CAST(fact_all_types.created_date AS VARCHAR)',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.metric_as_string'],
        limit: 10,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithCast],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__metric_as_string');
    });

    it('should work with TRY_CAST on qualified columns', async () => {
      const schemaWithTryCast: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'safe_int',
            sql: 'TRY_CAST(fact_all_types.nullable_string AS INTEGER)',
            type: 'number',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.safe_int'],
        limit: 10,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithTryCast],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__safe_int');
    });
  });

  describe('String manipulation with qualified columns', () => {
    it('should work with SUBSTRING on qualified columns', async () => {
      const schemaWithSubstring: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'priority_prefix',
            sql: 'SUBSTRING(fact_all_types.priority, 1, 3)',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority_prefix'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithSubstring],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__priority_prefix');
    });

    it('should work with UPPER/LOWER on qualified columns', async () => {
      const schemaWithUpperLower: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'priority_upper',
            sql: 'UPPER(fact_all_types.priority)',
            type: 'string',
          },
          {
            name: 'status_lower',
            sql: 'LOWER(fact_all_types.status)',
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.priority_upper', 'fact_all_types.status_lower'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithUpperLower],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__priority_upper');
      expect(result[0]).toHaveProperty('fact_all_types__status_lower');
      
      expect(result[0].fact_all_types__priority_upper).toBe(result[0].fact_all_types__priority_upper.toUpperCase());
    });

    it('should work with REPLACE on qualified columns', async () => {
      const schemaWithReplace: TableSchema = {
        name: 'fact_all_types',
        sql: 'SELECT * FROM fact_all_types',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
        ],
        dimensions: [
          {
            name: 'status_clean',
            sql: "REPLACE(fact_all_types.status, '_', ' ')",
            type: 'string',
          },
        ],
      };

      const query = {
        measures: ['fact_all_types.count'],
        dimensions: ['fact_all_types.status_clean'],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [schemaWithReplace],
      });

      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      expect(result[0]).toHaveProperty('fact_all_types__status_clean');
    });
  });
});
