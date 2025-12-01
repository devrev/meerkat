import { Query, TableSchema } from '@devrev/meerkat-core';
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

const SCHEMA: TableSchema = {
  name: 'users',
  sql: `
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  `,
  measures: [
    {
      name: 'count',
      sql: 'COUNT(*)',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'id',
      sql: 'id',
      type: 'number',
    },
    {
      name: 'name',
      sql: 'name',
      type: 'string',
    },
    {
      name: 'age',
      sql: 'age',
      type: 'number',
    },
  ],
  joins: [],
};

describe('SQL Expression Filters - Simple Tests', () => {
  it('should support simple IN with SQL expression', async () => {
    const query: Query = {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'in',
          sqlExpression: '(1, 3)',
        },
      ],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA] });
    console.log('Generated SQL:', sql);

    expect(sql).toContain('users__id IN (1, 3)');

    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(2);
    expect(output.map((r: any) => r.users__name).sort()).toEqual([
      'Alice',
      'Charlie',
    ]);
  });

  it('should support GT with SQL expression', async () => {
    const query: Query = {
      measures: ['users.count'],
      dimensions: ['users.name', 'users.age'],
      filters: [
        {
          member: 'users.age',
          operator: 'gt',
          sqlExpression: '28',
        },
      ],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA] });
    console.log('Generated SQL:', sql);

    expect(sql).toContain('users__age > 28');

    const output: any = await duckdbExec(sql);
    expect(output.length).toBeGreaterThan(0);
    expect(output.every((r: any) => r.users__age > 28)).toBeTruthy();
  });

  it('should work with AND combining values and SQL expressions', async () => {
    const query: Query = {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          and: [
            {
              member: 'users.name',
              operator: 'equals',
              values: ['Alice'],
            },
            {
              member: 'users.age',
              operator: 'gte',
              sqlExpression: '20',
            },
          ],
        },
      ],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA] });
    console.log('Generated SQL:', sql);

    expect(sql).toContain('users__age >= 20');

    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(1);
    expect(output[0].users__name).toBe('Alice');
  });

  it('should support complex subquery with IN', async () => {
    const query: Query = {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'in',
          sqlExpression: '(SELECT id FROM (VALUES (1), (3)) AS t(id))',
        },
      ],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA] });
    console.log('Generated SQL with subquery:', sql);

    expect(sql).toContain('users__id IN (SELECT id FROM');

    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(2);
    expect(output.map((r: any) => r.users__name).sort()).toEqual([
      'Alice',
      'Charlie',
    ]);
  });

  it('should support GT with calculated subquery value', async () => {
    const query: Query = {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.age',
          operator: 'gt',
          sqlExpression:
            '(SELECT AVG(age) FROM (VALUES (25), (30), (35)) AS t(age))',
        },
      ],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA] });
    console.log('Generated SQL with AVG:', sql);

    expect(sql).toContain('users__age > (SELECT AVG(age)');

    const output: any = await duckdbExec(sql);
    // Average is 30, so should return ages > 30 (Charlie: 35)
    expect(output).toHaveLength(1);
    expect(output[0].users__name).toBe('Charlie');
  });

  it('should still work with traditional values array', async () => {
    const query: Query = {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'in',
          values: ['1', '2'],
        },
      ],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA] });
    const output: any = await duckdbExec(sql);

    expect(output).toHaveLength(2);
    expect(output.map((r: any) => r.users__name).sort()).toEqual([
      'Alice',
      'Bob',
    ]);
  });
});
