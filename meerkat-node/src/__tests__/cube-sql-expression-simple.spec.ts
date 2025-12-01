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

describe('SQL Expression Filters', () => {
  it('should support simple IN with SQL expression', async () => {
    const query: Query = {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'in',
          sqlExpression: '1, 3',
        },
      ],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA] });

    const expectedSQL = `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE (users__id IN (1, 3)) GROUP BY users__name`;

    expect(sql).toBe(expectedSQL);

    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(2);
    expect(output.map((r: any) => r.users__name).sort()).toEqual([
      'Alice',
      'Charlie',
    ]);
  });

  it('should support NOT IN with SQL expression', async () => {
    const query: Query = {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'notIn',
          sqlExpression: '2',
        },
      ],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA] });

    const expectedSQL = `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE (users__id NOT IN (2)) GROUP BY users__name`;

    expect(sql).toBe(expectedSQL);

    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(2);
    expect(output.map((r: any) => r.users__name).sort()).toEqual([
      'Alice',
      'Charlie',
    ]);
  });

  it('should support complex subquery with IN', async () => {
    const query: Query = {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'in',
          sqlExpression: 'SELECT id FROM (VALUES (1), (3)) AS t(id)',
        },
      ],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA] });

    const expectedSQL = `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE (users__id IN (SELECT id FROM (VALUES (1), (3)) AS t(id))) GROUP BY users__name`;

    expect(sql).toBe(expectedSQL);

    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(2);
    expect(output.map((r: any) => r.users__name).sort()).toEqual([
      'Alice',
      'Charlie',
    ]);
  });

  it('should work with AND combining equals and IN with SQL expression', async () => {
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
              member: 'users.id',
              operator: 'in',
              sqlExpression: '1, 2, 3',
            },
          ],
        },
      ],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA] });

    const expectedSQL = `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE ((users__name = 'Alice') AND (users__id IN (1, 2, 3))) GROUP BY users__name`;

    expect(sql).toBe(expectedSQL);

    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(1);
    expect(output[0].users__name).toBe('Alice');
  });

  it('should work with OR combining IN with SQL expressions', async () => {
    const query: Query = {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          or: [
            {
              member: 'users.id',
              operator: 'in',
              sqlExpression: '1',
            },
            {
              member: 'users.id',
              operator: 'in',
              sqlExpression: '3',
            },
          ],
        },
      ],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA] });

    const expectedSQL = `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE ((users__id IN (1)) OR (users__id IN (3))) GROUP BY users__name`;

    expect(sql).toBe(expectedSQL);

    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(2);
    expect(output.map((r: any) => r.users__name).sort()).toEqual([
      'Alice',
      'Charlie',
    ]);
  });

  it('should support NOT IN with subquery', async () => {
    const query: Query = {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'notIn',
          sqlExpression: 'SELECT 2',
        },
      ],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA] });

    const expectedSQL = `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE (users__id NOT IN (SELECT 2)) GROUP BY users__name`;

    expect(sql).toBe(expectedSQL);

    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(2);
    expect(output.map((r: any) => r.users__name).sort()).toEqual([
      'Alice',
      'Charlie',
    ]);
  });

  it('should support multiple values in IN with SQL expression', async () => {
    const query: Query = {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'in',
          sqlExpression: '1, 2, 3',
        },
      ],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA] });

    const expectedSQL = `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE (users__id IN (1, 2, 3)) GROUP BY users__name`;

    expect(sql).toBe(expectedSQL);

    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(3);
    expect(output.map((r: any) => r.users__name).sort()).toEqual([
      'Alice',
      'Bob',
      'Charlie',
    ]);
  });

  it('should support NOT IN with multiple values in SQL expression', async () => {
    const query: Query = {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'notIn',
          sqlExpression: '1, 2',
        },
      ],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA] });

    const expectedSQL = `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE (users__id NOT IN (1, 2)) GROUP BY users__name`;

    expect(sql).toBe(expectedSQL);

    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(1);
    expect(output[0].users__name).toBe('Charlie');
  });

  it('should support combining IN SQL expression with traditional filters', async () => {
    const query: Query = {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          and: [
            {
              member: 'users.age',
              operator: 'gt',
              values: ['20'],
            },
            {
              member: 'users.id',
              operator: 'in',
              sqlExpression: '1, 3',
            },
          ],
        },
      ],
    };

    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA] });

    const expectedSQL = `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT age AS users__age, id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE ((users__age > 20) AND (users__id IN (1, 3))) GROUP BY users__name`;

    expect(sql).toBe(expectedSQL);

    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(2);
    expect(output.map((r: any) => r.users__name).sort()).toEqual([
      'Alice',
      'Charlie',
    ]);
  });

  it('should throw error for unsupported operators with SQL expression', async () => {
    const query: Query = {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.age',
          operator: 'gt',
          sqlExpression: '28',
        } as any,
      ],
    };

    await expect(
      cubeQueryToSQL({ query, tableSchemas: [SCHEMA] })
    ).rejects.toThrow(
      'SQL expressions are not supported for gt operator. Only "in" and "notIn" operators support SQL expressions.'
    );
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

    const expectedSQL = `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE (users__id IN (1, 2)) GROUP BY users__name`;

    expect(sql).toBe(expectedSQL);

    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(2);
    expect(output.map((r: any) => r.users__name).sort()).toEqual([
      'Alice',
      'Bob',
    ]);
  });
});
