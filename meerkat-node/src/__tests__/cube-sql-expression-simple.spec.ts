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

const SQL_EXPRESSION_TEST_DATA = [
  {
    testName: 'Simple IN with SQL expression',
    cubeInput: {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'in',
          sqlExpression: '1, 3',
        },
      ],
    },
    expectedSQL: `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE (users__id IN (1, 3)) GROUP BY users__name`,
    expectedOutput: [
      { users__count: 1n, users__name: 'Alice' },
      { users__count: 1n, users__name: 'Charlie' },
    ],
  },
  {
    testName: 'NOT IN with SQL expression',
    cubeInput: {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'notIn',
          sqlExpression: '2',
        },
      ],
    },
    expectedSQL: `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE (users__id NOT IN (2)) GROUP BY users__name`,
    expectedOutput: [
      { users__count: 1n, users__name: 'Alice' },
      { users__count: 1n, users__name: 'Charlie' },
    ],
  },
  {
    testName: 'Complex subquery with IN',
    cubeInput: {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'in',
          sqlExpression: 'SELECT id FROM (VALUES (1), (3)) AS t(id)',
        },
      ],
    },
    expectedSQL: `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE (users__id IN (SELECT id FROM (VALUES (1), (3)) AS t(id))) GROUP BY users__name`,
    expectedOutput: [
      { users__count: 1n, users__name: 'Alice' },
      { users__count: 1n, users__name: 'Charlie' },
    ],
  },
  {
    testName: 'AND combining equals and IN with SQL expression',
    cubeInput: {
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
    },
    expectedSQL: `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE ((users__name = 'Alice') AND (users__id IN (1, 2, 3))) GROUP BY users__name`,
    expectedOutput: [{ users__count: 1n, users__name: 'Alice' }],
  },
  {
    testName: 'OR combining IN with SQL expressions',
    cubeInput: {
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
    },
    expectedSQL: `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE ((users__id IN (1)) OR (users__id IN (3))) GROUP BY users__name`,
    expectedOutput: [
      { users__count: 1n, users__name: 'Alice' },
      { users__count: 1n, users__name: 'Charlie' },
    ],
  },
  {
    testName: 'NOT IN with subquery',
    cubeInput: {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'notIn',
          sqlExpression: 'SELECT 2',
        },
      ],
    },
    expectedSQL: `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE (users__id NOT IN (SELECT 2)) GROUP BY users__name`,
    expectedOutput: [
      { users__count: 1n, users__name: 'Alice' },
      { users__count: 1n, users__name: 'Charlie' },
    ],
  },
  {
    testName: 'Multiple values in IN with SQL expression',
    cubeInput: {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'in',
          sqlExpression: '1, 2, 3',
        },
      ],
    },
    expectedSQL: `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE (users__id IN (1, 2, 3)) GROUP BY users__name`,
    expectedOutput: [
      { users__count: 1n, users__name: 'Alice' },
      { users__count: 1n, users__name: 'Bob' },
      { users__count: 1n, users__name: 'Charlie' },
    ],
  },
  {
    testName: 'NOT IN with multiple values in SQL expression',
    cubeInput: {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'notIn',
          sqlExpression: '1, 2',
        },
      ],
    },
    expectedSQL: `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE (users__id NOT IN (1, 2)) GROUP BY users__name`,
    expectedOutput: [{ users__count: 1n, users__name: 'Charlie' }],
  },
  {
    testName: 'Combining IN SQL expression with gt filter',
    cubeInput: {
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
    },
    expectedSQL: `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT age AS users__age, id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE ((users__age > 20) AND (users__id IN (1, 3))) GROUP BY users__name`,
    expectedOutput: [
      { users__count: 1n, users__name: 'Alice' },
      { users__count: 1n, users__name: 'Charlie' },
    ],
  },
  {
    testName: 'Traditional IN with values array',
    cubeInput: {
      measures: ['users.count'],
      dimensions: ['users.name'],
      filters: [
        {
          member: 'users.id',
          operator: 'in',
          values: ['1', '2'],
        },
      ],
    },
    expectedSQL: `SELECT COUNT(*) AS users__count ,   users__name FROM (SELECT id AS users__id, name AS users__name, * FROM (
    SELECT * FROM (
      VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    ) AS t(id, name, age)
  ) AS users) AS users WHERE (users__id IN (1, 2)) GROUP BY users__name`,
    expectedOutput: [
      { users__count: 1n, users__name: 'Alice' },
      { users__count: 1n, users__name: 'Bob' },
    ],
  },
];

describe('SQL Expression Filters', () => {
  describe('useDotNotation: false (default)', () => {
    SQL_EXPRESSION_TEST_DATA.forEach((testCase) => {
      it(testCase.testName, async () => {
        const sql = await cubeQueryToSQL({
          query: testCase.cubeInput,
          tableSchemas: [SCHEMA],
          options: { useDotNotation: false },
        });

        // Compare generated SQL with expected SQL
        expect(sql).toBe(testCase.expectedSQL);

        // Execute query and validate results
        const output = await duckdbExec(sql);

        // Sort both actual and expected output by user name for consistent comparison
        const sortedOutput = [...output].sort((a, b) =>
          a.users__name.localeCompare(b.users__name)
        );
        const sortedExpected = [...testCase.expectedOutput].sort((a, b) =>
          a.users__name.localeCompare(b.users__name)
        );

        // Compare output with expected output (use toEqual for BigInt compatibility)
        expect(sortedOutput).toEqual(sortedExpected);
      });
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
        cubeQueryToSQL({
          query,
          tableSchemas: [SCHEMA],
          options: { useDotNotation: false },
        })
      ).rejects.toThrow(
        'SQL expressions are not supported for gt operator. Only "in" and "notIn" operators support SQL expressions.'
      );
    });
  });

  describe('useDotNotation: true (parity)', () => {
    const toDotNotation = (rows: any[]) =>
      rows.map((row) =>
        Object.fromEntries(
          Object.entries(row).map(([k, v]) => [k.replace(/__/g, '.'), v])
        )
      );

    SQL_EXPRESSION_TEST_DATA.forEach((testCase) => {
      it(testCase.testName, async () => {
        const sql = await cubeQueryToSQL({
          query: testCase.cubeInput,
          tableSchemas: [SCHEMA],
          options: { useDotNotation: true },
        });

        // Basic SQL sanity for dot notation aliases
        expect(sql).toContain('"users.count"');
        expect(sql).toContain('"users.name"');

        const output = await duckdbExec(sql);
        const sortedOutput = [...output].sort((a: any, b: any) =>
          a['users.name'].localeCompare(b['users.name'])
        );
        const expected = toDotNotation(testCase.expectedOutput).sort((a, b) =>
          a['users.name'].localeCompare(b['users.name'])
        );
        expect(sortedOutput).toEqual(expected);
      });
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
        cubeQueryToSQL({
          query,
          tableSchemas: [SCHEMA],
          options: { useDotNotation: true },
        })
      ).rejects.toThrow(
        'SQL expressions are not supported for gt operator. Only "in" and "notIn" operators support SQL expressions.'
      );
    });
  });
});
