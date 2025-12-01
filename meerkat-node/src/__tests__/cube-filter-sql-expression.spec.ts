import { Query, TableSchema } from '@devrev/meerkat-core';
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

const USERS_SCHEMA: TableSchema = {
  name: 'users',
  sql: `
    SELECT * FROM (
      VALUES
        (1, 'Alice', 'active', 25, 'US'),
        (2, 'Bob', 'inactive', 30, 'UK'),
        (3, 'Charlie', 'active', 35, 'US'),
        (4, 'David', 'pending', 28, 'CA'),
        (5, 'Eve', 'active', 32, 'US')
    ) AS t(id, name, status, age, country)
  `,
  measures: [
    {
      name: 'count',
      sql: 'COUNT(*)',
      type: 'number',
    },
    {
      name: 'id',
      sql: 'id',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'name',
      sql: 'name',
      type: 'string',
    },
    {
      name: 'status',
      sql: 'status',
      type: 'string',
    },
    {
      name: 'age',
      sql: 'age',
      type: 'number',
    },
    {
      name: 'country',
      sql: 'country',
      type: 'string',
    },
  ],
  joins: [],
};

describe('SQL Expression Filters', () => {
  describe('Basic SQL Expression Support', () => {
    it('should support IN with SQL subquery', async () => {
      const query: Query = {
        measures: ['users.count'],
        dimensions: ['users.name'],
        filters: [
          {
            member: 'users.id',
            operator: 'in',
            sql: '(1, 3, 5)',
          },
        ],
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [USERS_SCHEMA] });
      expect(sql).toContain('__meerkat_raw_sql__');

      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(3);
      expect(output.map((r: any) => r.users__name).sort()).toEqual([
        'Alice',
        'Charlie',
        'Eve',
      ]);
    });

    it('should support EQUALS with SQL expression', async () => {
      const query: Query = {
        measures: ['users.count'],
        dimensions: ['users.name'],
        filters: [
          {
            member: 'users.status',
            operator: 'equals',
            sql: "{member} = 'active'",
          },
        ],
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [USERS_SCHEMA] });
      const output: any = await duckdbExec(sql);

      expect(output).toHaveLength(3);
      expect(output.every((r: any) => r.users__name)).toBeTruthy();
    });

    it('should support GT with SQL expression using subquery', async () => {
      const query: Query = {
        measures: ['users.count'],
        dimensions: ['users.name', 'users.age'],
        filters: [
          {
            member: 'users.age',
            operator: 'gt',
            sql: '{member} > (SELECT AVG(age) FROM (VALUES (25), (30), (35), (28), (32)) AS t(age))',
          },
        ],
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [USERS_SCHEMA] });
      const output: any = await duckdbExec(sql);

      // Average is 30, so should get ages > 30
      expect(output.length).toBeGreaterThan(0);
      expect(output.every((r: any) => r.users__age > 30)).toBeTruthy();
    });
  });

  describe('SQL Expression with Logical Operators', () => {
    it('should support AND with mixed values and SQL', async () => {
      const query: Query = {
        measures: ['users.count'],
        dimensions: ['users.name'],
        filters: [
          {
            and: [
              {
                member: 'users.status',
                operator: 'equals',
                values: ['active'],
              },
              {
                member: 'users.country',
                operator: 'in',
                sql: "{member} IN ('US', 'CA')",
              },
            ],
          },
        ],
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [USERS_SCHEMA] });
      const output: any = await duckdbExec(sql);

      expect(output).toHaveLength(3);
      expect(output.map((r: any) => r.users__name).sort()).toEqual([
        'Alice',
        'Charlie',
        'Eve',
      ]);
    });

    it('should support OR with SQL expressions', async () => {
      const query: Query = {
        measures: ['users.count'],
        dimensions: ['users.name', 'users.age'],
        filters: [
          {
            or: [
              {
                member: 'users.age',
                operator: 'lt',
                sql: '{member} < 27',
              },
              {
                member: 'users.age',
                operator: 'gt',
                sql: '{member} > 33',
              },
            ],
          },
        ],
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [USERS_SCHEMA] });
      const output: any = await duckdbExec(sql);

      // Should get ages < 27 OR > 33
      expect(output.length).toBeGreaterThan(0);
      expect(
        output.every((r: any) => r.users__age < 27 || r.users__age > 33)
      ).toBeTruthy();
    });
  });

  describe('Complex SQL Expressions', () => {
    it('should support CONTAINS with custom SQL', async () => {
      const query: Query = {
        measures: ['users.count'],
        dimensions: ['users.name'],
        filters: [
          {
            member: 'users.name',
            operator: 'contains',
            sql: "LOWER({member}) LIKE '%li%'",
          },
        ],
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [USERS_SCHEMA] });
      const output: any = await duckdbExec(sql);

      // Should match Alice and Charlie
      expect(output.length).toBeGreaterThanOrEqual(2);
    });

    it('should support NOT IN with SQL expression', async () => {
      const query: Query = {
        measures: ['users.count'],
        dimensions: ['users.name'],
        filters: [
          {
            member: 'users.id',
            operator: 'notIn',
            sql: '{member} NOT IN (2, 4)',
          },
        ],
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [USERS_SCHEMA] });
      const output: any = await duckdbExec(sql);

      expect(output).toHaveLength(3);
      expect(output.map((r: any) => r.users__name).sort()).toEqual([
        'Alice',
        'Charlie',
        'Eve',
      ]);
    });
  });

  describe('Backwards Compatibility', () => {
    it('should still work with traditional values array', async () => {
      const query: Query = {
        measures: ['users.count'],
        dimensions: ['users.name'],
        filters: [
          {
            member: 'users.status',
            operator: 'in',
            values: ['active', 'pending'],
          },
        ],
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [USERS_SCHEMA] });
      const output: any = await duckdbExec(sql);

      expect(output).toHaveLength(4);
    });

    it('should work with equals and single value', async () => {
      const query: Query = {
        measures: ['users.count'],
        dimensions: ['users.name'],
        filters: [
          {
            member: 'users.country',
            operator: 'equals',
            values: ['US'],
          },
        ],
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [USERS_SCHEMA] });
      const output: any = await duckdbExec(sql);

      expect(output).toHaveLength(3);
      expect(output.every((r: any) => r.users__name)).toBeTruthy();
    });
  });
});
