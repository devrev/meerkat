import { Query, TableSchema } from '../types/cube-types';
import {
  getCombinedTableSchema,
  hasAnyJoinPaths,
  hasJoinPathsV2,
} from './index';

const scalar = (name: string, cols: string[] = ['id']): TableSchema => ({
  name,
  sql: `select * from ${name}`,
  dimensions: cols.map((c) => ({
    name: c,
    sql: `${name}.${c}`,
    type: 'string' as const,
  })),
  measures: [],
  joins: [],
});

const withArrayCols = (
  name: string,
  scalarCols: string[],
  arrayCols: string[]
): TableSchema => ({
  name,
  sql: `select * from ${name}`,
  dimensions: [
    ...scalarCols.map((c) => ({
      name: c,
      sql: `${name}.${c}`,
      type: 'string' as const,
    })),
    ...arrayCols.map((c) => ({
      name: c,
      sql: `${name}.${c}`,
      type: 'string_array' as const,
    })),
  ],
  measures: [],
  joins: [],
});

describe('joins router', () => {
  it('routes to v2 when joinPathsV2 is set', () => {
    const schemas = [
      withArrayCols('issues', ['id'], ['owned_by_ids']),
      scalar('users'),
    ];
    const cubeQuery: Query = {
      measures: [],
      dimensions: ['users.id'],
      joinPathsV2: [
        [
          {
            from: { table: 'issues', column: 'owned_by_ids' },
            to: { table: 'users', column: 'id' },
          },
        ],
      ],
    };
    const result = getCombinedTableSchema(schemas, cubeQuery);
    expect(result.sql).toContain('UNNEST(owned_by_ids)');
    expect(result.sql).not.toMatch(/CONTAINS/i);
  });

  it('routes to v1 when joinPathsV2 is absent', () => {
    const schemas = [
      {
        ...scalar('orders', ['id', 'customer_id']),
        joins: [{ sql: 'orders.customer_id = customers.id' }],
      },
      scalar('customers'),
    ];
    const cubeQuery: Query = {
      measures: [],
      dimensions: ['customers.id'],
      joinPaths: [[{ left: 'orders', right: 'customers', on: 'customer_id' }]],
    };
    const result = getCombinedTableSchema(schemas, cubeQuery);
    expect(result.sql).toContain('orders.customer_id = customers.id');
    expect(result.sql).not.toMatch(/UNNEST/);
  });
});

describe('join-path accessors', () => {
  const baseQuery: Query = { measures: [], dimensions: [] };

  it('hasAnyJoinPaths returns false for empty queries', () => {
    expect(hasAnyJoinPaths(baseQuery)).toBe(false);
    expect(hasAnyJoinPaths({ ...baseQuery, joinPaths: [] })).toBe(false);
    expect(hasAnyJoinPaths({ ...baseQuery, joinPathsV2: [] })).toBe(false);
  });

  it('hasAnyJoinPaths returns true for either legacy or v2 paths', () => {
    expect(
      hasAnyJoinPaths({
        ...baseQuery,
        joinPaths: [[{ left: 'a', right: 'b', on: 'id' }]],
      })
    ).toBe(true);
    expect(
      hasAnyJoinPaths({
        ...baseQuery,
        joinPathsV2: [
          [{ from: { table: 'a', column: 'id' }, to: { table: 'b', column: 'id' } }],
        ],
      })
    ).toBe(true);
  });

  it('hasJoinPathsV2 only matches the v2 field', () => {
    expect(hasJoinPathsV2(baseQuery)).toBe(false);
    expect(
      hasJoinPathsV2({
        ...baseQuery,
        joinPaths: [[{ left: 'a', right: 'b', on: 'id' }]],
      })
    ).toBe(false);
    expect(
      hasJoinPathsV2({
        ...baseQuery,
        joinPathsV2: [
          [{ from: { table: 'a', column: 'id' }, to: { table: 'b', column: 'id' } }],
        ],
      })
    ).toBe(true);
  });
});
