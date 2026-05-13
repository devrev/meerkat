import { StructuredJoin, TableSchema } from '../../types/cube-types';
import { createDirectedGraphV2, generateSqlQueryV2 } from './joins';

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

const sqlMapOf = (schemas: TableSchema[]): { [k: string]: string } =>
  schemas.reduce<{ [k: string]: string }>(
    (acc, s) => ({ ...acc, [s.name]: s.sql }),
    {}
  );

describe('joins-v2', () => {
  it('emits a plain equi-join when from is scalar', () => {
    const schemas = [scalar('orders', ['id', 'customer_id']), scalar('customers')];
    const sqlMap = sqlMapOf(schemas);
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'orders', column: 'customer_id' },
          to: { table: 'customers', column: 'id' },
        },
      ],
    ];
    const graph = createDirectedGraphV2(schemas, sqlMap, paths);
    const sql = generateSqlQueryV2(paths, sqlMap, graph, schemas);

    expect(sql).not.toMatch(/UNNEST/);
    expect(sql).toContain('orders.customer_id = customers.id');
  });

  it('wraps the base with UNNEST when from.column is array-typed', () => {
    const schemas = [
      withArrayCols('issues', ['id'], ['owned_by_ids']),
      scalar('users'),
    ];
    const sqlMap = sqlMapOf(schemas);
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'issues', column: 'owned_by_ids' },
          to: { table: 'users', column: 'id' },
        },
      ],
    ];
    const graph = createDirectedGraphV2(schemas, sqlMap, paths);
    const sql = generateSqlQueryV2(paths, sqlMap, graph, schemas);

    expect(sql).toContain('UNNEST(owned_by_ids) AS __mk_u_owned_by_ids');
    expect(sql).toContain('issues.__mk_u_owned_by_ids = users.id');
    expect(sql).not.toMatch(/CONTAINS/i);
  });

  it('shares one UNNEST projection across edges on the same base array column', () => {
    const schemas = [
      withArrayCols('issues', ['id'], ['owned_by_ids']),
      scalar('users'),
      scalar('admins'),
    ];
    const sqlMap = sqlMapOf(schemas);
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'issues', column: 'owned_by_ids' },
          to: { table: 'users', column: 'id' },
        },
        {
          from: { table: 'issues', column: 'owned_by_ids' },
          to: { table: 'admins', column: 'id' },
        },
      ],
    ];
    const graph = createDirectedGraphV2(schemas, sqlMap, paths);
    const sql = generateSqlQueryV2(paths, sqlMap, graph, schemas);

    expect(sql.match(/UNNEST\(owned_by_ids\)/g)).toHaveLength(1);
    expect(sql).toContain('issues.__mk_u_owned_by_ids = users.id');
    expect(sql).toContain('issues.__mk_u_owned_by_ids = admins.id');
  });

  it('throws when both sides of an edge are array-typed', () => {
    const schemas = [
      withArrayCols('issues', ['id'], ['owned_by_ids']),
      withArrayCols('groups', ['id'], ['member_ids']),
    ];
    const sqlMap = sqlMapOf(schemas);
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'issues', column: 'owned_by_ids' },
          to: { table: 'groups', column: 'member_ids' },
        },
      ],
    ];
    expect(() => createDirectedGraphV2(schemas, sqlMap, paths)).toThrow(
      /array-array joins are not supported/
    );
  });

});
