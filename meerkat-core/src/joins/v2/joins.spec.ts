import {
  MeerkatQueryFilter,
  StructuredJoin,
  TableSchema,
} from '../../types/cube-types';
import { GetQueryOutput } from '../../utils/duckdb-ast-parse-serialize';
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
  it('emits a plain equi-join when from is scalar', async () => {
    const schemas = [
      scalar('orders', ['id', 'customer_id']),
      scalar('customers'),
    ];
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
    const sql = await generateSqlQueryV2(paths, sqlMap, graph, schemas);

    expect(sql).not.toMatch(/UNNEST/);
    expect(sql).toContain('orders.customer_id = customers.id');
  });

  it('wraps the base with UNNEST when from.column is array-typed', async () => {
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
    const sql = await generateSqlQueryV2(paths, sqlMap, graph, schemas);

    expect(sql).toContain('UNNEST(owned_by_ids) AS __mk_u_owned_by_ids');
    expect(sql).toContain('issues.__mk_u_owned_by_ids = users.id');
    expect(sql).not.toMatch(/CONTAINS/i);
  });

  it('shares one UNNEST projection across edges on the same base array column', async () => {
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
    const sql = await generateSqlQueryV2(paths, sqlMap, graph, schemas);

    expect(sql.match(/UNNEST\(owned_by_ids\)/g)).toHaveLength(1);
    expect(sql).toContain('issues.__mk_u_owned_by_ids = users.id');
    expect(sql).toContain('issues.__mk_u_owned_by_ids = admins.id');
  });

  it('inlines dim.sql for composite-child synthetic array columns whose name is not a real column', async () => {
    // The synthetic dim `tags_$0_tag_id` is not a real column on the
    // base table — only the parent `tags` struct array is. UNNEST must
    // reference the dim's `sql` expression, not the synthetic name.
    const schemas: TableSchema[] = [
      {
        name: 'parts',
        sql: 'select * from parts',
        dimensions: [
          { name: 'id', sql: 'parts.id', type: 'string' },
          {
            name: 'tags_$0_tag_id',
            sql: "json_extract_string(tags, '$[*].tag_id')",
            type: 'string_array',
          },
        ],
        measures: [],
        joins: [],
      },
      scalar('tags'),
    ];
    const sqlMap = sqlMapOf(schemas);
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'parts', column: 'tags_$0_tag_id' },
          to: { table: 'tags', column: 'id' },
        },
      ],
    ];
    const graph = createDirectedGraphV2(schemas, sqlMap, paths);
    const sql = await generateSqlQueryV2(paths, sqlMap, graph, schemas);

    expect(sql).toContain(
      "UNNEST(json_extract_string(tags, '$[*].tag_id')) AS __mk_u_tags_$0_tag_id"
    );
    expect(sql).toContain('parts.__mk_u_tags_$0_tag_id = tags.id');
  });

  it('wraps a multi-hop intermediate table when its from.column is array-typed', async () => {
    const schemas = [
      scalar('tickets', ['id', 'part_id']),
      withArrayCols('parts', ['id'], ['tag_ids']),
      scalar('tags'),
    ];
    const sqlMap = sqlMapOf(schemas);
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'tickets', column: 'part_id' },
          to: { table: 'parts', column: 'id' },
        },
        {
          from: { table: 'parts', column: 'tag_ids' },
          to: { table: 'tags', column: 'id' },
        },
      ],
    ];
    const graph = createDirectedGraphV2(schemas, sqlMap, paths);
    const sql = await generateSqlQueryV2(paths, sqlMap, graph, schemas);

    expect(sql).toContain('tickets.part_id = parts.id');
    expect(sql).toContain('UNNEST(tag_ids) AS __mk_u_tag_ids');
    expect(sql).toContain('parts.__mk_u_tag_ids = tags.id');
  });

  it('strips table qualifier from CAST-wrapped array dim sql (ensureTableSchemasAlias output)', async () => {
    // After ensureTableSchemasAlias runs, the dim's sql becomes
    // `CAST(issue.owned_by_ids AS VARCHAR[])` instead of plain `owned_by_ids`.
    // The UNNEST wrap must strip the `issue.` qualifier since the inner
    // subquery is unnamed at that scope.
    const schemas: TableSchema[] = [
      {
        name: 'issue',
        sql: 'SELECT CAST(issue.owned_by_ids AS VARCHAR[]) AS issue__owned_by_ids, issue.id AS issue__id, * FROM (select * from devrev.issue) AS issue',
        dimensions: [
          { name: 'id', sql: 'issue.id', type: 'string' },
          {
            name: 'owned_by_ids',
            sql: 'CAST(issue.owned_by_ids AS VARCHAR[])',
            type: 'string_array',
          },
        ],
        measures: [],
        joins: [],
      },
      scalar('users'),
    ];
    const sqlMap = sqlMapOf(schemas);
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'issue', column: 'owned_by_ids' },
          to: { table: 'users', column: 'id' },
        },
      ],
    ];
    const graph = createDirectedGraphV2(schemas, sqlMap, paths);
    const sql = await generateSqlQueryV2(paths, sqlMap, graph, schemas);

    // The UNNEST expression must NOT contain `issue.` since it's in an unnamed subquery scope
    expect(sql).toContain('UNNEST(CAST(owned_by_ids AS VARCHAR[]))');
    expect(sql).not.toMatch(/UNNEST\(CAST\(issue\./);
    expect(sql).toContain('issue.__mk_u_owned_by_ids = users.id');
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

  it('aliases bridge tables when same table appears multiple times in a path', async () => {
    const schemas = [
      scalar('issue', ['id']),
      scalar('link', ['id', 'source_id', 'target_id', 'link_type_id']),
      scalar('part', ['id']),
      scalar('user', ['id']),
    ];
    const sqlMap = sqlMapOf(schemas);
    const bridgeTables = new Set(['link']);
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'issue', column: 'id' },
          to: { table: 'link', column: 'source_id' },
        },
        {
          from: { table: 'link', column: 'target_id' },
          to: { table: 'part', column: 'id' },
        },
        {
          from: { table: 'issue', column: 'id' },
          to: { table: 'link', column: 'source_id' },
        },
        {
          from: { table: 'link', column: 'target_id' },
          to: { table: 'user', column: 'id' },
        },
      ],
    ];
    const graph = createDirectedGraphV2(schemas, sqlMap, paths);
    const sql = await generateSqlQueryV2(
      paths,
      sqlMap,
      graph,
      schemas,
      undefined,
      bridgeTables
    );

    expect(sql).toContain('issue.id = link.source_id');
    expect(sql).toContain('link.target_id = part.id');
    expect(sql).toContain('issue.id = link__1.source_id');
    expect(sql).toContain('link__1.target_id = user.id');
  });

  it('aliases bridge tables across separate paths', async () => {
    const schemas = [
      scalar('issue', ['id']),
      scalar('link', ['id', 'source_id', 'target_id', 'link_type_id']),
      scalar('part', ['id']),
      scalar('user', ['id']),
    ];
    const sqlMap = sqlMapOf(schemas);
    const bridgeTables = new Set(['link']);
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'issue', column: 'id' },
          to: { table: 'link', column: 'source_id' },
        },
        {
          from: { table: 'link', column: 'target_id' },
          to: { table: 'part', column: 'id' },
        },
      ],
      [
        {
          from: { table: 'issue', column: 'id' },
          to: { table: 'link', column: 'source_id' },
        },
        {
          from: { table: 'link', column: 'target_id' },
          to: { table: 'user', column: 'id' },
        },
      ],
    ];
    const graph = createDirectedGraphV2(schemas, sqlMap, paths);
    const sql = await generateSqlQueryV2(
      paths,
      sqlMap,
      graph,
      schemas,
      undefined,
      bridgeTables
    );

    expect(sql).toContain('issue.id = link.source_id');
    expect(sql).toContain('link.target_id = part.id');
    expect(sql).toContain('issue.id = link__1.source_id');
    expect(sql).toContain('link__1.target_id = user.id');
  });

  it('does not alias when table is not a bridge even if it repeats', async () => {
    const schemas = [
      scalar('issue', ['id']),
      scalar('link', ['id', 'source_id', 'target_id']),
      scalar('part', ['id']),
    ];
    const sqlMap = sqlMapOf(schemas);
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'issue', column: 'id' },
          to: { table: 'link', column: 'source_id' },
        },
        {
          from: { table: 'link', column: 'target_id' },
          to: { table: 'part', column: 'id' },
        },
        {
          from: { table: 'issue', column: 'id' },
          to: { table: 'link', column: 'source_id' },
        },
      ],
    ];
    const graph = createDirectedGraphV2(schemas, sqlMap, paths);
    const sql = await generateSqlQueryV2(paths, sqlMap, graph, schemas);

    expect(sql).not.toContain('link__1');
  });

  it('throws when condition is present but getQueryOutput is not provided', async () => {
    const schemas = [
      scalar('issue', ['id']),
      scalar('link', ['id', 'source_id', 'link_type_id']),
    ];
    const sqlMap = sqlMapOf(schemas);
    const condition: MeerkatQueryFilter = {
      member: 'link.link_type_id',
      operator: 'equals',
      values: ['1234'],
    };
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'issue', column: 'id' },
          to: { table: 'link', column: 'source_id' },
          condition,
        },
      ],
    ];
    const graph = createDirectedGraphV2(schemas, sqlMap, paths);
    await expect(
      generateSqlQueryV2(paths, sqlMap, graph, schemas)
    ).rejects.toThrow(/getQueryOutput is required/);
  });

  it('appends serialized condition to ON clause when getQueryOutput is provided', async () => {
    const schemas = [
      scalar('issue', ['id']),
      scalar('link', ['id', 'source_id', 'link_type_id']),
    ];
    const sqlMap = sqlMapOf(schemas);
    const condition: MeerkatQueryFilter = {
      member: 'link.link_type_id',
      operator: 'equals',
      values: ['1234'],
    };
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'issue', column: 'id' },
          to: { table: 'link', column: 'source_id' },
          condition,
        },
      ],
    ];
    const graph = createDirectedGraphV2(schemas, sqlMap, paths);

    const mockGetQueryOutput: GetQueryOutput = async () => {
      return [
        {
          result: "SELECT (link_type_id = '1234') AS __meerkat_batch_expr_0__;",
        },
      ];
    };

    const sql = await generateSqlQueryV2(
      paths,
      sqlMap,
      graph,
      schemas,
      mockGetQueryOutput
    );

    expect(sql).toContain('issue.id = link.source_id');
    expect(sql).toContain("AND (link_type_id = '1234')");
  });

  it('serializes multiple conditions across edges in a single batch', async () => {
    const schemas = [
      scalar('issue', ['id']),
      scalar('link', ['id', 'source_id', 'target_id', 'link_type_id']),
      scalar('part', ['id']),
      scalar('user', ['id']),
    ];
    const sqlMap = sqlMapOf(schemas);
    const bridgeTables = new Set(['link']);
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'issue', column: 'id' },
          to: { table: 'link', column: 'source_id' },
          condition: {
            member: 'link.link_type_id',
            operator: 'equals',
            values: ['type_a'],
          },
        },
        {
          from: { table: 'link', column: 'target_id' },
          to: { table: 'part', column: 'id' },
        },
        {
          from: { table: 'part', column: 'id' },
          to: { table: 'link', column: 'source_id' },
          condition: {
            member: 'link.link_type_id',
            operator: 'equals',
            values: ['type_b'],
          },
        },
        {
          from: { table: 'link', column: 'target_id' },
          to: { table: 'user', column: 'id' },
        },
      ],
    ];
    const graph = createDirectedGraphV2(schemas, sqlMap, paths);

    let callCount = 0;
    const mockGetQueryOutput: GetQueryOutput = async () => {
      callCount++;
      return [
        {
          result:
            "SELECT (link_type_id = 'type_a') AS __meerkat_batch_expr_0__, (link_type_id = 'type_b') AS __meerkat_batch_expr_1__;",
        },
      ];
    };

    const sql = await generateSqlQueryV2(
      paths,
      sqlMap,
      graph,
      schemas,
      mockGetQueryOutput,
      bridgeTables
    );

    expect(callCount).toBe(1);
    expect(sql).toContain(
      "issue.id = link.source_id AND (link_type_id = 'type_a')"
    );
    expect(sql).toContain('link.target_id = part.id');
    expect(sql).toContain(
      "part.id = link__1.source_id AND (link_type_id = 'type_b')"
    );
    expect(sql).toContain('link__1.target_id = user.id');
  });

  it('rewrites condition members to the aliased bridge table (link__1)', async () => {
    // Reproduces: Issue → link (Child of) → Enhancement and Issue → link
    // (Dependency of) → Ticket. The second link join must filter on
    // link__1.link_type_id, not the first link's link_type_id.
    const schemas = [
      scalar('issue', ['id']),
      scalar('link', ['id', 'source_id', 'target_id', 'link_type_id']),
      scalar('enhancement', ['id', 'name']),
      scalar('ticket', ['id', 'severity']),
    ];
    const sqlMap = sqlMapOf(schemas);
    const bridgeTables = new Set(['link']);
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'issue', column: 'id' },
          to: { table: 'link', column: 'target_id' },
          condition: {
            member: 'link.link_type_id',
            operator: 'equals',
            values: ['custom_link_type/20'],
          },
        },
        {
          from: { table: 'link', column: 'source_id' },
          to: { table: 'enhancement', column: 'id' },
        },
      ],
      [
        {
          from: { table: 'issue', column: 'id' },
          to: { table: 'link', column: 'target_id' },
          condition: {
            member: 'link.link_type_id',
            operator: 'equals',
            values: ['custom_link_type/default-20'],
          },
        },
        {
          from: { table: 'link', column: 'source_id' },
          to: { table: 'ticket', column: 'id' },
        },
      ],
    ];
    const graph = createDirectedGraphV2(schemas, sqlMap, paths);

    let serializedAstQuery = '';
    const mockGetQueryOutput: GetQueryOutput = async (query) => {
      serializedAstQuery = query;
      return [
        {
          result:
            "SELECT (link.link_type_id = 'custom_link_type/20') AS __meerkat_batch_expr_0__, (link__1.link_type_id = 'custom_link_type/default-20') AS __meerkat_batch_expr_1__;",
        },
      ];
    };

    const sql = await generateSqlQueryV2(
      paths,
      sqlMap,
      graph,
      schemas,
      mockGetQueryOutput,
      bridgeTables
    );

    // Condition AST must reference the aliased instance — this is what
    // production DuckDB serialization emits as link__1.link_type_id.
    expect(serializedAstQuery).toContain(
      '"column_names":["link","link_type_id"]'
    );
    expect(serializedAstQuery).toContain(
      '"column_names":["link__1","link_type_id"]'
    );
    expect(sql).toContain(
      "issue.id = link.target_id AND (link.link_type_id = 'custom_link_type/20')"
    );
    expect(sql).toContain(
      "issue.id = link__1.target_id AND (link__1.link_type_id = 'custom_link_type/default-20')"
    );
    expect(sql).not.toContain(
      "issue.id = link__1.target_id AND (link.link_type_id = 'custom_link_type/default-20')"
    );
  });

  it('handles notSet condition (IS NULL)', async () => {
    const schemas = [
      scalar('issue', ['id']),
      scalar('link', ['id', 'source_id', 'deleted_at']),
    ];
    const sqlMap = sqlMapOf(schemas);
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'issue', column: 'id' },
          to: { table: 'link', column: 'source_id' },
          condition: {
            member: 'link.deleted_at',
            operator: 'notSet',
          },
        },
      ],
    ];
    const graph = createDirectedGraphV2(schemas, sqlMap, paths);

    const mockGetQueryOutput: GetQueryOutput = async () => {
      return [
        {
          result: 'SELECT (deleted_at IS NULL) AS __meerkat_batch_expr_0__;',
        },
      ];
    };

    const sql = await generateSqlQueryV2(
      paths,
      sqlMap,
      graph,
      schemas,
      mockGetQueryOutput
    );

    expect(sql).toContain('issue.id = link.source_id AND (deleted_at IS NULL)');
  });

  it('handles OR expression with multiple conditions', async () => {
    const schemas = [
      scalar('issue', ['id']),
      scalar('link', ['id', 'source_id', 'link_type_id']),
    ];
    const sqlMap = sqlMapOf(schemas);
    const condition: MeerkatQueryFilter = {
      or: [
        { member: 'link.link_type_id', operator: 'equals', values: ['type_a'] },
        { member: 'link.link_type_id', operator: 'equals', values: ['type_b'] },
      ],
    };
    const paths: StructuredJoin[][] = [
      [
        {
          from: { table: 'issue', column: 'id' },
          to: { table: 'link', column: 'source_id' },
          condition,
        },
      ],
    ];
    const graph = createDirectedGraphV2(schemas, sqlMap, paths);

    const mockGetQueryOutput: GetQueryOutput = async () => {
      return [
        {
          result:
            "SELECT ((link_type_id = 'type_a') OR (link_type_id = 'type_b')) AS __meerkat_batch_expr_0__;",
        },
      ];
    };

    const sql = await generateSqlQueryV2(
      paths,
      sqlMap,
      graph,
      schemas,
      mockGetQueryOutput
    );

    expect(sql).toContain('issue.id = link.source_id AND');
    expect(sql).toContain(
      "(link_type_id = 'type_a') OR (link_type_id = 'type_b')"
    );
  });
});
