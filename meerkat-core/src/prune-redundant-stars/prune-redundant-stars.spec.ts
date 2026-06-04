import { pruneRedundantStarsOverJoins } from './prune-redundant-stars';

/**
 * Builds a `json_serialize_sql` mock response for a single statement whose AST
 * is `selectNode`. The pass only ever issues one serialize query, so the mock
 * returns this for any input.
 */
const serializeResponse = (selectNode: unknown) => [
  {
    json_serialize_sql: JSON.stringify({
      error: false,
      statements: [{ node: selectNode }],
    }),
  },
];

const columnRef = (name: string, location?: number) => ({
  class: 'COLUMN_REF',
  type: 'COLUMN_REF',
  alias: '',
  column_names: [name],
  ...(location !== undefined ? { query_location: location } : {}),
});

const bareStar = (location: number) => ({
  class: 'STAR',
  type: 'STAR',
  alias: '',
  query_location: location,
  relation_name: '',
  exclude_list: [],
  replace_list: [],
  columns: false,
  expr: null,
});

const subqueryTable = (alias: string, selectList: unknown[]) => ({
  type: 'SUBQUERY',
  alias,
  subquery: { node: { type: 'SELECT_NODE', select_list: selectList } },
});

describe('pruneRedundantStarsOverJoins', () => {
  it('replaces a bare star over a JOIN with the referenced, unprojected columns', async () => {
    // SQL shape:
    //   SELECT base.id AS id, * FROM (base) LEFT JOIN (lookup) ...
    // where an outer scope references `resolved_name` (carried only by `*`),
    // while `devrev_copy` is referenced by nobody.
    const starLocation = 22;
    const sql = 'SELECT base.id AS id, * FROM base LEFT JOIN lookup';

    const selectNode = {
      type: 'SELECT_NODE',
      select_list: [columnRef('id', 7), bareStar(starLocation)],
      from_table: {
        type: 'JOIN',
        left: subqueryTable('base', [columnRef('id')]),
        // The lookup projects `resolved_name` explicitly and passes its raw
        // columns (incl. `devrev_copy`) through an inner star — exactly the real
        // shape. `devrev_copy` is therefore never a COLUMN_REF anywhere.
        right: subqueryTable('lookup', [columnRef('resolved_name'), bareStar(99)]),
      },
      // An outer reference to `resolved_name` so it counts as "needed".
      where_clause: columnRef('resolved_name'),
    };

    const executeQuery = jest.fn(async () => serializeResponse(selectNode));

    const result = await pruneRedundantStarsOverJoins(sql, executeQuery);

    expect(result.rewrittenStars).toBe(1);
    // `resolved_name` is referenced and not already projected → kept.
    // `id` is already projected → skipped. `devrev_copy` unreferenced → dropped.
    expect(result.sql).toBe(
      'SELECT base.id AS id, "resolved_name" FROM base LEFT JOIN lookup'
    );
  });

  it('drops the star (and its comma) when no extra columns are needed', async () => {
    const starLocation = 22;
    const sql = 'SELECT base.id AS id, * FROM base LEFT JOIN lookup';

    const selectNode = {
      type: 'SELECT_NODE',
      select_list: [columnRef('id', 7), bareStar(starLocation)],
      from_table: {
        type: 'JOIN',
        left: subqueryTable('base', [columnRef('id')]),
        // Lookup exposes only raw passthrough columns (via an inner star) that
        // nothing references → the outer star contributes nothing.
        right: subqueryTable('lookup', [bareStar(99)]),
      },
      // Nothing outside references the lookup columns.
    };

    const executeQuery = jest.fn(async () => serializeResponse(selectNode));

    const result = await pruneRedundantStarsOverJoins(sql, executeQuery);

    expect(result.rewrittenStars).toBe(1);
    // `id` already projected, `devrev_copy` unreferenced → star removed cleanly.
    expect(result.sql).toBe('SELECT base.id AS id FROM base LEFT JOIN lookup');
  });

  it('locates the star by BYTE offset when a multibyte char precedes it', async () => {
    // `query_location` is a UTF-8 byte offset. The 'é' in the literal makes the
    // star's byte offset (21) differ from its UTF-16 index (20). Indexing the
    // JS string with the raw byte offset would miss the star and silently skip
    // the fix — the exact ISS-301213 failure on customer data with accents.
    const sql = "SELECT 'café' AS c, * FROM base LEFT JOIN lookup";
    expect(sql.indexOf('*')).toBe(20);
    const starByteOffset = 21;

    const selectNode = {
      type: 'SELECT_NODE',
      select_list: [columnRef('c', 7), bareStar(starByteOffset)],
      from_table: {
        type: 'JOIN',
        left: subqueryTable('base', [columnRef('c')]),
        right: subqueryTable('lookup', [columnRef('resolved_name'), bareStar(99)]),
      },
      where_clause: columnRef('resolved_name'),
    };

    const executeQuery = jest.fn(async () => serializeResponse(selectNode));

    const result = await pruneRedundantStarsOverJoins(sql, executeQuery);

    expect(result.rewrittenStars).toBe(1);
    expect(result.sql).toBe(
      "SELECT 'café' AS c, \"resolved_name\" FROM base LEFT JOIN lookup"
    );
  });

  it('strips a following comma when a dropped star is first in the list', async () => {
    // Leading star with an empty replacement must not leave `SELECT , id`.
    const sql = 'SELECT *, base.id AS id FROM base LEFT JOIN lookup';
    const starByteOffset = 7;

    const selectNode = {
      type: 'SELECT_NODE',
      select_list: [bareStar(starByteOffset), columnRef('id', 10)],
      from_table: {
        type: 'JOIN',
        left: subqueryTable('base', [columnRef('id')]),
        right: subqueryTable('lookup', [bareStar(99)]),
      },
    };

    const executeQuery = jest.fn(async () => serializeResponse(selectNode));

    const result = await pruneRedundantStarsOverJoins(sql, executeQuery);

    expect(result.rewrittenStars).toBe(1);
    expect(result.sql).toBe('SELECT base.id AS id FROM base LEFT JOIN lookup');
  });

  it('returns the original SQL when executeQuery rejects', async () => {
    const sql = 'SELECT base.id AS id, * FROM base LEFT JOIN lookup';
    const executeQuery = jest.fn(async () => {
      throw new Error('DuckDB connection lost');
    });

    const result = await pruneRedundantStarsOverJoins(sql, executeQuery);

    expect(result.rewrittenStars).toBe(0);
    expect(result.sql).toBe(sql);
  });

  it('leaves a lone `SELECT *` over a single table untouched', async () => {
    const sql = 'SELECT * FROM base';
    const selectNode = {
      type: 'SELECT_NODE',
      select_list: [bareStar(7)],
      from_table: { type: 'BASE_TABLE', table_name: 'base' },
    };
    const executeQuery = jest.fn(async () => serializeResponse(selectNode));

    const result = await pruneRedundantStarsOverJoins(sql, executeQuery);

    expect(result.rewrittenStars).toBe(0);
    expect(result.sql).toBe(sql);
  });

  it('does not touch a star with an EXCLUDE list', async () => {
    const sql = 'SELECT * EXCLUDE (x), a FROM base LEFT JOIN lookup';
    const selectNode = {
      type: 'SELECT_NODE',
      select_list: [
        {
          class: 'STAR',
          type: 'STAR',
          alias: '',
          query_location: 7,
          relation_name: '',
          exclude_list: ['x'],
          replace_list: [],
          columns: false,
          expr: null,
        },
        columnRef('a', 25),
      ],
      from_table: {
        type: 'JOIN',
        left: subqueryTable('base', [columnRef('a')]),
        right: subqueryTable('lookup', [columnRef('b')]),
      },
    };
    const executeQuery = jest.fn(async () => serializeResponse(selectNode));

    const result = await pruneRedundantStarsOverJoins(sql, executeQuery);

    expect(result.rewrittenStars).toBe(0);
    expect(result.sql).toBe(sql);
  });

  it('returns the original SQL when DuckDB cannot parse it', async () => {
    const sql = 'not valid sql';
    const executeQuery = jest.fn(async () => [
      { json_serialize_sql: JSON.stringify({ error: true }) },
    ]);

    const result = await pruneRedundantStarsOverJoins(sql, executeQuery);

    expect(result.rewrittenStars).toBe(0);
    expect(result.sql).toBe(sql);
  });
});
