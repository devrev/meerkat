import {
  astDeserializerQuery,
  buildPreBaseQuerySync,
  canBuildPreBaseQuerySync,
  cubeToDuckdbAST,
  deserializeQuery,
  getCombinedTableSchema,
} from '@devrev/meerkat-core';
import { duckdbExec } from '../duckdb-exec';
import { buildDimIssueFixture } from './helpers/dim-issue-fixture';

/**
 * POC differential test: for every gated query shape, the AST-free
 * `buildPreBaseQuerySync` must produce a string byte-identical to the AST
 * round-trip (`cubeToDuckdbAST -> json_deserialize_sql`).
 */

const SCHEMA: any = {
  name: 't',
  sql: 'SELECT * FROM t',
  measures: [{ name: 'cnt', sql: 'COUNT(*)', type: 'number' }],
  dimensions: [
    { name: 'a', sql: 'a', type: 'string' },
    { name: 'b', sql: 'b', type: 'number' },
    { name: 'ts', sql: 'ts', type: 'time' },
    { name: 'row_order', sql: 'row_order', type: 'number' },
    { name: 'weird name', sql: 'weird', type: 'string', alias: 'Weird Name' },
  ],
};

const roundTrip = async (query: any, schema: any = SCHEMA): Promise<string> => {
  const combined = getCombinedTableSchema([schema], query);
  const ast = cubeToDuckdbAST(query, combined, { filterType: 'PROJECTION_FILTER' });
  const rows = (await duckdbExec(astDeserializerQuery(ast))) as any;
  return deserializeQuery(rows);
};

const GATED_CASES: Record<string, any> = {
  'order-only-no-measure': {
    dimensions: ['t.a', 't.b'],
    measures: [],
    filters: [{ and: [] }],
    order: { 't.row_order': 'asc' },
  },
  'measure+dims group by + order': {
    dimensions: ['t.a', 't.b'],
    measures: ['t.cnt'],
    filters: [{ and: [] }],
    order: { 't.a': 'desc' },
  },
  'limit-offset only': {
    dimensions: ['t.a'],
    measures: [],
    filters: [{ and: [] }],
    limit: 10,
    offset: 20,
  },
  'limit only': {
    dimensions: ['t.a'],
    measures: [],
    filters: [{ and: [] }],
    limit: 5,
  },
  'no order no filter': {
    dimensions: ['t.a'],
    measures: [],
    filters: [{ and: [] }],
  },
  'no filters key at all': {
    dimensions: ['t.a'],
    measures: ['t.cnt'],
    order: { 't.a': 'asc' },
  },
  'custom alias order': {
    dimensions: ['t.a'],
    measures: [],
    filters: [{ and: [] }],
    order: { 't.weird name': 'desc' },
  },
  'multi order keys': {
    dimensions: ['t.a', 't.b'],
    measures: ['t.cnt'],
    filters: [{ and: [] }],
    order: { 't.a': 'asc', 't.b': 'desc' },
  },
  'nested empty groups': {
    dimensions: ['t.a'],
    measures: [],
    filters: [{ and: [{ and: [] }, { or: [] }] }],
    order: { 't.a': 'asc' },
  },
};

describe('preBaseQuery AST-free POC — differential parity', () => {
  it('all gated cases match the round-trip byte-for-byte', async () => {
    for (const [label, query] of Object.entries(GATED_CASES)) {
      expect(canBuildPreBaseQuerySync(query)).toBe(true);
      const combined = getCombinedTableSchema([SCHEMA], query);
      const expected = await roundTrip(query);
      const actual = buildPreBaseQuerySync(query, combined);
      expect(`${label}: ${actual}`).toBe(`${label}: ${expected}`);
    }
  });

  it('gates OFF when a real filter is present', () => {
    const query: any = {
      dimensions: ['t.a'],
      measures: ['t.cnt'],
      filters: [{ and: [{ member: 't.b', operator: 'gt', values: ['5'] }] }],
    };
    expect(canBuildPreBaseQuerySync(query)).toBe(false);
  });

  it('matches for the dim_issue production shape', async () => {
    const { schema, query } = buildDimIssueFixture();
    expect(canBuildPreBaseQuerySync(query)).toBe(true);
    const combined = getCombinedTableSchema([schema], query);
    const expected = await roundTrip(query, schema);
    const actual = buildPreBaseQuerySync(query, combined);
    expect(actual).toBe(expected);
  });
});
