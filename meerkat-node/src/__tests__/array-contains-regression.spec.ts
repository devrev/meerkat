import { describe, expect, it } from 'vitest';
import type { Query, TableSchema } from '@devrev/meerkat-core';
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

interface FilterCase {
  member: string;
  operator: 'contains' | 'notContains';
  values: (string | number)[];
  ids: number[];
}

const table: TableSchema = {
  name: 'samples',
  sql: `SELECT * FROM (VALUES
    (1, ['red', 'blue'], [1, 2], 'Alpha'),
    (1, ['red', 'blue'], [1, 2], 'Alpha'),
    (2, ['redder'], [12], 'beta'),
    (3, []::VARCHAR[], []::DOUBLE[], 'ALPHABET'),
    (4, NULL::VARCHAR[], NULL::DOUBLE[], NULL),
    (5, [NULL]::VARCHAR[], [NULL]::DOUBLE[], 'gamma'),
    (6, ['blue', 'GREEN'], [2, 3], 'delta')
  ) source(id, tags, numbers, label)`,
  dimensions: [
    { name: 'id', sql: 'id', type: 'number' },
    { name: 'tags', sql: 'tags', type: 'string_array' },
    { name: 'numbers', sql: 'numbers', type: 'number_array' },
    { name: 'label', sql: 'label', type: 'string' },
  ],
  measures: [],
};

describe('typed contains filter execution', () => {
  it.each<FilterCase>([
    { member: 'tags', operator: 'contains', values: ['red'], ids: [1, 1] },
    {
      member: 'tags',
      operator: 'contains',
      values: ['red', 'GREEN'],
      ids: [1, 1, 6],
    },
    {
      member: 'tags',
      operator: 'notContains',
      values: ['red'],
      ids: [2, 3, 5, 6],
    },
    {
      member: 'tags',
      operator: 'notContains',
      values: ['red', 'GREEN'],
      ids: [2, 3, 5],
    },
    { member: 'numbers', operator: 'contains', values: [1, 3], ids: [1, 1, 6] },
    {
      member: 'numbers',
      operator: 'notContains',
      values: [1, 3],
      ids: [2, 3, 5],
    },
    {
      member: 'label',
      operator: 'contains',
      values: ['ALPHA'],
      ids: [1, 1, 3],
    },
    {
      member: 'label',
      operator: 'notContains',
      values: ['ALPHA'],
      ids: [2, 5, 6],
    },
  ])(
    '$member $operator $values retains exact matching rows',
    async ({ member, operator, values, ids }) => {
      const query: Query = {
        dimensions: ['samples.id'],
        measures: [],
        timeDimensions: [],
        type: 'sql',
        filters: [{ member: `samples.${member}`, operator, values }],
      };
      const sql = await cubeQueryToSQL({ query, tableSchemas: [table] });
      const rows = await duckdbExec<{ samples__id: number | bigint }[]>(sql);
      expect(
        rows.map((row) => Number(row.samples__id)).sort((a, b) => a - b)
      ).toEqual(ids);
    }
  );
});
