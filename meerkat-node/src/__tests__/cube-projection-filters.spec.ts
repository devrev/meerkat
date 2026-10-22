import type { Query, TableSchema } from '@devrev/meerkat-core';
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

const SCHEMA: TableSchema = {
  dimensions: [
    {
      name: 'other_dimension',
      sql: "'dashboard_others'",
      type: 'string',
    },
    {
      name: 'primary_part_id',
      sql: 'primary_part_id',
      type: 'string',
    },
    {
      name: 'id',
      sql: 'id',
      type: 'string',
    },
    {
      name: 'ticket_prioritized',
      sql: "CASE WHEN primary_part_id LIKE '%enhancement%' THEN 'yes' ELSE 'no' END",
      type: 'string',
    },
  ],
  measures: [
    {
      name: 'count_star',
      sql: 'COUNT(DISTINCT id)',
      type: 'number',
    },
  ],
  name: 'person',
  sql: 'SELECT * FROM person',
};

const QUERY: Query = {
  dimensions: ['person.other_dimension'],
  measures: ['person.count_star'],
  filters: [
    {
      and: [
        {
          member: 'person.ticket_prioritized',
          operator: 'notEquals',
          values: ['no'],
        },
      ],
    },
  ],
};

describe('cube-to-sql', () => {
  beforeAll(async () => {
    //Create test table
    await duckdbExec(`
    CREATE TABLE PERSON (
        id VARCHAR,
        primary_part_id VARCHAR,
    );
    `);
    //Insert test data
    await duckdbExec(`
    INSERT INTO person (id, primary_part_id) 
    VALUES 
    ('1', 'enhancement1'),
    ('2', 'product1'),
    ('3', 'feature'),
    ('4', 'enhancement2');
`);
  });

  it('Should construct the SQL query and apply filter on projections', async () => {
    const sql = await cubeQueryToSQL({ query: QUERY, tableSchemas: [SCHEMA] });
    const expectedSQL = `SELECT COUNT(DISTINCT id) AS person__count_star ,   person__other_dimension FROM (SELECT CASE WHEN primary_part_id LIKE '%enhancement%' THEN 'yes' ELSE 'no' END AS person__ticket_prioritized, 'dashboard_others' AS person__other_dimension, * FROM (SELECT * FROM person) AS person) AS person WHERE ((person__ticket_prioritized != 'no')) GROUP BY person__other_dimension`;
    expect(sql).toBe(expectedSQL);
    const output = await duckdbExec(sql);
    const expectQueryResult = [
      {
        person__count_star: BigInt(2),
        person__other_dimension: 'dashboard_others',
      },
    ];
    expect(output).toEqual(expectQueryResult);
  });

  it('Should be able to handle multi level filters', async () => {
    const sql = await cubeQueryToSQL({
      query: {
        ...QUERY,
        filters: [
          {
            and: [
              {
                or: [
                  {
                    member: 'person.id',
                    operator: 'notEquals',
                    values: ['1'],
                  },
                  {
                    member: 'person.ticket_prioritized',
                    operator: 'notEquals',
                    values: ['no'],
                  },
                ],
              },
            ],
          },
        ],
      },
      tableSchemas: [SCHEMA],
    });
    const expectedSQL = `SELECT COUNT(DISTINCT id) AS person__count_star ,   person__other_dimension FROM (SELECT id AS person__id, CASE WHEN primary_part_id LIKE '%enhancement%' THEN 'yes' ELSE 'no' END AS person__ticket_prioritized, 'dashboard_others' AS person__other_dimension, * FROM (SELECT * FROM person) AS person) AS person WHERE (((person__id != '1') OR (person__ticket_prioritized != 'no'))) GROUP BY person__other_dimension`;
    expect(sql).toBe(expectedSQL);
    const output = await duckdbExec(sql);
    const expectQueryResult = [
      {
        person__count_star: BigInt(4),
        person__other_dimension: 'dashboard_others',
      },
    ];
    expect(output).toEqual(expectQueryResult);
  });

  it('Should be able to handle same projection on multiple levels in filter', async () => {
    const sql = await cubeQueryToSQL({
      query: {
        ...QUERY,
        filters: [
          {
            and: [
              {
                member: 'person.id',
                operator: 'notEquals',
                values: ['2'],
              },
              {
                or: [
                  {
                    member: 'person.id',
                    operator: 'notEquals',
                    values: ['1'],
                  },
                  {
                    member: 'person.ticket_prioritized',
                    operator: 'notEquals',
                    values: ['no'],
                  },
                ],
              },
            ],
          },
        ],
      },
      tableSchemas: [SCHEMA],
    });
    const expectedSQL = `SELECT COUNT(DISTINCT id) AS person__count_star ,   person__other_dimension FROM (SELECT id AS person__id, CASE WHEN primary_part_id LIKE '%enhancement%' THEN 'yes' ELSE 'no' END AS person__ticket_prioritized, 'dashboard_others' AS person__other_dimension, * FROM (SELECT * FROM person) AS person) AS person WHERE ((person__id != '2') AND ((person__id != '1') OR (person__ticket_prioritized != 'no'))) GROUP BY person__other_dimension`;
    expect(sql).toBe(expectedSQL);
    const output = await duckdbExec(sql);
    const expectQueryResult = [
      {
        person__count_star: BigInt(3),
        person__other_dimension: 'dashboard_others',
      },
    ];
    expect(output).toEqual(expectQueryResult);
  });
});

describe('literal substring filters', () => {
  const table: TableSchema = {
    name: 'substring_records',
    sql: 'SELECT * FROM substring_records',
    dimensions: [
      { name: 'id', sql: 'id', type: 'number' },
      { name: 'label', sql: 'label', type: 'string' },
    ],
    measures: [],
  };

  beforeAll(async () => {
    await duckdbExec(`CREATE TABLE substring_records(id INTEGER, label VARCHAR);
      INSERT INTO substring_records VALUES
      (1, 'pre A_B post'), (2, 'pre axb post'), (3, '50% complete'),
      (4, '500 complete'), (5, 'C:\\files'), (6, 'C:files'),
      (7, 'O''Reilly'), (8, 'MixedCase'), (9, NULL), (10, 'pre A_B post')`);
  });

  it.each<{
    operator: 'contains' | 'notContains';
    values: string[];
    ids: number[];
  }>([
    { operator: 'contains', values: ['a_b'], ids: [1, 10] },
    { operator: 'contains', values: ['50%'], ids: [3] },
    { operator: 'contains', values: ['C:\\'], ids: [5] },
    { operator: 'contains', values: ["O'Reilly"], ids: [7] },
    { operator: 'contains', values: ['MIXEDCASE'], ids: [8] },
    { operator: 'contains', values: ['a_b', '50%'], ids: [1, 3, 10] },
    { operator: 'notContains', values: ['a_b'], ids: [2, 3, 4, 5, 6, 7, 8] },
    {
      operator: 'notContains',
      values: ['MIXEDCASE'],
      ids: [1, 2, 3, 4, 5, 6, 7, 10],
    },
    {
      operator: 'notContains',
      values: ['a_b', '50%'],
      ids: [2, 4, 5, 6, 7, 8],
    },
  ])(
    'executes $operator $values with literal values and SQL NULLs',
    async ({ operator, values, ids }) => {
      const sql = await cubeQueryToSQL({
        tableSchemas: [table],
        query: {
          dimensions: ['substring_records.id'],
          measures: [],
          filters: [{ member: 'substring_records.label', operator, values }],
          order: { 'substring_records.id': 'asc' },
        },
      });
      expect(await duckdbExec(sql)).toEqual(
        ids.map((id) => ({ substring_records__id: id }))
      );
    }
  );
});
