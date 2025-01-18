import { Query, TableSchema } from '@devrev/meerkat-core';
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

const CREATE_TEST_TABLE = `CREATE TABLE tickets (
	id INTEGER,
	owners VARCHAR[],
	tags VARCHAR[],
	created_by VARCHAR,
	subscribers_count INTEGER,
)`;

const INPUT_DATA_QUERY = `INSERT INTO tickets VALUES
(1, ['a', 'b'], ['t1'], 'x', 30),
(2, ['b', 'c', 'd'], ['t2', 't3'], 'x', 10),
(3, ['e'], ['t1', 't4', 't3'], 'y', 80),
(4, ['a', 'd'], ['t4'], 'z', 60),
(5, null, ['t4', 't5'], 'z', 60),
(6, ['b', 'c'], null, 'z', 60),
(6, null, null, 'z', 60)`;

const OWNERS_DIMENSION: TableSchema['dimensions'][0] = {
  name: 'owners',
  sql: 'owners',
  type: 'string_array',
};

const TAGS_DIMENSION: TableSchema['dimensions'][0] = {
  name: 'tags',
  sql: 'tags',
  type: 'string_array',
};

export const TABLE_SCHEMA: TableSchema = {
  name: 'tickets',
  sql: 'select * from tickets',
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
      type: 'string',
    },
    {
      name: 'created_by',
      sql: 'created_by',
      type: 'string',
    },
    {
      name: 'subscriber_count',
      sql: 'subscriber_count',
      type: 'string',
    },
  ],
};

describe('cube-to-sql', () => {
  beforeAll(async () => {
    //Create test table
    await duckdbExec(CREATE_TEST_TABLE);
    //Insert test data
    await duckdbExec(INPUT_DATA_QUERY);
    //Get SQL from cube query
  });
  it('Should unnest group by for  type field when modifier set', async () => {
    const query: Query = {
      measures: ['tickets.count'],
      dimensions: ['tickets.owners'],
      order: {
        'tickets.count': 'desc',
        'tickets.owners': 'desc',
      },
    };
    const TABLE_SCHEMA_WITH_UNNEST_OWNER = {
      ...TABLE_SCHEMA,
      dimensions: [
        ...TABLE_SCHEMA.dimensions,
        {
          ...OWNERS_DIMENSION,
          modifier: {
            shouldUnnestGroupBy: true,
          },
        },
        TAGS_DIMENSION,
      ],
    };
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [TABLE_SCHEMA_WITH_UNNEST_OWNER],
    });
    console.info(`SQL for Simple Cube Query: `, sql);
    expect(sql).toBe(
      'SELECT COUNT(*) AS tickets__count ,   tickets__owners FROM (SELECT *, array[unnest(owners)] AS tickets__owners FROM (select * from tickets) AS tickets) AS tickets GROUP BY tickets__owners ORDER BY tickets__count DESC, tickets__owners DESC'
    );
    const output = await duckdbExec(sql);
    expect(output).toEqual([
      {
        tickets__count: 3,
        tickets__owners: ['b'],
      },
      {
        tickets__count: 2,
        tickets__owners: ['d'],
      },
      {
        tickets__count: 2,
        tickets__owners: ['c'],
      },
      {
        tickets__count: 2,
        tickets__owners: ['a'],
      },
      {
        tickets__count: 1,
        tickets__owners: ['e'],
      },
    ]);
  });
  it('Should not unnest group by for  type field when modifier unset', async () => {
    const query: Query = {
      measures: ['tickets.count'],
      dimensions: ['tickets.owners'],
      order: {
        'tickets.count': 'desc',
        'tickets.owners': 'desc',
      },
    };
    const TABLE_SCHEMA_WITH_UNNEST_OWNER = {
      ...TABLE_SCHEMA,
      dimensions: [
        ...TABLE_SCHEMA.dimensions,
        OWNERS_DIMENSION,
        TAGS_DIMENSION,
      ],
    };
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [TABLE_SCHEMA_WITH_UNNEST_OWNER],
    });
    console.info(`SQL for Simple Cube Query: `, sql);
    expect(sql).toBe(
      'SELECT COUNT(*) AS tickets__count ,   tickets__owners FROM (SELECT *, owners AS tickets__owners FROM (select * from tickets) AS tickets) AS tickets GROUP BY tickets__owners ORDER BY tickets__count DESC, tickets__owners DESC'
    );
    const output = await duckdbExec(sql);
    expect(output).toEqual([
      {
        tickets__count: 2,
        tickets__owners: null,
      },
      {
        tickets__count: 1,
        tickets__owners: ['e'],
      },
      {
        tickets__count: 1,
        tickets__owners: ['b', 'c', 'd'],
      },
      {
        tickets__count: 1,
        tickets__owners: ['b', 'c'],
      },
      {
        tickets__count: 1,
        tickets__owners: ['a', 'd'],
      },
      {
        tickets__count: 1,
        tickets__owners: ['a', 'b'],
      },
    ]);
  });
  it('Should not unnest group by string field when config unset', async () => {
    const query: Query = {
      measures: ['tickets.count'],
      dimensions: ['tickets.created_by'],
      order: {
        'tickets.count': 'desc',
        'tickets.created_by': 'desc',
      },
    };
    const TABLE_SCHEMA_WITH_UNNEST_OWNER = {
      ...TABLE_SCHEMA,
      dimensions: [
        ...TABLE_SCHEMA.dimensions,
        OWNERS_DIMENSION,
        TAGS_DIMENSION,
      ],
    };
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [TABLE_SCHEMA_WITH_UNNEST_OWNER],
    });
    console.info(`SQL for Simple Cube Query: `, sql);
    expect(sql).toBe(
      'SELECT COUNT(*) AS tickets__count ,   tickets__created_by FROM (SELECT *, created_by AS tickets__created_by FROM (select * from tickets) AS tickets) AS tickets GROUP BY tickets__created_by ORDER BY tickets__count DESC, tickets__created_by DESC'
    );
    const output = await duckdbExec(sql);
    expect(output).toEqual([
      {
        tickets__count: 4,
        tickets__created_by: 'z',
      },
      {
        tickets__count: 2,
        tickets__created_by: 'x',
      },
      {
        tickets__count: 1,
        tickets__created_by: 'y',
      },
    ]);
  });
  it('Should not unnest group by string field when config set', async () => {
    const query: Query = {
      measures: ['tickets.count'],
      dimensions: ['tickets.created_by'],
      order: {
        'tickets.count': 'desc',
        'tickets.created_by': 'desc',
      },
    };
    const TABLE_SCHEMA_WITH_UNNEST_OWNER = {
      ...TABLE_SCHEMA,
      dimensions: [
        ...TABLE_SCHEMA.dimensions,
        { ...OWNERS_DIMENSION, modifier: { shouldUnnestGroupBy: true } },
        TAGS_DIMENSION,
      ],
    };
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [TABLE_SCHEMA_WITH_UNNEST_OWNER],
    });
    console.info(`SQL for Simple Cube Query: `, sql);
    expect(sql).toBe(
      'SELECT COUNT(*) AS tickets__count ,   tickets__created_by FROM (SELECT *, created_by AS tickets__created_by FROM (select * from tickets) AS tickets) AS tickets GROUP BY tickets__created_by ORDER BY tickets__count DESC, tickets__created_by DESC'
    );
    const output = await duckdbExec(sql);
    expect(output).toEqual([
      {
        tickets__count: 4,
        tickets__created_by: 'z',
      },
      {
        tickets__count: 2,
        tickets__created_by: 'x',
      },
      {
        tickets__count: 1,
        tickets__created_by: 'y',
      },
    ]);
  });
  it('Should unnest multiple columns', async () => {
    const query: Query = {
      measures: ['tickets.count'],
      dimensions: ['tickets.created_by', 'tickets.owners', 'tickets.tags'],
      order: {
        'tickets.count': 'desc',
        'tickets.created_by': 'desc',
        'tickets.tags': 'desc',
        'tickets.owners': 'desc',
      },
    };
    const TABLE_SCHEMA_WITH_UNNEST_OWNER = {
      ...TABLE_SCHEMA,
      dimensions: [
        ...TABLE_SCHEMA.dimensions,
        {
          ...OWNERS_DIMENSION,
          modifier: { shouldUnnestGroupBy: true },
        },
        {
          ...TAGS_DIMENSION,
          modifier: { shouldUnnestGroupBy: true },
        },
      ],
    };
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [TABLE_SCHEMA_WITH_UNNEST_OWNER],
    });
    console.info(`SQL for Simple Cube Query: `, sql);
    expect(sql).toBe(
      'SELECT COUNT(*) AS tickets__count ,   tickets__created_by,  tickets__owners,  tickets__tags FROM (SELECT *, created_by AS tickets__created_by, array[unnest(owners)] AS tickets__owners, array[unnest(tags)] AS tickets__tags FROM (select * from tickets) AS tickets) AS tickets GROUP BY tickets__created_by, tickets__owners, tickets__tags ORDER BY tickets__count DESC, tickets__created_by DESC, tickets__tags DESC, tickets__owners DESC'
    );
    const output = await duckdbExec(sql);
    expect(output).toEqual([
      {
        tickets__count: 1,
        tickets__created_by: 'z',
        tickets__owners: ['d'],
        tickets__tags: [null],
      },
      {
        tickets__count: 1,
        tickets__created_by: 'z',
        tickets__owners: ['c'],
        tickets__tags: [null],
      },
      {
        tickets__count: 1,
        tickets__created_by: 'z',
        tickets__owners: ['b'],
        tickets__tags: [null],
      },
      {
        tickets__count: 1,
        tickets__created_by: 'z',
        tickets__owners: [null],
        tickets__tags: ['t5'],
      },
      {
        tickets__count: 1,
        tickets__created_by: 'z',
        tickets__owners: [null],
        tickets__tags: ['t4'],
      },
      {
        tickets__count: 1,
        tickets__created_by: 'z',
        tickets__owners: ['a'],
        tickets__tags: ['t4'],
      },
      {
        tickets__count: 1,
        tickets__created_by: 'y',
        tickets__owners: [null],
        tickets__tags: ['t4'],
      },
      {
        tickets__count: 1,
        tickets__created_by: 'y',
        tickets__owners: [null],
        tickets__tags: ['t3'],
      },
      {
        tickets__count: 1,
        tickets__created_by: 'y',
        tickets__owners: ['e'],
        tickets__tags: ['t1'],
      },
      {
        tickets__count: 1,
        tickets__created_by: 'x',
        tickets__owners: ['d'],
        tickets__tags: [null],
      },
      {
        tickets__count: 1,
        tickets__created_by: 'x',
        tickets__owners: ['b'],
        tickets__tags: [null],
      },
      {
        tickets__count: 1,
        tickets__created_by: 'x',
        tickets__owners: ['c'],
        tickets__tags: ['t3'],
      },
      {
        tickets__count: 1,
        tickets__created_by: 'x',
        tickets__owners: ['b'],
        tickets__tags: ['t2'],
      },
      {
        tickets__count: 1,
        tickets__created_by: 'x',
        tickets__owners: ['a'],
        tickets__tags: ['t1'],
      },
    ]);
  });

  it('Should not unnest for filter projections', async () => {
    const query: Query = {
      measures: ['tickets.count'],
      dimensions: ['tickets.tags'],
      order: {
        'tickets.count': 'desc',
        'tickets.tags': 'desc',
      },
      filters: [
        {
          and: [
            {
              member: 'tickets.owners',
              operator: 'equals',
              values: ['a'],
            },
          ],
        },
      ],
    };
    const TABLE_SCHEMA_WITH_UNNEST_OWNER = {
      ...TABLE_SCHEMA,
      dimensions: [
        ...TABLE_SCHEMA.dimensions,
        {
          ...OWNERS_DIMENSION,
          modifier: { shouldUnnestGroupBy: true },
        },
        {
          ...TAGS_DIMENSION,
          modifier: { shouldUnnestGroupBy: true },
        },
      ],
    };
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [TABLE_SCHEMA_WITH_UNNEST_OWNER],
    });
    console.info(`SQL for Simple Cube Query: `, sql);
    expect(sql).toBe(
      "SELECT COUNT(*) AS tickets__count ,   tickets__tags FROM (SELECT *, owners AS tickets__owners, array[unnest(tags)] AS tickets__tags FROM (select * from tickets) AS tickets) AS tickets WHERE (('a' = ANY(SELECT unnest(tickets__owners)))) GROUP BY tickets__tags ORDER BY tickets__count DESC, tickets__tags DESC"
    );
    const output = await duckdbExec(sql);
    expect(output).toEqual([
      {
        tickets__count: 1,
        tickets__tags: ['t4'],
      },
      {
        tickets__count: 1,
        tickets__tags: ['t1'],
      },
    ]);
  });
  it('Should not unnest without measure', async () => {
    const query: Query = {
      measures: [],
      dimensions: ['tickets.owners'],
      order: {
        'tickets.owners': 'desc',
      },
    };
    const TABLE_SCHEMA_WITH_UNNEST_OWNER = {
      ...TABLE_SCHEMA,
      dimensions: [
        ...TABLE_SCHEMA.dimensions,
        {
          ...OWNERS_DIMENSION,
          modifier: {
            shouldUnnestGroupBy: true,
          },
        },
        TAGS_DIMENSION,
      ],
    };
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [TABLE_SCHEMA_WITH_UNNEST_OWNER],
    });
    console.info(`SQL for Simple Cube Query: `, sql);
    expect(sql).toBe(
      'SELECT  tickets__owners FROM (SELECT *, owners AS tickets__owners FROM (select * from tickets) AS tickets) AS tickets ORDER BY tickets__owners DESC'
    );
    const output = await duckdbExec(sql);
    expect(output).toEqual([
      {
        tickets__owners: ['e'],
      },
      {
        tickets__owners: ['b', 'c', 'd'],
      },
      {
        tickets__owners: ['b', 'c'],
      },
      {
        tickets__owners: ['a', 'd'],
      },
      {
        tickets__owners: ['a', 'b'],
      },
      {
        tickets__owners: null,
      },
      {
        tickets__owners: null,
      },
    ]);
  });
});
