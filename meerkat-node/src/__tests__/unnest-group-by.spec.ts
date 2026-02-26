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
        'SELECT COUNT(*) AS tickets__count ,   tickets__owners FROM (SELECT NULLIF(array[unnest(CASE WHEN owners IS NULL OR len(COALESCE(owners, [])) = 0 THEN [NULL] ELSE owners END)], [NULL]) AS tickets__owners, * FROM (select * from tickets) AS tickets) AS tickets GROUP BY tickets__owners ORDER BY tickets__count DESC, tickets__owners DESC'
      );
      const output = await duckdbExec(sql);
      expect(output).toEqual([
        {
          tickets__count: BigInt(3),
          tickets__owners: ['b'],
        },
        {
          tickets__count: BigInt(2),
          tickets__owners: ['d'],
        },
        {
          tickets__count: BigInt(2),
          tickets__owners: ['c'],
        },
        {
          tickets__count: BigInt(2),
          tickets__owners: ['a'],
        },
        {
          tickets__count: BigInt(2),
          tickets__owners: null,
        },
        {
          tickets__count: BigInt(1),
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
        'SELECT COUNT(*) AS tickets__count ,   tickets__owners FROM (SELECT owners AS tickets__owners, * FROM (select * from tickets) AS tickets) AS tickets GROUP BY tickets__owners ORDER BY tickets__count DESC, tickets__owners DESC'
      );
      const output = await duckdbExec(sql);
      expect(output).toEqual([
        {
          tickets__count: BigInt(2),
          tickets__owners: null,
        },
        {
          tickets__count: BigInt(1),
          tickets__owners: ['e'],
        },
        {
          tickets__count: BigInt(1),
          tickets__owners: ['b', 'c', 'd'],
        },
        {
          tickets__count: BigInt(1),
          tickets__owners: ['b', 'c'],
        },
        {
          tickets__count: BigInt(1),
          tickets__owners: ['a', 'd'],
        },
        {
          tickets__count: BigInt(1),
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
        'SELECT COUNT(*) AS tickets__count ,   tickets__created_by FROM (SELECT created_by AS tickets__created_by, * FROM (select * from tickets) AS tickets) AS tickets GROUP BY tickets__created_by ORDER BY tickets__count DESC, tickets__created_by DESC'
      );
      const output = await duckdbExec(sql);
      expect(output).toEqual([
        {
          tickets__count: BigInt(4),
          tickets__created_by: 'z',
        },
        {
          tickets__count: BigInt(2),
          tickets__created_by: 'x',
        },
        {
          tickets__count: BigInt(1),
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
        'SELECT COUNT(*) AS tickets__count ,   tickets__created_by FROM (SELECT created_by AS tickets__created_by, * FROM (select * from tickets) AS tickets) AS tickets GROUP BY tickets__created_by ORDER BY tickets__count DESC, tickets__created_by DESC'
      );
      const output = await duckdbExec(sql);
      expect(output).toEqual([
        {
          tickets__count: BigInt(4),
          tickets__created_by: 'z',
        },
        {
          tickets__count: BigInt(2),
          tickets__created_by: 'x',
        },
        {
          tickets__count: BigInt(1),
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
        'SELECT COUNT(*) AS tickets__count ,   tickets__created_by,  tickets__owners,  tickets__tags FROM (SELECT created_by AS tickets__created_by, NULLIF(array[unnest(CASE WHEN owners IS NULL OR len(COALESCE(owners, [])) = 0 THEN [NULL] ELSE owners END)], [NULL]) AS tickets__owners, NULLIF(array[unnest(CASE WHEN tags IS NULL OR len(COALESCE(tags, [])) = 0 THEN [NULL] ELSE tags END)], [NULL]) AS tickets__tags, * FROM (select * from tickets) AS tickets) AS tickets GROUP BY tickets__created_by, tickets__owners, tickets__tags ORDER BY tickets__count DESC, tickets__created_by DESC, tickets__tags DESC, tickets__owners DESC'
      );
      const output = await duckdbExec(sql);
      
      // Verify the query executes and returns results
      expect(output.length).toBeGreaterThan(0);
      
      // Verify that NULL arrays are preserved as null (rows with null owners and/or tags should exist)
      const rowsWithNullOwners = output.filter(
        (row: { tickets__owners: string[] | null }) => row.tickets__owners === null
      );
      const rowsWithNullTags = output.filter(
        (row: { tickets__tags: string[] | null }) => row.tickets__tags === null
      );
      
      // Should have rows with null owners (from records 5, 7 that had null owners)
      expect(rowsWithNullOwners.length).toBeGreaterThan(0);
      // Should have rows with null tags (from records 6, 7 that had null tags)
      expect(rowsWithNullTags.length).toBeGreaterThan(0);
      
      // Verify each row has the expected structure
      output.forEach((row: { tickets__count: bigint; tickets__created_by: string; tickets__owners: string[] | null; tickets__tags: string[] | null }) => {
        expect(row).toHaveProperty('tickets__count');
        expect(row).toHaveProperty('tickets__created_by');
        expect(row).toHaveProperty('tickets__owners');
        expect(row).toHaveProperty('tickets__tags');
        // owners and tags can be either an array or null
        expect(row.tickets__owners === null || Array.isArray(row.tickets__owners)).toBe(true);
        expect(row.tickets__tags === null || Array.isArray(row.tickets__tags)).toBe(true);
      });
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
        "SELECT COUNT(*) AS tickets__count ,   tickets__tags FROM (SELECT owners AS tickets__owners, NULLIF(array[unnest(CASE WHEN tags IS NULL OR len(COALESCE(tags, [])) = 0 THEN [NULL] ELSE tags END)], [NULL]) AS tickets__tags, * FROM (select * from tickets) AS tickets) AS tickets WHERE (list_has_all(tickets__owners, main.list_value('a'))) GROUP BY tickets__tags ORDER BY tickets__count DESC, tickets__tags DESC"
      );
      const output = await duckdbExec(sql);
      expect(output).toEqual([
        {
          tickets__count: BigInt(1),
          tickets__tags: ['t4'],
        },
        {
          tickets__count: BigInt(1),
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
        'SELECT  tickets__owners FROM (SELECT owners AS tickets__owners, * FROM (select * from tickets) AS tickets) AS tickets ORDER BY tickets__owners DESC'
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

    it('Should preserve NULL array values when unnesting with shouldUnnestGroupBy', async () => {
      const query: Query = {
        measures: ['tickets.count'],
        dimensions: ['tickets.owners'],
        order: {
          'tickets.owners': 'asc',
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
      console.info(`SQL for NULL array preservation: `, sql);
      const output = await duckdbExec(sql);
      
      // Verify that NULL arrays are preserved as null instead of being dropped
      const nullOwnerRows = output.filter(
        (row: { tickets__owners: string[] | null }) => row.tickets__owners === null
      );
      
      // Should have rows with null for the 2 records that have NULL owners (ids 5 and 7)
      expect(nullOwnerRows.length).toBe(1);
      expect(nullOwnerRows[0].tickets__count).toBe(BigInt(2));
      expect(nullOwnerRows[0].tickets__owners).toBeNull();
    });

    it('Should handle empty arrays by converting to NULL when unnesting', async () => {
      // Create a table with empty arrays to test the len() = 0 condition
      await duckdbExec(`CREATE TABLE IF NOT EXISTS tickets_empty_test (
        id INTEGER,
        tags VARCHAR[]
      )`);
      await duckdbExec(`INSERT INTO tickets_empty_test VALUES
        (1, ['a', 'b']),
        (2, []),
        (3, NULL),
        (4, ['c'])
      `);

      const EMPTY_TEST_SCHEMA: TableSchema = {
        name: 'tickets_empty_test',
        sql: 'select * from tickets_empty_test',
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
            name: 'tags',
            sql: 'tags',
            type: 'string_array',
            modifier: {
              shouldUnnestGroupBy: true,
            },
          },
        ],
      };

      const query: Query = {
        measures: ['tickets_empty_test.count'],
        dimensions: ['tickets_empty_test.tags'],
        order: {
          'tickets_empty_test.tags': 'asc',
        },
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [EMPTY_TEST_SCHEMA],
      });
      console.info(`SQL for empty array handling: `, sql);

      // Verify the SQL contains the NULLIF and CASE WHEN logic for NULL and empty array handling
      expect(sql).toContain('NULLIF(array[unnest(CASE WHEN tags IS NULL OR len(COALESCE(tags, [])) = 0 THEN [NULL] ELSE tags END)], [NULL])');

      const output = await duckdbExec(sql);

      // Both empty array (id=2) and NULL array (id=3) should be grouped together as null
      const nullTagRows = output.filter(
        (row: { tickets_empty_test__tags: string[] | null }) => 
          row.tickets_empty_test__tags === null
      );
      
      // Should have 2 rows combined (one from empty array, one from NULL)
      expect(nullTagRows.length).toBe(1);
      expect(nullTagRows[0].tickets_empty_test__count).toBe(BigInt(2));
      expect(nullTagRows[0].tickets_empty_test__tags).toBeNull();

      // Cleanup
      await duckdbExec('DROP TABLE tickets_empty_test');
    });
});
