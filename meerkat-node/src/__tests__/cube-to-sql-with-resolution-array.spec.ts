import { Query, ResolutionConfig, TableSchema } from '@devrev/meerkat-core';
import { cubeQueryToSQLWithResolutionWithArray } from '../cube-to-sql-with-resolution/cube-to-sql-with-resolution';
import { duckdbExec } from '../duckdb-exec';

const CREATE_TEST_TABLE = `CREATE TABLE tickets (
  id INTEGER,
  owners VARCHAR[],
  tags VARCHAR[],
  created_by VARCHAR,
  subscribers_count INTEGER
)`;

const INPUT_DATA_QUERY = `INSERT INTO tickets VALUES
(1, ['owner1', 'owner2'], ['tag1'], 'user1', 30),
(2, ['owner2', 'owner3'], ['tag2', 'tag3'], 'user2', 10),
(3, ['owner4'], ['tag1', 'tag4', 'tag3'], 'user3', 80)`;

const CREATE_RESOLUTION_TABLE = `CREATE TABLE owners_lookup (
  id VARCHAR,
  display_name VARCHAR,
  email VARCHAR
)`;

const RESOLUTION_DATA_QUERY = `INSERT INTO owners_lookup VALUES
('owner1', 'Alice Smith', 'alice@example.com'),
('owner2', 'Bob Jones', 'bob@example.com'),
('owner3', 'Charlie Brown', 'charlie@example.com'),
('owner4', 'Diana Prince', 'diana@example.com')`;

const TICKETS_TABLE_SCHEMA: TableSchema = {
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
      type: 'number',
    },
    {
      name: 'created_by',
      sql: 'created_by',
      type: 'string',
    },
    {
      name: 'owners',
      sql: 'owners',
      type: 'string_array',
    },
    {
      name: 'tags',
      sql: 'tags',
      type: 'string_array',
    },
  ],
};

const OWNERS_LOOKUP_SCHEMA: TableSchema = {
  name: 'owners_lookup',
  sql: 'select * from owners_lookup',
  measures: [],
  dimensions: [
    {
      name: 'id',
      sql: 'id',
      type: 'string',
    },
    {
      name: 'display_name',
      sql: 'display_name',
      type: 'string',
    },
    {
      name: 'email',
      sql: 'email',
      type: 'string',
    },
  ],
};

describe('cubeQueryToSQLWithResolutionWithArray - Phase 1: Unnest', () => {
  jest.setTimeout(1000000);
  beforeAll(async () => {
    // Create test tables
    await duckdbExec(CREATE_TEST_TABLE);
    await duckdbExec(INPUT_DATA_QUERY);
    await duckdbExec(CREATE_RESOLUTION_TABLE);
    await duckdbExec(RESOLUTION_DATA_QUERY);
  });

  it('Should add row_id and unnest array fields that need resolution', async () => {
    const query: Query = {
      measures: ['tickets.count'],
      dimensions: ['tickets.id', 'tickets.owners'],
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [
        {
          name: 'tickets.owners',
          isArrayType: true,
          source: 'owners_lookup',
          joinColumn: 'id',
          resolutionColumns: ['display_name', 'email'],
        },
      ],
      tableSchemas: [OWNERS_LOOKUP_SCHEMA],
    };

    const sql = await cubeQueryToSQLWithResolutionWithArray({
      query,
      tableSchemas: [TICKETS_TABLE_SCHEMA],
      resolutionConfig,
    });

    console.log('Phase 1 SQL (with row_id and unnest):', sql);

    // Verify the SQL includes row_id
    expect(sql).toContain('row_id');

    // Verify the SQL unnests the owners array
    expect(sql).toContain('unnest');

    // Verify it includes the owners dimension
    expect(sql).toContain('owners');

    // Execute the SQL to verify it works
    const result = (await duckdbExec(sql)) as any[];
    // console.log('Phase 1 Result:', JSON.stringify(result, null, 2));

    // The result should have unnested rows (more rows than the original 3)
    // Original: 3 rows, but ticket 1 has 2 owners, ticket 2 has 2 owners, ticket 3 has 1 owner
    // Expected: 5 unnested rows
    expect(result.length).toBe(5);

    // Each row should have a row_id
    expect(result[0]).toHaveProperty('__base_query_with_row_id__row_id');
    expect(result[0]).toHaveProperty(
      '__base_query_with_row_id__tickets__count'
    );
    expect(result[0]).toHaveProperty('__base_query_with_row_id__tickets__id');
    expect(result[0]).toHaveProperty(
      '__base_query_with_row_id__tickets__owners'
    );

    debugger;
    // Verify row_ids are preserved (rows with same original row should have same row_id)
    const rowIds = result.map((r) => r.row_id);
    expect(rowIds.length).toBe(5); // 5 unnested rows total
  });

  it('Should handle multiple array fields that need unnesting', async () => {
    const query: Query = {
      measures: ['tickets.count'],
      dimensions: ['tickets.id', 'tickets.owners', 'tickets.tags'],
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [
        {
          name: 'tickets.owners',
          isArrayType: true,
          source: 'owners_lookup',
          joinColumn: 'id',
          resolutionColumns: ['display_name'],
        },
        {
          name: 'tickets.tags',
          isArrayType: true,
          source: 'tags_lookup',
          joinColumn: 'id',
          resolutionColumns: ['tag_name'],
        },
      ],
      tableSchemas: [OWNERS_LOOKUP_SCHEMA],
    };

    const sql = await cubeQueryToSQLWithResolutionWithArray({
      query,
      tableSchemas: [TICKETS_TABLE_SCHEMA],
      resolutionConfig,
    });

    console.log('Phase 1 SQL (multiple arrays):', sql);

    // Verify row_id is included
    expect(sql).toContain('row_id');

    // Both arrays should be unnested
    expect(sql.match(/unnest/g)?.length).toBeGreaterThanOrEqual(2);

    // Execute the SQL to verify it works
    const result = (await duckdbExec(sql)) as any[];

    expect(result.length).toBe(7);

    // Each row should have a row_id
    expect(result[0]).toHaveProperty('__base_query_with_row_id__row_id');
    expect(result[0]).toHaveProperty(
      '__base_query_with_row_id__tickets__count'
    );
    expect(result[0]).toHaveProperty('__base_query_with_row_id__tickets__id');
    expect(result[0]).toHaveProperty(
      '__base_query_with_row_id__tickets__owners'
    );
    expect(result[0]).toHaveProperty('__base_query_with_row_id__tickets__tags');
  });

  it('Should return regular SQL when no array fields need resolution', async () => {
    const query: Query = {
      measures: ['tickets.count'],
      dimensions: ['tickets.id', 'tickets.created_by'],
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
    };

    const sql = await cubeQueryToSQLWithResolutionWithArray({
      query,
      tableSchemas: [TICKETS_TABLE_SCHEMA],
      resolutionConfig,
    });

    console.log('SQL without resolution:', sql);

    // Should not have row_id or unnest when no array resolution is needed
    expect(sql).not.toContain('row_id');
    expect(sql).not.toContain('unnest');
  });
});
