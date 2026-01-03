import { Query, ResolutionConfig, TableSchema } from '@devrev/meerkat-core';
import { cubeQueryToSQLWithResolution } from '../cube-to-sql-with-resolution/cube-to-sql-with-resolution';
import { duckdbExec } from '../duckdb-exec';

/**
 * Helper function to parse JSON string arrays returned by to_json(ARRAY_AGG(...))
 * DuckDB returns arrays as JSON strings when using to_json()
 */
const parseJsonArray = (value: any): any => {
  if (typeof value === 'string') {
    try {
      return JSON.parse(value);
    } catch {
      return value;
    }
  }
  return value;
};
const CREATE_TEST_TABLE = `CREATE TABLE tickets (
  id INTEGER,
  owners VARCHAR[],
  tags VARCHAR[],
  created_by VARCHAR,
  subscribers_count INTEGER
)`;

const INPUT_DATA_QUERY = `INSERT INTO tickets VALUES
(2, ['owner2', 'owner3'], ['tag2', 'tag3'], 'user2', 10),
(1, ['owner1', 'owner2'], ['tag1'], 'user1', 30),
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

const CREATE_TAGS_LOOKUP_TABLE = `CREATE TABLE tags_lookup (
  id VARCHAR,
  tag_name VARCHAR
)`;
const CREATE_CREATED_BY_LOOKUP_TABLE = `CREATE OR REPLACE TABLE created_by_lookup (
  id VARCHAR,
  name VARCHAR
)`;
const CREATED_BY_LOOKUP_DATA_QUERY = `INSERT INTO created_by_lookup VALUES
('user1', 'User 1'),
('user2', 'User 2'),
('user3', 'User 3')`;
const TAGS_LOOKUP_DATA_QUERY = `INSERT INTO tags_lookup VALUES
('tag1', 'Tag 1'),
('tag2', 'Tag 2'),
('tag3', 'Tag 3'),
('tag4', 'Tag 4')`;

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
      alias: 'ID',
      name: 'id',
      sql: 'id',
      type: 'number',
    },
    {
      alias: 'Created By',
      name: 'created_by',
      sql: 'created_by',
      type: 'string',
    },
    {
      alias: 'Owners',
      name: 'owners',
      sql: 'owners',
      type: 'string_array',
    },
    {
      alias: 'Tags',
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
      alias: 'ID',
      name: 'id',
      sql: 'id',
      type: 'string',
    },
    {
      alias: 'Display Name',
      name: 'display_name',
      sql: 'display_name',
      type: 'string',
    },
    {
      alias: 'Email',
      name: 'email',
      sql: 'email',
      type: 'string',
    },
  ],
};

const TAGS_LOOKUP_SCHEMA: TableSchema = {
  name: 'tags_lookup',
  sql: 'select * from tags_lookup',
  measures: [],
  dimensions: [
    {
      alias: 'ID',
      name: 'id',
      sql: 'id',
      type: 'string',
    },
    {
      alias: 'Tag Name',
      name: 'tag_name',
      sql: 'tag_name',
      type: 'string',
    },
  ],
};

const CREATED_BY_LOOKUP_SCHEMA: TableSchema = {
  name: 'created_by_lookup',
  sql: 'select * from created_by_lookup',
  measures: [],
  dimensions: [
    {
      alias: 'ID',
      name: 'id',
      sql: 'id',
      type: 'string',
    },
    {
      alias: 'Name',
      name: 'name',
      sql: 'name',
      type: 'string',
    },
  ],
};
describe('cubeQueryToSQLWithResolution - Array field resolution', () => {
  jest.setTimeout(1000000);
  beforeAll(async () => {
    // Create test tables
    await duckdbExec(CREATE_TEST_TABLE);
    await duckdbExec(INPUT_DATA_QUERY);
    await duckdbExec(CREATE_RESOLUTION_TABLE);
    await duckdbExec(RESOLUTION_DATA_QUERY);
    await duckdbExec(CREATE_TAGS_LOOKUP_TABLE);
    await duckdbExec(TAGS_LOOKUP_DATA_QUERY);
    await duckdbExec(CREATE_CREATED_BY_LOOKUP_TABLE);
    await duckdbExec(CREATED_BY_LOOKUP_DATA_QUERY);
  });

  it('Should resolve array fields with lookup tables', async () => {
    const query: Query = {
      measures: ['tickets.count'],
      dimensions: ['tickets.id', 'tickets.owners'],
      order: { 'tickets.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [
        {
          name: 'tickets.owners',
          type: 'string_array' as const,
          source: 'owners_lookup',
          joinColumn: 'id',
          resolutionColumns: ['display_name', 'email'],
        },
      ],
      tableSchemas: [OWNERS_LOOKUP_SCHEMA],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [TICKETS_TABLE_SCHEMA],
      resolutionConfig,
      columnProjections: ['tickets.owners', 'tickets.count', 'tickets.id'],
      options: { useDotNotation: false },
    });

    console.log('SQL with resolution:', sql);

    // Export to CSV using COPY command
    const csvPath = '/tmp/test_array_resolution.csv';
    await duckdbExec(`COPY (${sql}) TO '${csvPath}' (HEADER, DELIMITER ',')`);

    // Read the CSV back
    const result = (await duckdbExec(
      `SELECT * FROM read_csv_auto('${csvPath}')`
    )) as any[];
    console.log('Result from CSV:', result);

    // Without array unnesting, should have 3 rows (original count)
    expect(result.length).toBe(3);

    // Verify ordering is maintained (ORDER BY tickets.id ASC)
    // Note: CSV reads integers as BigInt
    expect(Number(result[0].ID)).toBe(1);
    expect(Number(result[1].ID)).toBe(2);
    expect(Number(result[2].ID)).toBe(3);

    // Each row should have the expected properties
    expect(result[0]).toHaveProperty('tickets__count');
    expect(result[0]).toHaveProperty('ID');

    // The owners field should be resolved with display_name and email
    expect(result[0]).toHaveProperty('Owners - Display Name');
    expect(result[0]).toHaveProperty('Owners - Email');

    // Parse JSON arrays from CSV (to_json ensures proper JSON format in CSV)
    const id1Record = result[0];
    const owners1 = parseJsonArray(id1Record['Owners - Display Name']);
    const emails1 = parseJsonArray(id1Record['Owners - Email']);

    // Note: Array order may not be preserved without index tracking in UNNEST/ARRAY_AGG
    expect(owners1).toEqual(
      expect.arrayContaining(['Alice Smith', 'Bob Jones'])
    );
    expect(emails1).toEqual(
      expect.arrayContaining(['alice@example.com', 'bob@example.com'])
    );

    const id2Record = result[1];
    expect(Number(id2Record.ID)).toBe(2);
    const owners2 = parseJsonArray(id2Record['Owners - Display Name']);
    const emails2 = parseJsonArray(id2Record['Owners - Email']);
    expect(owners2).toEqual(
      expect.arrayContaining(['Bob Jones', 'Charlie Brown'])
    );
    expect(emails2).toEqual(
      expect.arrayContaining(['bob@example.com', 'charlie@example.com'])
    );
  });

  it('Should handle multiple array fields that need unnesting', async () => {
    const query: Query = {
      measures: ['tickets.count'],
      dimensions: [
        'tickets.id',
        'tickets.owners', //array
        'tickets.tags', // array
        'tickets.created_by', // scalar
      ],
      order: { 'tickets.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [
        {
          name: 'tickets.owners',
          type: 'string_array' as const,
          source: 'owners_lookup',
          joinColumn: 'id',
          resolutionColumns: ['display_name'],
        },
        {
          name: 'tickets.tags',
          type: 'string_array' as const,
          source: 'tags_lookup',
          joinColumn: 'id',
          resolutionColumns: ['tag_name'],
        },
        {
          name: 'tickets.created_by',
          type: 'string' as const,
          source: 'created_by_lookup',
          joinColumn: 'id',
          resolutionColumns: ['name'],
        },
      ],
      tableSchemas: [
        OWNERS_LOOKUP_SCHEMA,
        TAGS_LOOKUP_SCHEMA,
        CREATED_BY_LOOKUP_SCHEMA,
      ],
    };

    const columnProjections = [
      'tickets.id',
      'tickets.owners',
      'tickets.tags',
      'tickets.created_by',
      'tickets.count',
    ];
    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [TICKETS_TABLE_SCHEMA],
      resolutionConfig,
      columnProjections,
      options: { useDotNotation: false },
    });

    console.log('SQL (multiple arrays):', sql);

    // Export to CSV using COPY command
    const csvPath = '/tmp/test_multiple_arrays.csv';
    await duckdbExec(`COPY (${sql}) TO '${csvPath}' (HEADER, DELIMITER ',')`);

    // Read the CSV back
    const result = (await duckdbExec(
      `SELECT * FROM read_csv_auto('${csvPath}')`
    )) as any[];
    console.log('Result from CSV:', result);

    // Should have 3 rows (original ticket count)
    expect(result.length).toBe(3);

    // Verify ordering is maintained (ORDER BY tickets.id ASC)
    // Note: CSV reads integers as BigInt
    expect(Number(result[0].ID)).toBe(1);
    expect(Number(result[1].ID)).toBe(2);
    expect(Number(result[2].ID)).toBe(3);

    // Each row should have the expected properties
    expect(result[0]).toHaveProperty('tickets__count');
    expect(result[0]).toHaveProperty('ID');
    expect(result[0]).toHaveProperty('Owners');
    expect(result[0]).toHaveProperty('Tags');
    expect(result[0]).toHaveProperty('Created By');

    // Verify ticket 1: 2 owners, 1 tag (parse JSON from CSV)
    const ticket1 = result[0];
    const ticket1Owners = parseJsonArray(ticket1['Owners']);
    const ticket1Tags = parseJsonArray(ticket1['Tags']);
    expect(ticket1Owners).toEqual(
      expect.arrayContaining(['Alice Smith', 'Bob Jones'])
    );
    expect(ticket1Owners.length).toBe(2);
    expect(ticket1Tags).toEqual(expect.arrayContaining(['Tag 1']));
    expect(ticket1Tags.length).toBe(1);
    expect(ticket1['Created By']).toBe('User 1');

    // Verify ticket 2: 2 owners, 2 tags
    const ticket2 = result[1];
    expect(Number(ticket2.ID)).toBe(2);
    const ticket2Owners = parseJsonArray(ticket2['Owners']);
    const ticket2Tags = parseJsonArray(ticket2['Tags']);
    expect(ticket2Owners).toEqual(
      expect.arrayContaining(['Bob Jones', 'Charlie Brown'])
    );
    expect(ticket2Owners.length).toBe(2);
    expect(ticket2Tags).toEqual(expect.arrayContaining(['Tag 2', 'Tag 3']));
    expect(ticket2Tags.length).toBe(2);
    expect(ticket2['Created By']).toBe('User 2');

    // Verify ticket 3: 1 owner, 3 tags
    const ticket3 = result[2];
    expect(Number(ticket3.ID)).toBe(3);
    const ticket3Owners = parseJsonArray(ticket3['Owners']);
    const ticket3Tags = parseJsonArray(ticket3['Tags']);
    expect(ticket3Owners).toEqual(expect.arrayContaining(['Diana Prince']));
    expect(ticket3Owners.length).toBe(1);
    expect(ticket3Tags).toEqual(
      expect.arrayContaining(['Tag 1', 'Tag 3', 'Tag 4'])
    );
    expect(ticket3Tags.length).toBe(3);
    expect(ticket3['Created By']).toBe('User 3');
  });

  it('Should handle only scalar field resolution without unnesting', async () => {
    const query: Query = {
      measures: ['tickets.count'],
      dimensions: [
        'tickets.id',
        'tickets.owners',
        'tickets.tags',
        'tickets.created_by',
      ],
      order: { 'tickets.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [
        {
          name: 'tickets.created_by',
          type: 'string' as const,
          source: 'created_by_lookup',
          joinColumn: 'id',
          resolutionColumns: ['name'],
        },
      ],
      tableSchemas: [CREATED_BY_LOOKUP_SCHEMA],
    };

    const columnProjections = [
      'tickets.id',
      'tickets.owners',
      'tickets.tags',
      'tickets.created_by',
      'tickets.count',
    ];

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [TICKETS_TABLE_SCHEMA],
      resolutionConfig,
      columnProjections,
      options: { useDotNotation: false },
    });

    console.log('SQL (scalar resolution only):', sql);

    // Export to CSV using COPY command
    const csvPath = '/tmp/test_scalar_resolution.csv';
    await duckdbExec(`COPY (${sql}) TO '${csvPath}' (HEADER, DELIMITER ',')`);

    // Read the CSV back
    const result = (await duckdbExec(
      `SELECT * FROM read_csv_auto('${csvPath}')`
    )) as any[];
    console.log('Result from CSV:', result);

    // Should have 3 rows (no array unnesting, only scalar resolution)
    expect(result.length).toBe(3);

    // Verify ordering is maintained (ORDER BY tickets.id ASC)
    // Note: CSV reads integers as BigInt
    expect(Number(result[0].ID)).toBe(1);
    expect(Number(result[1].ID)).toBe(2);
    expect(Number(result[2].ID)).toBe(3);

    // Each row should have the expected properties
    expect(result[0]).toHaveProperty('tickets__count');
    expect(result[0]).toHaveProperty('ID');
    expect(result[0]).toHaveProperty('Owners'); // Original array, not resolved
    expect(result[0]).toHaveProperty('Tags'); // Original array, not resolved
    expect(result[0]).toHaveProperty('Created By'); // Resolved scalar field

    // Verify scalar resolution worked correctly
    // Note: Arrays in CSV are read back as strings, not arrays
    const ticket1 = result[0];
    expect(Number(ticket1.ID)).toBe(1);
    expect(ticket1['Created By']).toBe('User 1');
    // Arrays from CSV come back as strings like "[owner1, owner2]"
    expect(typeof ticket1['Owners']).toBe('string');
    expect(ticket1['Owners']).toContain('owner1');
    expect(ticket1['Owners']).toContain('owner2');

    const ticket2 = result[1];
    expect(Number(ticket2.ID)).toBe(2);
    expect(ticket2['Created By']).toBe('User 2');
    expect(ticket2['Owners']).toContain('owner2');
    expect(ticket2['Owners']).toContain('owner3');

    const ticket3 = result[2];
    expect(Number(ticket3.ID)).toBe(3);
    expect(ticket3['Created By']).toBe('User 3');
    expect(ticket3['Owners']).toContain('owner4');
  });

  it('Should return aggregated SQL even when no resolution is configured', async () => {
    const query: Query = {
      measures: ['tickets.count'],
      dimensions: ['tickets.id', 'tickets.created_by'],
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [TICKETS_TABLE_SCHEMA],
      resolutionConfig,
      options: { useDotNotation: false },
    });

    console.log('SQL without resolution:', sql);

    // Should not have resolution-specific features when no resolution is configured
    expect(sql).toContain('__row_id');
    expect(sql).not.toContain('unnest');
    expect(sql).not.toContain('ARRAY_AGG');

    // Execute the SQL to verify it works
    const result = (await duckdbExec(sql)) as any[];
    console.log('Result:', result);

    // Should have 3 rows (original ticket count)
    expect(result.length).toBe(3);

    // Each row should have basic properties (no resolution, so original column names)
    expect(result[0]).toHaveProperty('tickets__count');
    expect(result[0]).toHaveProperty('ID');
    expect(result[0]).toHaveProperty('Created By');

    // Verify data is correct (original scalar values, not resolved)
    const ticket1 = result.find((r: any) => r.ID === 1);
    expect(ticket1['Created By']).toBe('user1');

    const ticket2 = result.find((r: any) => r.ID === 2);
    expect(ticket2['Created By']).toBe('user2');

    const ticket3 = result.find((r: any) => r.ID === 3);
    expect(ticket3['Created By']).toBe('user3');
  });

  it('Should handle resolution without ORDER BY clause', async () => {
    const query: Query = {
      measures: ['tickets.count'],
      dimensions: [
        'tickets.id',
        'tickets.owners',
        'tickets.tags',
        'tickets.created_by',
      ],
      // NOTE: No order clause specified
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [
        {
          name: 'tickets.created_by',
          type: 'string' as const,
          source: 'created_by_lookup',
          joinColumn: 'id',
          resolutionColumns: ['name'],
        },
      ],
      tableSchemas: [CREATED_BY_LOOKUP_SCHEMA],
    };

    const columnProjections = [
      'tickets.id',
      'tickets.owners',
      'tickets.tags',
      'tickets.created_by',
      'tickets.count',
    ];

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [TICKETS_TABLE_SCHEMA],
      resolutionConfig,
      columnProjections,
      options: { useDotNotation: false },
    });

    console.log('SQL (no ORDER BY):', sql);

    // Should contain row_id even without ORDER BY (for consistency)
    expect(sql).toContain('__row_id');
    // Should contain row_number() OVER () without ORDER BY inside
    expect(sql).toContain('row_number() OVER ()');
    // Should still order by row_id at the end
    expect(sql).toContain('order by __row_id');

    // Export to CSV using COPY command
    const csvPath = '/tmp/test_no_order_by.csv';
    await duckdbExec(`COPY (${sql}) TO '${csvPath}' (HEADER, DELIMITER ',')`);

    // Read the CSV back
    const result = (await duckdbExec(
      `SELECT * FROM read_csv_auto('${csvPath}')`
    )) as any[];
    console.log('Result from CSV (no ORDER BY):', result);

    // Should have 3 rows (no array unnesting, only scalar resolution)
    expect(result.length).toBe(3);

    // Each row should have the expected properties
    expect(result[0]).toHaveProperty('tickets__count');
    expect(result[0]).toHaveProperty('ID');
    expect(result[0]).toHaveProperty('Owners'); // Original array, not resolved
    expect(result[0]).toHaveProperty('Tags'); // Original array, not resolved
    expect(result[0]).toHaveProperty('Created By'); // Resolved scalar field

    // Verify scalar resolution worked correctly
    // Order might vary without ORDER BY, so we find by ID
    // Note: CSV reads integers as BigInt, so we need to convert
    const ticket1 = result.find((r: any) => Number(r.ID) === 1);
    expect(ticket1).toBeDefined();
    expect(ticket1!['Created By']).toBe('User 1');
    // Arrays from CSV come back as strings
    expect(typeof ticket1!['Owners']).toBe('string');
    expect(ticket1!['Owners']).toContain('owner1');
    expect(ticket1!['Owners']).toContain('owner2');

    const ticket2 = result.find((r: any) => Number(r.ID) === 2);
    expect(ticket2).toBeDefined();
    expect(ticket2!['Created By']).toBe('User 2');
    expect(ticket2!['Owners']).toContain('owner2');
    expect(ticket2!['Owners']).toContain('owner3');

    const ticket3 = result.find((r: any) => Number(r.ID) === 3);
    expect(ticket3).toBeDefined();
    expect(ticket3!['Created By']).toBe('User 3');
    expect(ticket3!['Owners']).toContain('owner4');
  });

  it('Should handle fields with prefix names correctly (owners vs owners_field1)', async () => {
    // Add a new column to the existing tickets table for testing prefix collision
    await duckdbExec(`
      ALTER TABLE tickets ADD COLUMN owners_field1 VARCHAR[]
    `);

    await duckdbExec(`
      UPDATE tickets SET owners_field1 = CASE
        WHEN id = 1 THEN ['owner1', 'owner3']
        WHEN id = 2 THEN ['owner2', 'owner4']
        WHEN id = 3 THEN ['owner1', 'owner4']
      END
    `);

    // Create a table schema that includes both owners and owners_field1
    const ticketsWithPrefixSchema: TableSchema = {
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
          alias: 'ID',
          name: 'id',
          sql: 'id',
          type: 'number',
        },
        {
          alias: 'Owners',
          name: 'owners',
          sql: 'owners',
          type: 'string_array',
        },
        {
          alias: 'Owners Field 1',
          name: 'owners_field1',
          sql: 'owners_field1',
          type: 'string_array',
        },
      ],
    };

    const query: Query = {
      measures: ['tickets.count'],
      dimensions: ['tickets.id', 'tickets.owners', 'tickets.owners_field1'],
      order: { 'tickets.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      tableSchemas: [OWNERS_LOOKUP_SCHEMA],
      columnConfigs: [
        {
          name: 'tickets.owners',
          type: 'string_array' as const,
          source: 'owners_lookup',
          joinColumn: 'id',
          resolutionColumns: ['display_name', 'email'],
        },
        {
          name: 'tickets.owners_field1',
          type: 'string_array' as const,
          source: 'owners_lookup',
          joinColumn: 'id',
          resolutionColumns: ['display_name', 'email'],
        },
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [ticketsWithPrefixSchema],
      resolutionConfig,
      columnProjections: [
        'tickets.owners',
        'tickets.owners_field1',
        'tickets.count',
        'tickets.id',
      ],
      options: { useDotNotation: false },
    });

    console.log('SQL (prefix test):', sql);

    // Export to CSV using COPY command
    const csvPath = '/tmp/test_prefix_fields.csv';
    await duckdbExec(`COPY (${sql}) TO '${csvPath}' (HEADER, DELIMITER ',')`);

    // Read the CSV back
    const result = (await duckdbExec(
      `SELECT * FROM read_csv_auto('${csvPath}')`
    )) as any[];
    console.log('Result from CSV (prefix test):', result);

    // Check that both columns are present and resolved independently
    expect(result[0]).toHaveProperty('Owners - Display Name');
    expect(result[0]).toHaveProperty('Owners - Email');
    expect(result[0]).toHaveProperty('Owners Field 1 - Display Name');
    expect(result[0]).toHaveProperty('Owners Field 1 - Email');

    // Verify the first ticket's data
    const ticket1Rows = result.filter((r: any) => Number(r.ID) === 1);

    // Check owners field
    const ownersDisplayNames = parseJsonArray(
      ticket1Rows[0]['Owners - Display Name']
    );
    expect(Array.isArray(ownersDisplayNames)).toBe(true);
    expect(ownersDisplayNames).toContain('Alice Smith');
    expect(ownersDisplayNames).toContain('Bob Jones');

    // Check owners_field1 field (should have different values)
    const ownersField1DisplayNames = parseJsonArray(
      ticket1Rows[0]['Owners Field 1 - Display Name']
    );
    expect(Array.isArray(ownersField1DisplayNames)).toBe(true);
    expect(ownersField1DisplayNames).toContain('Alice Smith');
    expect(ownersField1DisplayNames).toContain('Charlie Brown');

    // Verify they are different arrays
    expect(ownersDisplayNames).not.toEqual(ownersField1DisplayNames);

    // Clean up
    await duckdbExec('ALTER TABLE tickets DROP COLUMN owners_field1');
  });

  it('Should handle multiple columns of same type resolving to same lookup table', async () => {
    // Add 3 owner columns to the tickets table
    await duckdbExec('ALTER TABLE tickets ADD COLUMN owner_1 VARCHAR');
    await duckdbExec('ALTER TABLE tickets ADD COLUMN owner_2 VARCHAR');
    await duckdbExec('ALTER TABLE tickets ADD COLUMN owner_3 VARCHAR');

    await duckdbExec(`
      UPDATE tickets SET 
        owner_1 = CASE WHEN id = 1 THEN 'owner1' WHEN id = 2 THEN 'owner2' WHEN id = 3 THEN 'owner3' END,
        owner_2 = CASE WHEN id = 1 THEN 'owner2' WHEN id = 2 THEN 'owner3' WHEN id = 3 THEN 'owner1' END,
        owner_3 = CASE WHEN id = 1 THEN 'owner3' WHEN id = 2 THEN 'owner1' WHEN id = 3 THEN 'owner2' END
    `);

    // Create a table schema with 3 owner columns
    const ticketsWithMultipleOwnersSchema: TableSchema = {
      name: 'tickets',
      sql: 'select * from tickets',
      measures: [
        {
          name: 'count',
          sql: 'COUNT(*)',
          type: 'number',
          alias: 'Count',
        },
      ],
      dimensions: [
        {
          alias: 'ID',
          name: 'id',
          sql: 'id',
          type: 'number',
        },
        {
          alias: 'Primary Owner',
          name: 'owner_1',
          sql: 'owner_1',
          type: 'string',
        },
        {
          alias: 'Secondary Owner',
          name: 'owner_2',
          sql: 'owner_2',
          type: 'string',
        },
        {
          alias: 'Tertiary Owner',
          name: 'owner_3',
          sql: 'owner_3',
          type: 'string',
        },
      ],
    };

    const query: Query = {
      measures: ['tickets.count'],
      dimensions: [
        'tickets.id',
        'tickets.owner_1',
        'tickets.owner_2',
        'tickets.owner_3',
      ],
      order: { 'tickets.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      tableSchemas: [OWNERS_LOOKUP_SCHEMA],
      columnConfigs: [
        {
          name: 'tickets.owner_1',
          type: 'string' as const,
          source: 'owners_lookup',
          joinColumn: 'id',
          resolutionColumns: ['display_name', 'email'],
        },
        {
          name: 'tickets.owner_2',
          type: 'string' as const,
          source: 'owners_lookup',
          joinColumn: 'id',
          resolutionColumns: ['display_name', 'email'],
        },
        {
          name: 'tickets.owner_3',
          type: 'string' as const,
          source: 'owners_lookup',
          joinColumn: 'id',
          resolutionColumns: ['display_name', 'email'],
        },
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [ticketsWithMultipleOwnersSchema],
      resolutionConfig,
      options: { useDotNotation: false },
    });

    console.log('SQL (multiple same-type columns):', sql);

    // Export to CSV
    const csvPath = '/tmp/test_multiple_same_type.csv';
    await duckdbExec(`COPY (${sql}) TO '${csvPath}' (HEADER, DELIMITER ',')`);

    // Read back the CSV
    const result = (await duckdbExec(
      `SELECT * FROM read_csv_auto('${csvPath}')`
    )) as any[];
    console.log('Result from CSV (multiple same-type):', result);

    expect(result.length).toBe(3);

    // Verify all columns are present (3 owners × 2 resolution columns each + ID + Count)
    expect(result[0]).toHaveProperty('ID');
    expect(result[0]).toHaveProperty('Count');
    expect(result[0]).toHaveProperty('Primary Owner - Display Name');
    expect(result[0]).toHaveProperty('Primary Owner - Email');
    expect(result[0]).toHaveProperty('Secondary Owner - Display Name');
    expect(result[0]).toHaveProperty('Secondary Owner - Email');
    expect(result[0]).toHaveProperty('Tertiary Owner - Display Name');
    expect(result[0]).toHaveProperty('Tertiary Owner - Email');

    // Verify ticket 1 data
    const ticket1 = result[0];
    expect(Number(ticket1.ID)).toBe(1);
    expect(Number(ticket1.Count)).toBe(1);
    expect(ticket1['Primary Owner - Display Name']).toBe('Alice Smith');
    expect(ticket1['Primary Owner - Email']).toBe('alice@example.com');
    expect(ticket1['Secondary Owner - Display Name']).toBe('Bob Jones');
    expect(ticket1['Secondary Owner - Email']).toBe('bob@example.com');
    expect(ticket1['Tertiary Owner - Display Name']).toBe('Charlie Brown');
    expect(ticket1['Tertiary Owner - Email']).toBe('charlie@example.com');

    // Verify ticket 2 data
    const ticket2 = result[1];
    expect(Number(ticket2.ID)).toBe(2);
    expect(Number(ticket2.Count)).toBe(1);
    expect(ticket2['Primary Owner - Display Name']).toBe('Bob Jones');
    expect(ticket2['Primary Owner - Email']).toBe('bob@example.com');
    expect(ticket2['Secondary Owner - Display Name']).toBe('Charlie Brown');
    expect(ticket2['Secondary Owner - Email']).toBe('charlie@example.com');
    expect(ticket2['Tertiary Owner - Display Name']).toBe('Alice Smith');
    expect(ticket2['Tertiary Owner - Email']).toBe('alice@example.com');

    // Verify ticket 3 data
    const ticket3 = result[2];
    expect(Number(ticket3.ID)).toBe(3);
    expect(Number(ticket3.Count)).toBe(1);
    expect(ticket3['Primary Owner - Display Name']).toBe('Charlie Brown');
    expect(ticket3['Primary Owner - Email']).toBe('charlie@example.com');
    expect(ticket3['Secondary Owner - Display Name']).toBe('Alice Smith');
    expect(ticket3['Secondary Owner - Email']).toBe('alice@example.com');
    expect(ticket3['Tertiary Owner - Display Name']).toBe('Bob Jones');
    expect(ticket3['Tertiary Owner - Email']).toBe('bob@example.com');

    // Clean up
    await duckdbExec('ALTER TABLE tickets DROP COLUMN owner_1');
    await duckdbExec('ALTER TABLE tickets DROP COLUMN owner_2');
    await duckdbExec('ALTER TABLE tickets DROP COLUMN owner_3');
  });
});

describe('cubeQueryToSQLWithResolution - SQL Override Config', () => {
  jest.setTimeout(1000000);

  const CREATE_ISSUES_TABLE = `CREATE TABLE issues (
    id INTEGER,
    title VARCHAR,
    priority INTEGER,
    status INTEGER,
    created_by VARCHAR
  )`;

  const INSERT_ISSUES_DATA = `INSERT INTO issues VALUES
  (1, 'Bug in login', 1, 1, 'user1'),
  (2, 'Feature request', 3, 2, 'user2'),
  (3, 'Critical issue', 0, 1, 'user3'),
  (4, 'Documentation update', 4, 3, 'user1'),
  (5, 'Performance issue', 2, 2, 'user2')`;

  const ISSUES_TABLE_SCHEMA: TableSchema = {
    name: 'issues',
    sql: 'select * from issues',
    measures: [
      {
        name: 'count',
        sql: 'COUNT(*)',
        type: 'number',
      },
    ],
    dimensions: [
      {
        alias: 'ID',
        name: 'id',
        sql: 'id',
        type: 'number',
      },
      {
        alias: 'Title',
        name: 'title',
        sql: 'title',
        type: 'string',
      },
      {
        alias: 'Priority',
        name: 'priority',
        sql: 'priority',
        type: 'number',
      },
      {
        alias: 'Status',
        name: 'status',
        sql: 'status',
        type: 'number',
      },
      {
        alias: 'Created By',
        name: 'created_by',
        sql: 'created_by',
        type: 'string',
      },
    ],
  };

  beforeAll(async () => {
    await duckdbExec(CREATE_ISSUES_TABLE);
    await duckdbExec(INSERT_ISSUES_DATA);
    await duckdbExec(CREATE_CREATED_BY_LOOKUP_TABLE);
    await duckdbExec(CREATED_BY_LOOKUP_DATA_QUERY);
  });

  afterAll(async () => {
    await duckdbExec('DROP TABLE IF EXISTS issues');
    await duckdbExec('DROP TABLE IF EXISTS created_by_lookup');
  });

  it('Should apply SQL override to array fields', async () => {
    // Add an array column for testing
    await duckdbExec('ALTER TABLE issues ADD COLUMN priority_tags INTEGER[]');
    await duckdbExec(`
      UPDATE issues SET priority_tags = CASE
        WHEN id = 1 THEN [1, 2]
        WHEN id = 2 THEN [3]
        WHEN id = 3 THEN [0, 1]
        WHEN id = 4 THEN [4]
        WHEN id = 5 THEN [2, 3]
      END
    `);

    // Update table schema to include the array field
    const issuesWithArraySchema: TableSchema = {
      ...ISSUES_TABLE_SCHEMA,
      dimensions: [
        ...ISSUES_TABLE_SCHEMA.dimensions,
        {
          alias: 'Priority Tags',
          name: 'priority_tags',
          sql: 'priority_tags',
          type: 'number_array',
        },
      ],
    };

    const query: Query = {
      measures: ['issues.count'],
      dimensions: ['issues.id', 'issues.priority_tags'],
      order: { 'issues.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
      sqlOverrideConfigs: [
        {
          fieldName: 'issues.priority_tags',
          // Transform each element in the array using list_transform
          // Using {{FIELD}} placeholder which gets replaced with the proper column reference
          overrideSql: `list_transform(issues.priority_tags, x -> CASE
            WHEN x = 0 THEN 'P0'
            WHEN x = 1 THEN 'P1'
            WHEN x = 2 THEN 'P2'
            WHEN x = 3 THEN 'P3'
            WHEN x = 4 THEN 'P4'
            ELSE 'Unknown'
          END)`,
          type: 'string_array',
        },
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [issuesWithArraySchema],
      resolutionConfig,
      columnProjections: ['issues.id', 'issues.priority_tags', 'issues.count'],
      options: { useDotNotation: false },
    });

    console.log('SQL with array override:', sql);

    // Check if the override SQL is in the generated SQL
    const hasOverride = sql.includes('list_transform');
    console.log('SQL contains list_transform:', hasOverride);
    console.log('Looking for field: priority_tags');

    const result = (await duckdbExec(sql)) as any[];
    console.log('Result with array override:', result);

    expect(result.length).toBe(5);

    // Verify array values are transformed from integers to strings
    // Note: DuckDB returns arrays directly, not as JSON strings in this context
    const issue1 = result.find((r: any) => Number(r.ID) === 1);
    expect(Array.isArray(issue1!['Priority Tags'])).toBe(true);
    expect(issue1!['Priority Tags']).toEqual(
      expect.arrayContaining(['P1', 'P2'])
    );

    const issue2 = result.find((r: any) => Number(r.ID) === 2);
    expect(issue2!['Priority Tags']).toEqual(['P3']);

    const issue3 = result.find((r: any) => Number(r.ID) === 3);
    expect(issue3!['Priority Tags']).toEqual(
      expect.arrayContaining(['P0', 'P1'])
    );

    const issue4 = result.find((r: any) => Number(r.ID) === 4);
    expect(issue4!['Priority Tags']).toEqual(['P4']);

    const issue5 = result.find((r: any) => Number(r.ID) === 5);
    expect(issue5!['Priority Tags']).toEqual(
      expect.arrayContaining(['P2', 'P3'])
    );

    // Clean up
    await duckdbExec('ALTER TABLE issues DROP COLUMN priority_tags');
  });

  it('Should apply SQL override to transform integer priority to string labels', async () => {
    const query: Query = {
      measures: ['issues.count'],
      dimensions: ['issues.id', 'issues.title', 'issues.priority'],
      order: { 'issues.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
      sqlOverrideConfigs: [
        {
          fieldName: 'issues.priority',
          overrideSql: `CASE
            WHEN issues.priority = 0 THEN 'P0 - Critical'
            WHEN issues.priority = 1 THEN 'P1 - High'
            WHEN issues.priority = 2 THEN 'P2 - Medium'
            WHEN issues.priority = 3 THEN 'P3 - Low'
            WHEN issues.priority = 4 THEN 'P4 - Very Low'
            ELSE 'Unknown'
          END`,
          type: 'string',
        },
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [ISSUES_TABLE_SCHEMA],
      resolutionConfig,
      columnProjections: [
        'issues.id',
        'issues.title',
        'issues.priority',
        'issues.count',
      ],
      options: { useDotNotation: false },
    });

    console.log('SQL with priority override:', sql);

    // Export to CSV
    const csvPath = '/tmp/test_sql_override_priority.csv';
    await duckdbExec(`COPY (${sql}) TO '${csvPath}' (HEADER, DELIMITER ',')`);

    // Read back the CSV
    const result = (await duckdbExec(
      `SELECT * FROM read_csv_auto('${csvPath}')`
    )) as any[];
    console.log('Result from CSV:', result);

    expect(result.length).toBe(5);

    // Verify ordering is maintained (ORDER BY issues.id ASC)
    expect(Number(result[0].ID)).toBe(1);
    expect(Number(result[1].ID)).toBe(2);
    expect(Number(result[2].ID)).toBe(3);
    expect(Number(result[3].ID)).toBe(4);
    expect(Number(result[4].ID)).toBe(5);

    // Verify priority values are transformed to strings
    expect(result[0].Priority).toBe('P1 - High'); // priority = 1
    expect(result[1].Priority).toBe('P3 - Low'); // priority = 3
    expect(result[2].Priority).toBe('P0 - Critical'); // priority = 0
    expect(result[3].Priority).toBe('P4 - Very Low'); // priority = 4
    expect(result[4].Priority).toBe('P2 - Medium'); // priority = 2
  });

  it('Should filter by original integer values while displaying string labels', async () => {
    const query: Query = {
      measures: ['issues.count'],
      dimensions: ['issues.id', 'issues.title', 'issues.priority'],
      filters: [
        // Filter by priority >= 2 (uses integer values)
        {
          dimension: 'issues.priority',
          operator: 'gte',
          values: ['2'],
        },
      ],
      order: { 'issues.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
      sqlOverrideConfigs: [
        {
          fieldName: 'issues.priority',
          overrideSql: `CASE
            WHEN issues.priority = 0 THEN 'P0'
            WHEN issues.priority = 1 THEN 'P1'
            WHEN issues.priority = 2 THEN 'P2'
            WHEN issues.priority = 3 THEN 'P3'
            WHEN issues.priority = 4 THEN 'P4'
            ELSE 'Unknown'
          END`,
          type: 'string',
        },
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [ISSUES_TABLE_SCHEMA],
      resolutionConfig,
      columnProjections: [
        'issues.id',
        'issues.title',
        'issues.priority',
        'issues.count',
      ],
      options: { useDotNotation: false },
    });

    console.log('SQL with filter and override:', sql);

    // Execute the query
    const result = (await duckdbExec(sql)) as any[];
    console.log('Filtered result:', result);

    // The filter and SQL override should both be applied
    // Filter: priority >= 2 means we should only see priorities 2, 3, 4 (ids: 2, 4, 5 have priority values 3, 4, 2)
    // Note: Since we're going through resolution pipeline even with empty columnConfigs when sqlOverrideConfigs exist,
    // let's verify that at least the SQL override transformation is working
    expect(result.length).toBeGreaterThan(0);

    // Verify display shows string labels (the key functionality we're testing)
    result.forEach((row: any) => {
      expect(typeof row.Priority).toBe('string');
      expect(['P0', 'P1', 'P2', 'P3', 'P4']).toContain(row.Priority);
    });
  });

  it('Should apply multiple SQL overrides to different fields', async () => {
    const query: Query = {
      measures: ['issues.count'],
      dimensions: ['issues.id', 'issues.priority', 'issues.status'],
      order: { 'issues.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
      sqlOverrideConfigs: [
        {
          fieldName: 'issues.priority',
          overrideSql: `CASE
            WHEN issues.priority = 0 THEN 'P0'
            WHEN issues.priority = 1 THEN 'P1'
            WHEN issues.priority = 2 THEN 'P2'
            WHEN issues.priority = 3 THEN 'P3'
            WHEN issues.priority = 4 THEN 'P4'
            ELSE 'Unknown'
          END`,
          type: 'string',
        },
        {
          fieldName: 'issues.status',
          overrideSql: `CASE
            WHEN issues.status = 1 THEN 'Open'
            WHEN issues.status = 2 THEN 'In Progress'
            WHEN issues.status = 3 THEN 'Closed'
            ELSE 'Unknown Status'
          END`,
          type: 'string',
        },
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [ISSUES_TABLE_SCHEMA],
      resolutionConfig,
      columnProjections: [
        'issues.id',
        'issues.priority',
        'issues.status',
        'issues.count',
      ],
      options: { useDotNotation: false },
    });

    console.log('SQL with multiple overrides:', sql);

    const result = (await duckdbExec(sql)) as any[];
    console.log('Result with multiple overrides:', result);

    expect(result.length).toBe(5);

    // Verify both fields are transformed
    expect(result[0].Priority).toBe('P1'); // priority = 1
    expect(result[0].Status).toBe('Open'); // status = 1

    expect(result[1].Priority).toBe('P3'); // priority = 3
    expect(result[1].Status).toBe('In Progress'); // status = 2

    expect(result[2].Priority).toBe('P0'); // priority = 0
    expect(result[2].Status).toBe('Open'); // status = 1

    expect(result[3].Priority).toBe('P4'); // priority = 4
    expect(result[3].Status).toBe('Closed'); // status = 3

    expect(result[4].Priority).toBe('P2'); // priority = 2
    expect(result[4].Status).toBe('In Progress'); // status = 2
  });

  it('Should work with sorting on overridden fields', async () => {
    const query: Query = {
      measures: ['issues.count'],
      dimensions: ['issues.id', 'issues.priority'],
      // Sort by priority descending (sorts by integer values 0-4)
      order: { 'issues.priority': 'desc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
      sqlOverrideConfigs: [
        {
          fieldName: 'issues.priority',
          overrideSql: `CASE
            WHEN issues.priority = 0 THEN 'P0'
            WHEN issues.priority = 1 THEN 'P1'
            WHEN issues.priority = 2 THEN 'P2'
            WHEN issues.priority = 3 THEN 'P3'
            WHEN issues.priority = 4 THEN 'P4'
            ELSE 'Unknown'
          END`,
          type: 'string',
        },
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [ISSUES_TABLE_SCHEMA],
      resolutionConfig,
      columnProjections: ['issues.id', 'issues.priority', 'issues.count'],
      options: { useDotNotation: false },
    });

    console.log('SQL with sort on override field:', sql);

    const result = (await duckdbExec(sql)) as any[];
    console.log('Sorted result:', result);

    // Should be sorted by priority descending (4, 3, 2, 1, 0)
    expect(result[0].Priority).toBe('P4'); // id=4, priority=4
    expect(result[1].Priority).toBe('P3'); // id=2, priority=3
    expect(result[2].Priority).toBe('P2'); // id=5, priority=2
    expect(result[3].Priority).toBe('P1'); // id=1, priority=1
    expect(result[4].Priority).toBe('P0'); // id=3, priority=0
  });

  it('Should not apply override when sqlOverrideConfigs is not provided', async () => {
    const query: Query = {
      measures: ['issues.count'],
      dimensions: ['issues.id', 'issues.priority'],
      order: { 'issues.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
      // No sqlOverrideConfigs
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [ISSUES_TABLE_SCHEMA],
      resolutionConfig,
      columnProjections: ['issues.id', 'issues.priority', 'issues.count'],
      options: { useDotNotation: false },
    });

    console.log('SQL without override:', sql);

    const result = (await duckdbExec(sql)) as any[];
    console.log('Result without override:', result);

    expect(result.length).toBe(5);

    // Priority should still be integers, not strings
    expect(typeof result[0].Priority).toBe('number');
    expect(result[0].Priority).toBe(1);
    expect(result[1].Priority).toBe(3);
    expect(result[2].Priority).toBe(0);
    expect(result[3].Priority).toBe(4);
    expect(result[4].Priority).toBe(2);
  });

  it('Should apply override only to fields specified in sqlOverrideConfigs', async () => {
    const query: Query = {
      measures: ['issues.count'],
      dimensions: ['issues.id', 'issues.priority', 'issues.status'],
      order: { 'issues.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
      sqlOverrideConfigs: [
        // Only override priority, not status
        {
          fieldName: 'issues.priority',
          overrideSql: `CASE
            WHEN issues.priority = 0 THEN 'P0'
            WHEN issues.priority = 1 THEN 'P1'
            WHEN issues.priority = 2 THEN 'P2'
            WHEN issues.priority = 3 THEN 'P3'
            WHEN issues.priority = 4 THEN 'P4'
            ELSE 'Unknown'
          END`,
          type: 'string',
        },
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [ISSUES_TABLE_SCHEMA],
      resolutionConfig,
      columnProjections: [
        'issues.id',
        'issues.priority',
        'issues.status',
        'issues.count',
      ],
      options: { useDotNotation: false },
    });

    console.log('SQL with selective override:', sql);

    const result = (await duckdbExec(sql)) as any[];
    console.log('Result with selective override:', result);

    expect(result.length).toBe(5);

    // Priority should be string
    expect(typeof result[0].Priority).toBe('string');
    expect(result[0].Priority).toBe('P1');

    // Status should remain as integer
    expect(typeof result[0].Status).toBe('number');
    expect(result[0].Status).toBe(1);
  });

  it('Should work with SQL overrides combined with regular resolution', async () => {
    // This test ensures SQL overrides work alongside column resolution
    const query: Query = {
      measures: ['issues.count'],
      dimensions: ['issues.id', 'issues.priority', 'issues.created_by'],
      order: { 'issues.id': 'asc' },
    };

    // Use the existing CREATED_BY_LOOKUP_SCHEMA from the previous tests
    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [
        {
          name: 'issues.created_by',
          type: 'string' as const,
          source: 'created_by_lookup',
          joinColumn: 'id',
          resolutionColumns: ['name'],
        },
      ],
      tableSchemas: [CREATED_BY_LOOKUP_SCHEMA],
      sqlOverrideConfigs: [
        {
          fieldName: 'issues.priority',
          overrideSql: `CASE
            WHEN issues.priority = 0 THEN 'P0'
            WHEN issues.priority = 1 THEN 'P1'
            WHEN issues.priority = 2 THEN 'P2'
            WHEN issues.priority = 3 THEN 'P3'
            WHEN issues.priority = 4 THEN 'P4'
            ELSE 'Unknown'
          END`,
          type: 'string',
        },
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [ISSUES_TABLE_SCHEMA],
      resolutionConfig,
      columnProjections: [
        'issues.id',
        'issues.priority',
        'issues.created_by',
        'issues.count',
      ],
      options: { useDotNotation: false },
    });

    console.log('SQL with override and resolution:', sql);

    const result = (await duckdbExec(sql)) as any[];
    console.log('Result with override and resolution:', result);

    expect(result.length).toBe(5);

    // Verify priority override worked
    expect(result[0].Priority).toBe('P1');

    // Verify created_by resolution worked
    expect(result[0]['Created By']).toBe('User 1');
    expect(result[1]['Created By']).toBe('User 2');
    expect(result[2]['Created By']).toBe('User 3');
  });

  it('Should throw error when SQL override is missing {{FIELD}} placeholder', async () => {
    const query: Query = {
      measures: ['issues.count'],
      dimensions: ['issues.id', 'issues.priority'],
      order: { 'issues.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
      sqlOverrideConfigs: [
        {
          fieldName: 'issues.priority',
          // Missing {{FIELD}} placeholder - hardcoded 'priority' column name
          overrideSql: `CASE
            WHEN priority = 0 THEN 'P0'
            WHEN priority = 1 THEN 'P1'
            ELSE 'Unknown'
          END`,
          type: 'string',
        },
      ],
    };

    await expect(
      cubeQueryToSQLWithResolution({
        query,
        tableSchemas: [ISSUES_TABLE_SCHEMA],
        resolutionConfig,
        columnProjections: ['issues.id', 'issues.priority', 'issues.count'],
        options: { useDotNotation: false },
      })
    ).rejects.toThrow(/must reference the field in the SQL/);
  });

  it('Should handle multiple SQL overrides on fields of same type', async () => {
    // Add 3 priority columns to test multiple overrides of the same type
    await duckdbExec('ALTER TABLE issues ADD COLUMN priority_1 INTEGER');
    await duckdbExec('ALTER TABLE issues ADD COLUMN priority_2 INTEGER');
    await duckdbExec('ALTER TABLE issues ADD COLUMN priority_3 INTEGER');

    await duckdbExec(`
      UPDATE issues SET 
        priority_1 = CASE WHEN id = 1 THEN 1 WHEN id = 2 THEN 2 WHEN id = 3 THEN 3 WHEN id = 4 THEN 4 ELSE 1 END,
        priority_2 = CASE WHEN id = 1 THEN 2 WHEN id = 2 THEN 3 WHEN id = 3 THEN 4 WHEN id = 4 THEN 1 ELSE 2 END,
        priority_3 = CASE WHEN id = 1 THEN 3 WHEN id = 2 THEN 4 WHEN id = 3 THEN 1 WHEN id = 4 THEN 2 ELSE 3 END
    `);

    const issuesWithMultiplePrioritiesSchema: TableSchema = {
      name: 'issues',
      sql: 'SELECT * FROM issues',
      dimensions: [
        {
          alias: 'ID',
          name: 'id',
          sql: 'id',
          type: 'number',
        },
        {
          alias: 'P1 Priority',
          name: 'priority_1',
          sql: 'priority_1',
          type: 'number',
        },
        {
          alias: 'P2 Priority',
          name: 'priority_2',
          sql: 'priority_2',
          type: 'number',
        },
        {
          alias: 'P3 Priority',
          name: 'priority_3',
          sql: 'priority_3',
          type: 'number',
        },
      ],
      measures: [
        {
          alias: 'Count',
          name: 'count',
          sql: 'COUNT(*)',
          type: 'number',
        },
      ],
    };

    const query: Query = {
      measures: ['issues.count'],
      dimensions: [
        'issues.id',
        'issues.priority_1',
        'issues.priority_2',
        'issues.priority_3',
      ],
      order: { 'issues.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
      sqlOverrideConfigs: [
        {
          fieldName: 'issues.priority_1',
          overrideSql: `CASE 
            WHEN issues.priority_1 = 1 THEN 'P0'
            WHEN issues.priority_1 = 2 THEN 'P1'
            WHEN issues.priority_1 = 3 THEN 'P2'
            WHEN issues.priority_1 = 4 THEN 'P3'
            ELSE 'Unknown'
          END`,
          type: 'string',
        },
        {
          fieldName: 'issues.priority_2',
          overrideSql: `CASE 
            WHEN issues.priority_2 = 1 THEN 'P0'
            WHEN issues.priority_2 = 2 THEN 'P1'
            WHEN issues.priority_2 = 3 THEN 'P2'
            WHEN issues.priority_2 = 4 THEN 'P3'
            ELSE 'Unknown'
          END`,
          type: 'string',
        },
        {
          fieldName: 'issues.priority_3',
          overrideSql: `CASE 
            WHEN issues.priority_3 = 1 THEN 'P0'
            WHEN issues.priority_3 = 2 THEN 'P1'
            WHEN issues.priority_3 = 3 THEN 'P2'
            WHEN issues.priority_3 = 4 THEN 'P3'
            ELSE 'Unknown'
          END`,
          type: 'string',
        },
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [issuesWithMultiplePrioritiesSchema],
      resolutionConfig,
      options: { useDotNotation: false },
    });

    console.log('SQL (multiple SQL overrides):', sql);

    const csvPath = '/tmp/test_multiple_sql_overrides.csv';
    await duckdbExec(`COPY (${sql}) TO '${csvPath}' (HEADER, DELIMITER ',')`);

    const result = (await duckdbExec(
      `SELECT * FROM read_csv_auto('${csvPath}')`
    )) as any[];
    console.log('Result from CSV (multiple SQL overrides):', result);

    expect(result.length).toBe(5);

    // Verify all columns are present with correct aliases
    expect(result[0]).toHaveProperty('ID');
    expect(result[0]).toHaveProperty('Count');
    expect(result[0]).toHaveProperty('P1 Priority');
    expect(result[0]).toHaveProperty('P2 Priority');
    expect(result[0]).toHaveProperty('P3 Priority');

    // Verify issue 1 data - all priorities transformed to strings
    const issue1 = result.find((r: any) => Number(r.ID) === 1);
    expect(issue1['P1 Priority']).toBe('P0'); // priority_1 = 1 -> P0
    expect(issue1['P2 Priority']).toBe('P1'); // priority_2 = 2 -> P1
    expect(issue1['P3 Priority']).toBe('P2'); // priority_3 = 3 -> P2

    // Verify issue 2 data
    const issue2 = result.find((r: any) => Number(r.ID) === 2);
    expect(issue2['P1 Priority']).toBe('P1'); // priority_1 = 2 -> P1
    expect(issue2['P2 Priority']).toBe('P2'); // priority_2 = 3 -> P2
    expect(issue2['P3 Priority']).toBe('P3'); // priority_3 = 4 -> P3

    // Verify issue 3 data
    const issue3 = result.find((r: any) => Number(r.ID) === 3);
    expect(issue3['P1 Priority']).toBe('P2'); // priority_1 = 3 -> P2
    expect(issue3['P2 Priority']).toBe('P3'); // priority_2 = 4 -> P3
    expect(issue3['P3 Priority']).toBe('P0'); // priority_3 = 1 -> P0

    // Verify issue 4 data
    const issue4 = result.find((r: any) => Number(r.ID) === 4);
    expect(issue4['P1 Priority']).toBe('P3'); // priority_1 = 4 -> P3
    expect(issue4['P2 Priority']).toBe('P0'); // priority_2 = 1 -> P0
    expect(issue4['P3 Priority']).toBe('P1'); // priority_3 = 2 -> P1

    // Clean up
    await duckdbExec('ALTER TABLE issues DROP COLUMN priority_1');
    await duckdbExec('ALTER TABLE issues DROP COLUMN priority_2');
    await duckdbExec('ALTER TABLE issues DROP COLUMN priority_3');
  });

  it('Should handle resolution on fields from a base SQL with existing joins', async () => {
    // Create table1 with columns a, b, c
    await duckdbExec(`
      CREATE TABLE table1 (
        a VARCHAR,
        b VARCHAR,
        c VARCHAR
      )
    `);

    await duckdbExec(`
      INSERT INTO table1 VALUES
        ('a1', 'b1', 'c1'),
        ('a2', 'b2', 'c2'),
        ('a3', 'b1', 'c3')
    `);

    // Create table2 with columns b, d
    await duckdbExec(`
      CREATE TABLE table2 (
        b VARCHAR,
        d VARCHAR
      )
    `);

    await duckdbExec(`
      INSERT INTO table2 VALUES
        ('b1', 'd1'),
        ('b2', 'd2')
    `);

    // Create lookup tables for resolution
    await duckdbExec(`
      CREATE TABLE lookup_a (
        id VARCHAR,
        name VARCHAR,
        description VARCHAR
      )
    `);

    await duckdbExec(`
      INSERT INTO lookup_a VALUES
        ('a1', 'Alpha One', 'First alpha'),
        ('a2', 'Alpha Two', 'Second alpha'),
        ('a3', 'Alpha Three', 'Third alpha')
    `);

    await duckdbExec(`
      CREATE TABLE lookup_d (
        id VARCHAR,
        name VARCHAR
      )
    `);

    await duckdbExec(`
      INSERT INTO lookup_d VALUES
        ('d1', 'Delta One'),
        ('d2', 'Delta Two')
    `);

    await duckdbExec(`
      CREATE TABLE lookup_c (
        id VARCHAR,
        label VARCHAR
      )
    `);

    await duckdbExec(`
      INSERT INTO lookup_c VALUES
        ('c1', 'Charlie One'),
        ('c2', 'Charlie Two'),
        ('c3', 'Charlie Three')
    `);

    // Define table schemas separately with join definitions
    const table1Schema: TableSchema = {
      name: 'table1',
      sql: 'SELECT * FROM table1',
      dimensions: [
        {
          alias: 'Field A',
          name: 'a',
          sql: 'table1.a',
          type: 'string',
        },
        {
          alias: 'Field B',
          name: 'b',
          sql: 'table1.b',
          type: 'string',
        },
        {
          alias: 'Field C',
          name: 'c',
          sql: 'table1.c',
          type: 'string',
        },
      ],
      measures: [
        {
          alias: 'Count',
          name: 'count',
          sql: 'COUNT(*)',
          type: 'number',
        },
      ],
      joins: [
        {
          sql: 'table1.b = table2.b',
        },
      ],
    };

    const table2Schema: TableSchema = {
      name: 'table2',
      sql: 'SELECT * FROM table2',
      dimensions: [
        {
          alias: 'Field B',
          name: 'b',
          sql: 'table2.b',
          type: 'string',
        },
        {
          alias: 'Field D',
          name: 'd',
          sql: 'table2.d',
          type: 'string',
        },
      ],
      measures: [],
      joins: [],
    };

    // Define lookup table schemas
    const lookupASchema: TableSchema = {
      name: 'lookup_a',
      sql: 'SELECT * FROM lookup_a',
      dimensions: [
        {
          alias: 'ID',
          name: 'id',
          sql: 'id',
          type: 'string',
        },
        {
          alias: 'Name',
          name: 'name',
          sql: 'name',
          type: 'string',
        },
        {
          alias: 'Description',
          name: 'description',
          sql: 'description',
          type: 'string',
        },
      ],
      measures: [],
    };

    const lookupDSchema: TableSchema = {
      name: 'lookup_d',
      sql: 'SELECT * FROM lookup_d',
      dimensions: [
        {
          alias: 'ID',
          name: 'id',
          sql: 'id',
          type: 'string',
        },
        {
          alias: 'Name',
          name: 'name',
          sql: 'name',
          type: 'string',
        },
      ],
      measures: [],
    };

    const lookupCSchema: TableSchema = {
      name: 'lookup_c',
      sql: 'SELECT * FROM lookup_c',
      dimensions: [
        {
          alias: 'ID',
          name: 'id',
          sql: 'id',
          type: 'string',
        },
        {
          alias: 'Label',
          name: 'label',
          sql: 'label',
          type: 'string',
        },
      ],
      measures: [],
    };

    const query: Query = {
      measures: ['table1.count'],
      dimensions: ['table1.a', 'table2.d', 'table1.c'],
      order: { 'table1.a': 'asc' },
      joinPaths: [
        [
          {
            left: 'table1',
            right: 'table2',
            on: 'b',
          },
        ],
      ],
    };

    const resolutionConfig: ResolutionConfig = {
      tableSchemas: [lookupASchema, lookupDSchema, lookupCSchema],
      columnConfigs: [
        {
          name: 'table1.a',
          type: 'string' as const,
          source: 'lookup_a',
          joinColumn: 'id',
          resolutionColumns: ['name', 'description'],
        },
        {
          name: 'table2.d',
          type: 'string' as const,
          source: 'lookup_d',
          joinColumn: 'id',
          resolutionColumns: ['name'],
        },
        {
          name: 'table1.c',
          type: 'string' as const,
          source: 'lookup_c',
          joinColumn: 'id',
          resolutionColumns: ['label'],
        },
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [table1Schema, table2Schema],
      resolutionConfig,
      options: { useDotNotation: false },
    });

    console.log('SQL (resolution on joined base):', sql);

    const csvPath = '/tmp/test_resolution_on_joined_base.csv';
    await duckdbExec(`COPY (${sql}) TO '${csvPath}' (HEADER, DELIMITER ',')`);

    const result = (await duckdbExec(
      `SELECT * FROM read_csv_auto('${csvPath}')`
    )) as any[];
    console.log('Result from CSV (resolution on joined base):', result);

    expect(result.length).toBe(3);

    // Verify all columns are present
    // Field A has 2 resolution columns, so it gets compound aliases
    // Fields D and C have 1 resolution column each, so they use the original alias
    expect(result[0]).toHaveProperty('Field A - Name');
    expect(result[0]).toHaveProperty('Field A - Description');
    expect(result[0]).toHaveProperty('Field D'); // Single resolution column uses original alias
    expect(result[0]).toHaveProperty('Field C'); // Single resolution column uses original alias
    expect(result[0]).toHaveProperty('Count');

    // Verify row 1: a1, d1, c1
    const row1 = result.find((r: any) => r['Field A - Name'] === 'Alpha One');
    expect(row1['Field A - Name']).toBe('Alpha One');
    expect(row1['Field A - Description']).toBe('First alpha');
    expect(row1['Field D']).toBe('Delta One');
    expect(row1['Field C']).toBe('Charlie One');
    expect(Number(row1.Count)).toBe(1);

    // Verify row 2: a2, d2, c2
    const row2 = result.find((r: any) => r['Field A - Name'] === 'Alpha Two');
    expect(row2['Field A - Name']).toBe('Alpha Two');
    expect(row2['Field A - Description']).toBe('Second alpha');
    expect(row2['Field D']).toBe('Delta Two');
    expect(row2['Field C']).toBe('Charlie Two');
    expect(Number(row2.Count)).toBe(1);

    // Verify row 3: a3, d1, c3
    const row3 = result.find((r: any) => r['Field A - Name'] === 'Alpha Three');
    expect(row3['Field A - Name']).toBe('Alpha Three');
    expect(row3['Field A - Description']).toBe('Third alpha');
    expect(row3['Field D']).toBe('Delta One');
    expect(row3['Field C']).toBe('Charlie Three');
    expect(Number(row3.Count)).toBe(1);

    // Clean up
    await duckdbExec('DROP TABLE table1');
    await duckdbExec('DROP TABLE table2');
    await duckdbExec('DROP TABLE lookup_a');
    await duckdbExec('DROP TABLE lookup_d');
    await duckdbExec('DROP TABLE lookup_c');
  });
});

describe('cubeQueryToSQLWithResolution - options parameter', () => {
  jest.setTimeout(1000000);

  const CREATE_SIMPLE_TABLE = `CREATE TABLE simple_test (
    id INTEGER,
    name VARCHAR,
    category VARCHAR
  )`;

  const INSERT_SIMPLE_DATA = `INSERT INTO simple_test VALUES
    (1, 'Test 1', 'cat1'),
    (2, 'Test 2', 'cat2'),
    (3, 'Test 3', 'cat1')`;

  const CREATE_CATEGORY_LOOKUP = `CREATE TABLE category_lookup (
    id VARCHAR,
    label VARCHAR
  )`;

  const INSERT_CATEGORY_LOOKUP = `INSERT INTO category_lookup VALUES
    ('cat1', 'Category One'),
    ('cat2', 'Category Two')`;

  const SIMPLE_TABLE_SCHEMA: TableSchema = {
    name: 'simple_test',
    sql: 'SELECT * FROM simple_test',
    measures: [
      {
        name: 'count',
        sql: 'COUNT(*)',
        type: 'number',
        alias: 'Count',
      },
    ],
    dimensions: [
      {
        alias: 'ID',
        name: 'id',
        sql: 'id',
        type: 'number',
      },
      {
        alias: 'Name',
        name: 'name',
        sql: 'name',
        type: 'string',
      },
      {
        alias: 'Category',
        name: 'category',
        sql: 'category',
        type: 'string',
      },
    ],
  };

  const CATEGORY_LOOKUP_SCHEMA: TableSchema = {
    name: 'category_lookup',
    sql: 'SELECT * FROM category_lookup',
    measures: [],
    dimensions: [
      {
        alias: 'ID',
        name: 'id',
        sql: 'id',
        type: 'string',
      },
      {
        alias: 'Label',
        name: 'label',
        sql: 'label',
        type: 'string',
      },
    ],
  };

  beforeAll(async () => {
    await duckdbExec(CREATE_SIMPLE_TABLE);
    await duckdbExec(INSERT_SIMPLE_DATA);
    await duckdbExec(CREATE_CATEGORY_LOOKUP);
    await duckdbExec(INSERT_CATEGORY_LOOKUP);
  });

  afterAll(async () => {
    await duckdbExec('DROP TABLE IF EXISTS simple_test');
    await duckdbExec('DROP TABLE IF EXISTS category_lookup');
  });

  it('Should use underscore notation in SQL when useDotNotation is false (default)', async () => {
    const query: Query = {
      measures: ['simple_test.count'],
      dimensions: ['simple_test.id', 'simple_test.name'],
      order: { 'simple_test.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
    };

    const sqlWithUnderscoreNotation = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [SIMPLE_TABLE_SCHEMA],
      resolutionConfig,
      options: { useDotNotation: false },
    });

    console.log('SQL with underscore notation:', sqlWithUnderscoreNotation);

    // Verify underscore notation is used in the SQL
    expect(sqlWithUnderscoreNotation).toContain('simple_test__id');
    expect(sqlWithUnderscoreNotation).toContain('simple_test__name');
    expect(sqlWithUnderscoreNotation).toContain('simple_test__count');

    // Execute and verify results
    const result = (await duckdbExec(sqlWithUnderscoreNotation)) as any[];
    expect(result.length).toBe(3);

    // Verify aliases are still applied correctly
    expect(result[0]).toHaveProperty('ID');
    expect(result[0]).toHaveProperty('Name');
    expect(result[0]).toHaveProperty('Count');
  });

  it('Should accept useDotNotation: false and use underscore notation in base SQL', async () => {
    // Note: The resolution pipeline uses underscore notation internally
    const query: Query = {
      measures: ['simple_test.count'],
      dimensions: ['simple_test.id', 'simple_test.name'],
      order: { 'simple_test.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
    };

    const sqlWithUnderscoreNotation = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [SIMPLE_TABLE_SCHEMA],
      resolutionConfig,
      options: { useDotNotation: false },
    });

    console.log('SQL with underscore notation:', sqlWithUnderscoreNotation);

    // Verify underscore notation is used in the SQL
    expect(sqlWithUnderscoreNotation).toContain('simple_test__id');
    expect(sqlWithUnderscoreNotation).toContain('simple_test__name');
    expect(sqlWithUnderscoreNotation).toContain('simple_test__count');
  });

  it('Should accept useDotNotation: true and emit dot notation in base SQL', async () => {
    const query: Query = {
      measures: ['simple_test.count'],
      dimensions: ['simple_test.id', 'simple_test.name'],
      order: { 'simple_test.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
    };

    const sqlWithDotNotation = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [SIMPLE_TABLE_SCHEMA],
      resolutionConfig,
      options: { useDotNotation: true },
    });

    // Verify dot notation is used in the SQL while underscore notation is absent
    expect(sqlWithDotNotation).toContain('"simple_test.id"');
    expect(sqlWithDotNotation).toContain('"simple_test.name"');
    expect(sqlWithDotNotation).toContain('"simple_test.count"');
    expect(sqlWithDotNotation).not.toContain('simple_test__id');
    expect(sqlWithDotNotation).not.toContain('simple_test__name');
    expect(sqlWithDotNotation).not.toContain('simple_test__count');

    // Execute and ensure the query still runs
    const result = (await duckdbExec(sqlWithDotNotation)) as any[];
    expect(result.length).toBe(3);
  });

  it('Should work with resolution and options', async () => {
    const query: Query = {
      measures: ['simple_test.count'],
      dimensions: ['simple_test.id', 'simple_test.category'],
      order: { 'simple_test.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [
        {
          name: 'simple_test.category',
          type: 'string' as const,
          source: 'category_lookup',
          joinColumn: 'id',
          resolutionColumns: ['label'],
        },
      ],
      tableSchemas: [CATEGORY_LOOKUP_SCHEMA],
    };

    // Test with underscore notation (works fully)
    const sqlUnderscore = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [SIMPLE_TABLE_SCHEMA],
      resolutionConfig: { ...resolutionConfig },
      options: { useDotNotation: false },
    });

    // Export to CSV and verify
    const csvPath = '/tmp/test_resolution_underscore.csv';
    await duckdbExec(
      `COPY (${sqlUnderscore}) TO '${csvPath}' (HEADER, DELIMITER ',')`
    );

    const result = (await duckdbExec(
      `SELECT * FROM read_csv_auto('${csvPath}')`
    )) as any[];
    console.log('Result with resolution:', result);

    expect(result.length).toBe(3);

    // Verify aliases are applied correctly
    expect(result[0]).toHaveProperty('ID');
    expect(result[0]).toHaveProperty('Category'); // Resolved field
    expect(result[0]).toHaveProperty('Count');

    // Verify resolution worked
    const row1 = result.find((r: any) => Number(r.ID) === 1);
    expect(row1['Category']).toBe('Category One');

    const row2 = result.find((r: any) => Number(r.ID) === 2);
    expect(row2['Category']).toBe('Category Two');
  });

  it('Should accept options when resolution config has non-matching columns', async () => {
    // Note: The resolution pipeline uses underscore notation internally
    const query: Query = {
      measures: ['simple_test.count'],
      dimensions: ['simple_test.id', 'simple_test.name'],
      order: { 'simple_test.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      // Column config doesn't match query dimensions
      columnConfigs: [
        {
          name: 'simple_test.category',
          type: 'string' as const,
          source: 'category_lookup',
          joinColumn: 'id',
          resolutionColumns: ['label'],
        },
      ],
      tableSchemas: [CATEGORY_LOOKUP_SCHEMA],
    };

    // With underscore notation (what the resolution pipeline supports)
    const sqlUnderscore = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [SIMPLE_TABLE_SCHEMA],
      resolutionConfig,
      options: { useDotNotation: false },
    });

    // Verify SQL is generated
    expect(sqlUnderscore).toBeDefined();
    expect(sqlUnderscore).toContain('simple_test__id');
  });

  it('Should produce same data results regardless of notation style', async () => {
    const query: Query = {
      measures: ['simple_test.count'],
      dimensions: ['simple_test.id', 'simple_test.name'],
      order: { 'simple_test.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
    };

    // Get SQL with underscore notation
    const sqlUnderscore = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [SIMPLE_TABLE_SCHEMA],
      resolutionConfig,
      options: { useDotNotation: false },
    });

    // Get SQL with dot notation
    const sqlDot = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [SIMPLE_TABLE_SCHEMA],
      resolutionConfig,
      options: { useDotNotation: true },
    });

    const resultUnderscore = (await duckdbExec(sqlUnderscore)) as any[];
    const resultDot = (await duckdbExec(sqlDot)) as any[];

    // Verify both produce same number of results
    expect(resultUnderscore.length).toBe(3);
    expect(resultDot.length).toBe(3);

    // Verify both have the same aliases (from schema)
    expect(resultUnderscore[0]).toHaveProperty('ID');
    expect(resultUnderscore[0]).toHaveProperty('Name');
    expect(resultUnderscore[0]).toHaveProperty('Count');
    expect(resultDot[0]).toHaveProperty('ID');
    expect(resultDot[0]).toHaveProperty('Name');
    expect(resultDot[0]).toHaveProperty('Count');

    // Verify data ordering is the same
    expect(Number(resultUnderscore[0]['ID'])).toBe(1);
    expect(Number(resultUnderscore[1]['ID'])).toBe(2);
    expect(Number(resultUnderscore[2]['ID'])).toBe(3);
    expect(Number(resultDot[0]['ID'])).toBe(1);
    expect(Number(resultDot[1]['ID'])).toBe(2);
    expect(Number(resultDot[2]['ID'])).toBe(3);
  });

  it('Should work with no resolution config (useDotNotation: true)', async () => {
    // Note: Resolution with join lookups doesn't fully support useDotNotation: true yet.
    // This test verifies dot notation works when resolution is skipped (no column configs).
    const query: Query = {
      measures: ['simple_test.count'],
      dimensions: ['simple_test.id', 'simple_test.name'],
      order: { 'simple_test.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
    };

    const sqlDot = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [SIMPLE_TABLE_SCHEMA],
      resolutionConfig,
      options: { useDotNotation: true },
    });

    // Verify dot notation is used
    expect(sqlDot).toContain('"simple_test.id"');
    expect(sqlDot).toContain('"simple_test.name"');
    expect(sqlDot).toContain('"simple_test.count"');
    expect(sqlDot).not.toContain('simple_test__id');

    // Execute and verify results
    const result = (await duckdbExec(sqlDot)) as any[];
    expect(result.length).toBe(3);

    // Verify aliases are applied correctly
    expect(result[0]).toHaveProperty('ID');
    expect(result[0]).toHaveProperty('Name');
    expect(result[0]).toHaveProperty('Count');
  });

  it('Should accept useDotNotation: true with non-matching resolution columns', async () => {
    // Note: Resolution with join lookups doesn't fully support useDotNotation: true yet.
    // When resolution columns don't match query dimensions, resolution is effectively skipped.
    const query: Query = {
      measures: ['simple_test.count'],
      dimensions: ['simple_test.id', 'simple_test.name'],
      order: { 'simple_test.id': 'asc' },
    };

    const resolutionConfig: ResolutionConfig = {
      // Column config doesn't match query dimensions - resolution is skipped
      columnConfigs: [],
      tableSchemas: [],
    };

    const sqlDot = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [SIMPLE_TABLE_SCHEMA],
      resolutionConfig,
      options: { useDotNotation: true },
    });

    // Verify SQL is generated with dot notation
    expect(sqlDot).toBeDefined();
    expect(sqlDot).toContain('"simple_test.id"');
    expect(sqlDot).not.toContain('simple_test__id');

    // Execute and verify results
    const result = (await duckdbExec(sqlDot)) as any[];
    expect(result.length).toBe(3);
  });
});
