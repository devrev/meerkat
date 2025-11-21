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
const CREATE_CREATED_BY_LOOKUP_TABLE = `CREATE TABLE created_by_lookup (
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
    expect(result[0]).toHaveProperty('Owners - Display Name');
    expect(result[0]).toHaveProperty('Tags - Tag Name');
    expect(result[0]).toHaveProperty('Created By - Name');

    // Verify ticket 1: 2 owners, 1 tag (parse JSON from CSV)
    const ticket1 = result[0];
    const ticket1Owners = parseJsonArray(ticket1['Owners - Display Name']);
    const ticket1Tags = parseJsonArray(ticket1['Tags - Tag Name']);
    expect(ticket1Owners).toEqual(
      expect.arrayContaining(['Alice Smith', 'Bob Jones'])
    );
    expect(ticket1Owners.length).toBe(2);
    expect(ticket1Tags).toEqual(expect.arrayContaining(['Tag 1']));
    expect(ticket1Tags.length).toBe(1);
    expect(ticket1['Created By - Name']).toBe('User 1');

    // Verify ticket 2: 2 owners, 2 tags
    const ticket2 = result[1];
    expect(Number(ticket2.ID)).toBe(2);
    const ticket2Owners = parseJsonArray(ticket2['Owners - Display Name']);
    const ticket2Tags = parseJsonArray(ticket2['Tags - Tag Name']);
    expect(ticket2Owners).toEqual(
      expect.arrayContaining(['Bob Jones', 'Charlie Brown'])
    );
    expect(ticket2Owners.length).toBe(2);
    expect(ticket2Tags).toEqual(expect.arrayContaining(['Tag 2', 'Tag 3']));
    expect(ticket2Tags.length).toBe(2);
    expect(ticket2['Created By - Name']).toBe('User 2');

    // Verify ticket 3: 1 owner, 3 tags
    const ticket3 = result[2];
    expect(Number(ticket3.ID)).toBe(3);
    const ticket3Owners = parseJsonArray(ticket3['Owners - Display Name']);
    const ticket3Tags = parseJsonArray(ticket3['Tags - Tag Name']);
    expect(ticket3Owners).toEqual(expect.arrayContaining(['Diana Prince']));
    expect(ticket3Owners.length).toBe(1);
    expect(ticket3Tags).toEqual(
      expect.arrayContaining(['Tag 1', 'Tag 3', 'Tag 4'])
    );
    expect(ticket3Tags.length).toBe(3);
    expect(ticket3['Created By - Name']).toBe('User 3');
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
    expect(result[0]).toHaveProperty('Created By - Name'); // Resolved scalar field

    // Verify scalar resolution worked correctly
    // Note: Arrays in CSV are read back as strings, not arrays
    const ticket1 = result[0];
    expect(Number(ticket1.ID)).toBe(1);
    expect(ticket1['Created By - Name']).toBe('User 1');
    // Arrays from CSV come back as strings like "[owner1, owner2]"
    expect(typeof ticket1['Owners']).toBe('string');
    expect(ticket1['Owners']).toContain('owner1');
    expect(ticket1['Owners']).toContain('owner2');

    const ticket2 = result[1];
    expect(Number(ticket2.ID)).toBe(2);
    expect(ticket2['Created By - Name']).toBe('User 2');
    expect(ticket2['Owners']).toContain('owner2');
    expect(ticket2['Owners']).toContain('owner3');

    const ticket3 = result[2];
    expect(Number(ticket3.ID)).toBe(3);
    expect(ticket3['Created By - Name']).toBe('User 3');
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
    expect(result[0]).toHaveProperty('Created By - Name'); // Resolved scalar field

    // Verify scalar resolution worked correctly
    // Order might vary without ORDER BY, so we find by ID
    // Note: CSV reads integers as BigInt, so we need to convert
    const ticket1 = result.find((r: any) => Number(r.ID) === 1);
    expect(ticket1).toBeDefined();
    expect(ticket1!['Created By - Name']).toBe('User 1');
    // Arrays from CSV come back as strings
    expect(typeof ticket1!['Owners']).toBe('string');
    expect(ticket1!['Owners']).toContain('owner1');
    expect(ticket1!['Owners']).toContain('owner2');

    const ticket2 = result.find((r: any) => Number(r.ID) === 2);
    expect(ticket2).toBeDefined();
    expect(ticket2!['Created By - Name']).toBe('User 2');
    expect(ticket2!['Owners']).toContain('owner2');
    expect(ticket2!['Owners']).toContain('owner3');

    const ticket3 = result.find((r: any) => Number(r.ID) === 3);
    expect(ticket3).toBeDefined();
    expect(ticket3!['Created By - Name']).toBe('User 3');
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
});
