import { Query, ResolutionConfig, TableSchema } from '@devrev/meerkat-core';
import { cubeQueryToSQLWithResolution } from '../cube-to-sql-with-resolution/cube-to-sql-with-resolution';
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
describe('cubeQueryToSQLWithResolutionWithArray - Phase 1: Unnest', () => {
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

  //   it('Should add row_id and unnest array fields that need resolution', async () => {
  //     const query: Query = {
  //       measures: ['tickets.count'],
  //       dimensions: ['tickets.id', 'tickets.owners'],
  //     };

  //     const resolutionConfig: ResolutionConfig = {
  //       columnConfigs: [
  //         {
  //           name: 'tickets.owners',
  //           isArrayType: true,
  //           source: 'owners_lookup',
  //           joinColumn: 'id',
  //           resolutionColumns: ['display_name', 'email'],
  //         },
  //       ],
  //       tableSchemas: [OWNERS_LOOKUP_SCHEMA],
  //     };

  //     debugger;
  //     const sql = await cubeQueryToSQLWithResolutionWithArray({
  //       query,
  //       tableSchemas: [TICKETS_TABLE_SCHEMA],
  //       resolutionConfig,
  //     });

  //     console.log('Phase 1 SQL (with row_id and unnest):', sql);

  //     // Verify the SQL includes row_id
  //     expect(sql).toContain('row_id');

  //     // Verify the SQL unnests the owners array
  //     expect(sql).toContain('unnest');

  //     // Verify it includes the owners dimension
  //     expect(sql).toContain('owners');

  //     // Execute the SQL to verify it works
  //     const result = (await duckdbExec(sql)) as any[];
  //     // console.log('Phase 1 Result:', JSON.stringify(result, null, 2));

  //     // The result should have unnested rows (more rows than the original 3)
  //     // Original: 3 rows, but ticket 1 has 2 owners, ticket 2 has 2 owners, ticket 3 has 1 owner
  //     // Expected: 5 unnested rows
  //     expect(result.length).toBe(5);

  //     // Each row should have a row_id
  //     expect(result[0]).toHaveProperty('__row_id');
  //     expect(result[0]).toHaveProperty('tickets__count');
  //     expect(result[0]).toHaveProperty('tickets__id');
  //     expect(result[0]).toHaveProperty('tickets__owners');

  //     debugger;
  //     // Verify row_ids are preserved (rows with same original row should have same row_id)
  //     const rowIds = result.map((r) => r.row_id);
  //     expect(rowIds.length).toBe(5); // 5 unnested rows total
  //   });

  it('Should handle multiple array fields that need unnesting', async () => {
    const query: Query = {
      measures: ['tickets.count'],
      dimensions: [
        'tickets.id',
        'tickets.owners', //array
        'tickets.tags', // array
        'tickets.created_by', // scalar
      ],
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
        {
          name: 'tickets.created_by',
          isArrayType: false,
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

    console.log('Phase 1 SQL (multiple arrays):', sql);

    // Verify row_id is included
    expect(sql).toContain('row_id');

    // Both arrays should be unnested
    expect(sql.match(/unnest/g)?.length).toBeGreaterThanOrEqual(2);

    // Execute the SQL to verify it works
    const result = (await duckdbExec(sql)) as any[];

    expect(result.length).toBe(7);

    // Each row should have a row_id
    expect(result[0]).toHaveProperty('__row_id');
    expect(result[0]).toHaveProperty('tickets__count');
    expect(result[0]).toHaveProperty('tickets__id');
    expect(result[0]).toHaveProperty('tickets__owners - display_name');
    expect(result[0]).toHaveProperty('tickets__tags - tag_name');
  });

  // it('test an proper query from UI', async () => {
  //   const query = {
  //     dimensions: ['dim_ticket.tags_json'],
  //     filters: [
  //       {
  //         and: [
  //           {
  //             and: [
  //               {
  //                 member: 'dim_ticket.created_date',
  //                 operator: 'inDateRange',
  //                 values: [
  //                   '2025-08-02T09:30:00.000Z',
  //                   '2025-10-31T10:29:59.999Z',
  //                 ],
  //               },
  //               {
  //                 member: 'dim_ticket.work_type',
  //                 operator: 'in',
  //                 values: ['ticket'],
  //               },
  //               {
  //                 member: 'dim_ticket.stage_json',
  //                 operator: 'in',
  //                 values: [
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/514',
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/512',
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/501',
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/485',
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/440',
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/25',
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/24',
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/22',
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/21',
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/19',
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/13',
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/10',
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/8',
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/7',
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/6',
  //                   'don:core:dvrv-us-1:devo/787:custom_stage/4',
  //                 ],
  //               },
  //             ],
  //           },
  //         ],
  //       },
  //     ],
  //     limit: 50000,
  //     measures: ['dim_ticket.id_count___function__count'],
  //   } as any;
  //   const tableSchemas = [
  //     {
  //       dimensions: [
  //         {
  //           name: 'rev_oid',
  //           sql: 'dim_ticket.rev_oid',
  //           type: 'string',
  //           alias: 'Customer Workspace',
  //         },
  //         {
  //           name: 'title',
  //           sql: 'dim_ticket.title',
  //           type: 'string',
  //           alias: 'Title',
  //         },
  //         {
  //           name: 'last_internal_comment_date',
  //           sql: 'dim_ticket.last_internal_comment_date',
  //           type: 'time',
  //           alias: 'Last Internal Comment Date',
  //         },
  //         {
  //           name: 'work_type',
  //           sql: "'ticket'",
  //           type: 'time',
  //           alias: 'Work Type',
  //         },
  //         {
  //           name: 'is_spam',
  //           sql: 'dim_ticket.is_spam',
  //           type: 'boolean',
  //           alias: 'Spam',
  //         },
  //         {
  //           name: 'object_type',
  //           sql: 'dim_ticket.object_type',
  //           type: 'string',
  //           alias: 'Object type',
  //         },
  //         {
  //           name: 'modified_by_id',
  //           sql: 'dim_ticket.modified_by_id',
  //           type: 'string',
  //           alias: 'Modified by',
  //         },
  //         {
  //           name: 'source_channel',
  //           sql: 'dim_ticket.source_channel',
  //           type: 'string',
  //           alias: 'Source channel',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'channels',
  //           sql: 'dim_ticket.channels',
  //           type: 'number_array',
  //           alias: 'Channels',
  //         },
  //         {
  //           name: 'created_by_id',
  //           sql: 'dim_ticket.created_by_id',
  //           type: 'string',
  //           alias: 'Created by',
  //         },
  //         {
  //           name: 'created_date',
  //           sql: 'dim_ticket.created_date',
  //           type: 'time',
  //           alias: 'Created date',
  //         },
  //         {
  //           name: 'target_close_date',
  //           sql: 'dim_ticket.target_close_date',
  //           type: 'time',
  //           alias: 'Target close date',
  //         },
  //         {
  //           name: 'applies_to_part_id',
  //           sql: 'dim_ticket.applies_to_part_id',
  //           type: 'string',
  //           alias: 'Part',
  //         },
  //         {
  //           name: 'subtype',
  //           sql: 'dim_ticket.subtype',
  //           type: 'string',
  //           alias: 'Subtype',
  //         },
  //         {
  //           name: 'actual_close_date',
  //           sql: 'dim_ticket.actual_close_date',
  //           type: 'time',
  //           alias: 'Close date',
  //         },
  //         {
  //           name: 'reported_by_id',
  //           sql: 'dim_ticket.reported_by_id',
  //           type: 'string',
  //           alias: 'Reported by ID',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'reported_by_ids',
  //           sql: 'dim_ticket.reported_by_ids',
  //           type: 'string_array',
  //           alias: 'Reported by',
  //         },
  //         {
  //           name: 'needs_response',
  //           sql: 'dim_ticket.needs_response',
  //           type: 'boolean',
  //           alias: 'Needs Response',
  //         },
  //         {
  //           name: 'group',
  //           sql: 'dim_ticket.group',
  //           type: 'string',
  //           alias: 'Group',
  //         },
  //         {
  //           name: 'modified_date',
  //           sql: 'dim_ticket.modified_date',
  //           type: 'time',
  //           alias: 'Modified date',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'owned_by_ids',
  //           sql: 'dim_ticket.owned_by_ids',
  //           type: 'string_array',
  //           alias: 'Owner',
  //         },
  //         {
  //           name: 'id',
  //           sql: 'dim_ticket.id',
  //           type: 'string',
  //           alias: 'ID',
  //         },
  //         {
  //           name: 'sla_tracker_id',
  //           sql: 'dim_ticket.sla_tracker_id',
  //           type: 'string',
  //           alias: 'SLA Tracker',
  //         },
  //         {
  //           name: 'severity',
  //           sql: 'dim_ticket.severity',
  //           type: 'number',
  //           alias: 'Severity',
  //         },
  //         {
  //           name: 'stage_json',
  //           sql: "json_extract_string(dim_ticket.stage_json, '$.stage_id')",
  //           type: 'string',
  //           alias: 'Stage',
  //         },
  //         {
  //           name: 'sla_id',
  //           sql: 'dim_ticket.sla_id',
  //           type: 'string',
  //           alias: 'SLA Name',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'links_json',
  //           sql: "list_distinct(CAST(json_extract_string(dim_ticket.links_json, '$[*].target_object_type') AS VARCHAR[]))",
  //           type: 'string_array',
  //           alias: 'Links',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'tags_json',
  //           sql: "CAST(json_extract_string(dim_ticket.tags_json, '$[*].tag_id') AS VARCHAR[])",
  //           type: 'string_array',
  //           alias: 'Tags',
  //         },
  //         {
  //           name: 'surveys_aggregation_json',
  //           sql: "list_aggregate(CAST(json_extract_string(dim_ticket.surveys_aggregation_json, '$[*].minimum') AS integer[]), 'min')",
  //           type: 'number',
  //           alias: 'CSAT Rating',
  //         },
  //         {
  //           name: 'sla_summary_target_time',
  //           sql: "cast(json_extract_string(dim_ticket.sla_summary, '$.target_time') as timestamp)",
  //           type: 'time',
  //           alias: 'Next SLA Target',
  //         },
  //         {
  //           name: 'staged_info',
  //           sql: "cast(json_extract_string(dim_ticket.staged_info, '$.is_staged') as boolean)",
  //           type: 'boolean',
  //           alias: 'Changes Need Review',
  //         },
  //         {
  //           name: 'tnt__account_id',
  //           sql: 'tnt__account_id',
  //           type: 'string',
  //           alias: 'account id',
  //         },
  //         {
  //           name: 'tnt__actual_effort_spent',
  //           sql: 'tnt__actual_effort_spent',
  //           type: 'number',
  //           alias: 'Actual Effort Spent',
  //         },
  //         {
  //           name: 'tnt__ai_subtype',
  //           sql: 'tnt__ai_subtype',
  //           type: 'string',
  //           alias: 'AI Subtype',
  //         },
  //         {
  //           name: 'tnt__asdfad',
  //           sql: 'tnt__asdfad',
  //           type: 'string',
  //           alias: 'asdfad',
  //         },
  //         {
  //           name: 'tnt__bool_summary',
  //           sql: 'tnt__bool_summary',
  //           type: 'boolean',
  //           alias: 'bool summary',
  //         },
  //         {
  //           name: 'tnt__boolyesnoresp',
  //           sql: 'tnt__boolyesnoresp',
  //           type: 'boolean',
  //           alias: 'boolyesnoresp',
  //         },
  //         {
  //           name: 'tnt__capability_part',
  //           sql: 'tnt__capability_part',
  //           type: 'string',
  //           alias: 'Capability part',
  //         },
  //         {
  //           name: 'tnt__card',
  //           sql: 'tnt__card',
  //           type: 'string',
  //           alias: 'card',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'tnt__cust_account',
  //           sql: 'tnt__cust_account',
  //           type: 'string_array',
  //           alias: 'cust_account',
  //         },
  //         {
  //           name: 'tnt__custom_field_for_sla',
  //           sql: 'tnt__custom_field_for_sla',
  //           type: 'string',
  //           alias: 'custom_field_for_sla',
  //         },
  //         {
  //           name: 'tnt__custom_id_field',
  //           sql: 'tnt__custom_id_field',
  //           type: 'string',
  //           alias: 'custom id field',
  //         },
  //         {
  //           name: 'tnt__customer_tier',
  //           sql: 'tnt__customer_tier',
  //           type: 'string',
  //           alias: 'Customer Tier',
  //         },
  //         {
  //           name: 'tnt__date_field',
  //           sql: 'tnt__date_field',
  //           type: 'time',
  //           alias: 'date_field',
  //         },
  //         {
  //           name: 'tnt__department_list',
  //           sql: 'tnt__department_list',
  //           type: 'string',
  //           alias: 'Department List',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'tnt__email_references',
  //           sql: 'tnt__email_references',
  //           type: 'string_array',
  //           alias: 'tnt__email_references',
  //         },
  //         {
  //           name: 'tnt__escalated',
  //           sql: 'tnt__escalated',
  //           type: 'boolean',
  //           alias: 'Escalated',
  //         },
  //         {
  //           name: 'tnt__estimated_effort',
  //           sql: 'tnt__estimated_effort',
  //           type: 'number',
  //           alias: 'Estimated Effort',
  //         },
  //         {
  //           name: 'tnt__external_source_id',
  //           sql: 'tnt__external_source_id',
  //           type: 'string',
  //           alias: 'tnt__external_source_id',
  //         },
  //         {
  //           name: 'tnt__feature_affected',
  //           sql: 'tnt__feature_affected',
  //           type: 'string',
  //           alias: 'Feature Affected',
  //         },
  //         {
  //           name: 'tnt__field1',
  //           sql: 'tnt__field1',
  //           type: 'string',
  //           alias: 'field1',
  //         },
  //         {
  //           name: 'tnt__foo',
  //           sql: 'tnt__foo',
  //           type: 'string',
  //           alias: 'tnt__foo',
  //         },
  //         {
  //           name: 'tnt__fruit',
  //           sql: 'tnt__fruit',
  //           type: 'string',
  //           alias: 'Fruit',
  //         },
  //         {
  //           name: 'tnt__git_template_name',
  //           sql: 'tnt__git_template_name',
  //           type: 'string',
  //           alias: 'Github Template Name',
  //         },
  //         {
  //           name: 'tnt__id_field',
  //           sql: 'tnt__id_field',
  //           type: 'string',
  //           alias: 'id field',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'tnt__id_list_field',
  //           sql: 'tnt__id_list_field',
  //           type: 'string_array',
  //           alias: 'id list field',
  //         },
  //         {
  //           name: 'tnt__impact',
  //           sql: 'tnt__impact',
  //           type: 'string',
  //           alias: 'Impact',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'tnt__include_on_emails',
  //           sql: 'tnt__include_on_emails',
  //           type: 'string_array',
  //           alias: 'Include on emails',
  //         },
  //         {
  //           name: 'tnt__issue_score',
  //           sql: 'tnt__issue_score',
  //           type: 'number',
  //           alias: 'Issue Score',
  //         },
  //         {
  //           name: 'tnt__knowledge_gap',
  //           sql: 'tnt__knowledge_gap',
  //           type: 'string',
  //           alias: 'Knowledge Gap',
  //         },
  //         {
  //           name: 'tnt__linear_assignee',
  //           sql: 'tnt__linear_assignee',
  //           type: 'string',
  //           alias: 'Linear Assignee',
  //         },
  //         {
  //           name: 'tnt__linear_description',
  //           sql: 'tnt__linear_description',
  //           type: 'string',
  //           alias: 'Linear Description',
  //         },
  //         {
  //           name: 'tnt__linear_id',
  //           sql: 'tnt__linear_id',
  //           type: 'string',
  //           alias: 'Linear Id',
  //         },
  //         {
  //           name: 'tnt__linear_identifier',
  //           sql: 'tnt__linear_identifier',
  //           type: 'string',
  //           alias: 'Linear Identifier',
  //         },
  //         {
  //           name: 'tnt__linear_priority',
  //           sql: 'tnt__linear_priority',
  //           type: 'string',
  //           alias: 'Linear Priority',
  //         },
  //         {
  //           name: 'tnt__linear_state',
  //           sql: 'tnt__linear_state',
  //           type: 'string',
  //           alias: 'Linear State',
  //         },
  //         {
  //           name: 'tnt__linear_team',
  //           sql: 'tnt__linear_team',
  //           type: 'string',
  //           alias: 'Linear Team',
  //         },
  //         {
  //           name: 'tnt__linear_title',
  //           sql: 'tnt__linear_title',
  //           type: 'string',
  //           alias: 'Linear Title',
  //         },
  //         {
  //           name: 'tnt__linear_url',
  //           sql: 'tnt__linear_url',
  //           type: 'string',
  //           alias: 'Linear Url',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'tnt__members',
  //           sql: 'tnt__members',
  //           type: 'string_array',
  //           alias: 'tnt__members',
  //         },
  //         {
  //           name: 'tnt__micro_service_name',
  //           sql: 'tnt__micro_service_name',
  //           type: 'string',
  //           alias: 'Microservice Name',
  //         },
  //         {
  //           name: 'tnt__notify_others',
  //           sql: 'tnt__notify_others',
  //           type: 'string',
  //           alias: 'notify others',
  //         },
  //         {
  //           name: 'tnt__numeric_field',
  //           sql: 'tnt__numeric_field',
  //           type: 'number',
  //           alias: 'numeric field',
  //         },
  //         {
  //           name: 'tnt__parent_part',
  //           sql: 'tnt__parent_part',
  //           type: 'string',
  //           alias: 'Parent part',
  //         },
  //         {
  //           name: 'tnt__part_product',
  //           sql: 'tnt__part_product',
  //           type: 'string',
  //           alias: 'part product',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'tnt__paytm_customer_name',
  //           sql: 'tnt__paytm_customer_name',
  //           type: 'string_array',
  //           alias: 'paytm customer name',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'tnt__platform',
  //           sql: 'tnt__platform',
  //           type: 'string_array',
  //           alias: 'Platform',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'tnt__portal_test',
  //           sql: 'tnt__portal_test',
  //           type: 'string_array',
  //           alias: 'portal test',
  //         },
  //         {
  //           name: 'tnt__product',
  //           sql: 'tnt__product',
  //           type: 'string',
  //           alias: 'Product',
  //         },
  //         {
  //           name: 'tnt__project',
  //           sql: 'tnt__project',
  //           type: 'string',
  //           alias: 'Project',
  //         },
  //         {
  //           name: 'tnt__project_stage',
  //           sql: 'tnt__project_stage',
  //           type: 'string',
  //           alias: 'Project Stage',
  //         },
  //         {
  //           name: 'tnt__rank',
  //           sql: 'tnt__rank',
  //           type: 'number',
  //           alias: 'Rank',
  //         },
  //         {
  //           name: 'tnt__remaining_effort',
  //           sql: 'tnt__remaining_effort',
  //           type: 'number',
  //           alias: 'Remaining Effort',
  //         },
  //         {
  //           name: 'tnt__required_text_field',
  //           sql: 'tnt__required_text_field',
  //           type: 'string',
  //           alias: 'required text field',
  //         },
  //         {
  //           name: 'tnt__resolution_sla_met',
  //           sql: 'tnt__resolution_sla_met',
  //           type: 'string',
  //           alias: 'Resolution SLA Met',
  //         },
  //         {
  //           name: 'tnt__resolution_sla_target_time',
  //           sql: 'tnt__resolution_sla_target_time',
  //           type: 'string',
  //           alias: 'Resolution SLA Target Time',
  //         },
  //         {
  //           name: 'tnt__resolution_turnaround_time',
  //           sql: 'tnt__resolution_turnaround_time',
  //           type: 'string',
  //           alias: 'Resolution SLA Turnaround Time',
  //         },
  //         {
  //           name: 'tnt__response_sla_met',
  //           sql: 'tnt__response_sla_met',
  //           type: 'string',
  //           alias: 'First Response SLA Met',
  //         },
  //         {
  //           name: 'tnt__response_sla_target_time',
  //           sql: 'tnt__response_sla_target_time',
  //           type: 'string',
  //           alias: 'First Response SLA Target Time',
  //         },
  //         {
  //           name: 'tnt__response_turnaround_time',
  //           sql: 'tnt__response_turnaround_time',
  //           type: 'string',
  //           alias: 'First Response SLA Turnaround Time',
  //         },
  //         {
  //           name: 'tnt__root_cause_analysis',
  //           sql: 'tnt__root_cause_analysis',
  //           type: 'string',
  //           alias: 'Root cause analysis',
  //         },
  //         {
  //           name: 'tnt__search_field_query',
  //           sql: 'tnt__search_field_query',
  //           type: 'string',
  //           alias: 'Search Field Query',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'tnt__stakeholders',
  //           sql: 'tnt__stakeholders',
  //           type: 'string_array',
  //           alias: 'stakeholders',
  //         },
  //         {
  //           name: 'tnt__stray_user',
  //           sql: 'tnt__stray_user',
  //           type: 'string',
  //           alias: 'stray user',
  //         },
  //         {
  //           name: 'tnt__stray_users',
  //           sql: 'tnt__stray_users',
  //           type: 'string',
  //           alias: 'stray users',
  //         },
  //         {
  //           name: 'tnt__test',
  //           sql: 'tnt__test',
  //           type: 'number',
  //           alias: 'Test',
  //         },
  //         {
  //           name: 'tnt__test123',
  //           sql: 'tnt__test123',
  //           type: 'string',
  //           alias: 'test123',
  //         },
  //         {
  //           name: 'tnt__test_capability_part',
  //           sql: 'tnt__test_capability_part',
  //           type: 'string',
  //           alias: 'Test Capability Part',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'tnt__test_field_for_sla',
  //           sql: 'tnt__test_field_for_sla',
  //           type: 'string_array',
  //           alias: 'test field for sla',
  //         },
  //         {
  //           name: 'tnt__test_filed',
  //           sql: 'tnt__test_filed',
  //           type: 'string',
  //           alias: 'test field',
  //         },
  //         {
  //           name: 'tnt__test_mikasa',
  //           sql: 'tnt__test_mikasa',
  //           type: 'string',
  //           alias: 'Test mikasa',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'tnt__test_multivalue_testrequiredfield',
  //           sql: 'tnt__test_multivalue_testrequiredfield',
  //           type: 'string_array',
  //           alias: 'test multivalue testrequiredfield',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'tnt__test_multivalue_textfield',
  //           sql: 'tnt__test_multivalue_textfield',
  //           type: 'string_array',
  //           alias: 'test multivalue textfield',
  //         },
  //         {
  //           name: 'tnt__test_product_part',
  //           sql: 'tnt__test_product_part',
  //           type: 'string',
  //           alias: 'Test Product part',
  //         },
  //         {
  //           name: 'tnt__testing_bool_drpdown',
  //           sql: 'tnt__testing_bool_drpdown',
  //           type: 'string',
  //           alias: 'Testing Bool Drpdown',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'tnt__text_list_account',
  //           sql: 'tnt__text_list_account',
  //           type: 'string_array',
  //           alias: 'Text list account',
  //         },
  //         {
  //           name: 'tnt__ticket_custom_part',
  //           sql: 'tnt__ticket_custom_part',
  //           type: 'string',
  //           alias: 'ticket custom part',
  //         },
  //         {
  //           name: 'tnt__ticket_dropd',
  //           sql: 'tnt__ticket_dropd',
  //           type: 'string',
  //           alias: 'ticket dropdown',
  //         },
  //         {
  //           name: 'tnt__ticket_type',
  //           sql: 'tnt__ticket_type',
  //           type: 'string',
  //           alias: 'Ticket type',
  //         },
  //         {
  //           name: 'tnt__tier',
  //           sql: 'tnt__tier',
  //           type: 'string',
  //           alias: 'Tier',
  //         },
  //         {
  //           name: 'tnt__urgency',
  //           sql: 'tnt__urgency',
  //           type: 'string',
  //           alias: 'Urgency',
  //         },
  //         {
  //           name: 'tnt__velocity_test_enum',
  //           sql: 'tnt__velocity_test_enum',
  //           type: 'string',
  //           alias: 'velocity test enum',
  //         },
  //         {
  //           name: 'tnt__version',
  //           sql: 'tnt__version',
  //           type: 'string',
  //           alias: 'Version',
  //         },
  //         {
  //           name: 'tnt__work_duration',
  //           sql: 'tnt__work_duration',
  //           type: 'string',
  //           alias: 'Work duration',
  //         },
  //         {
  //           name: 'tnt__workspace_custom',
  //           sql: 'tnt__workspace_custom',
  //           type: 'string',
  //           alias: 'workspace_custom',
  //         },
  //         {
  //           name: 'ctype_deal_registration__buying_region',
  //           sql: 'ctype_deal_registration__buying_region',
  //           type: 'string',
  //           alias: 'Theater',
  //         },
  //         {
  //           name: 'ctype_deal_registration__deal_status',
  //           sql: 'ctype_deal_registration__deal_status',
  //           type: 'string',
  //           alias: 'Deal Status',
  //         },
  //         {
  //           name: 'ctype_deal_registration__deal_type',
  //           sql: 'ctype_deal_registration__deal_type',
  //           type: 'string',
  //           alias: 'Deal Type',
  //         },
  //         {
  //           name: 'ctype_deal_registration__employee_count',
  //           sql: 'ctype_deal_registration__employee_count',
  //           type: 'string',
  //           alias: 'Employee Count',
  //         },
  //         {
  //           name: 'ctype_deal_registration__estimated_deal_value',
  //           sql: 'ctype_deal_registration__estimated_deal_value',
  //           type: 'number',
  //           alias: 'Estimated Deal Value',
  //         },
  //         {
  //           name: 'ctype_deal_registration__expected_close_date',
  //           sql: 'ctype_deal_registration__expected_close_date',
  //           type: 'time',
  //           alias: 'Expected Close Date',
  //         },
  //         {
  //           name: 'ctype_deal_registration__partner_type',
  //           sql: 'ctype_deal_registration__partner_type',
  //           type: 'string',
  //           alias: 'Partner Type',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_deal_registration__products_or_services',
  //           sql: 'ctype_deal_registration__products_or_services',
  //           type: 'string_array',
  //           alias: 'Product(s) or Service(s)',
  //         },
  //         {
  //           name: 'ctype_deal_registration__prospect_company_name',
  //           sql: 'ctype_deal_registration__prospect_company_name',
  //           type: 'string',
  //           alias: 'Prospect Company Name',
  //         },
  //         {
  //           name: 'ctype_deal_registration__prospect_company_website',
  //           sql: 'ctype_deal_registration__prospect_company_website',
  //           type: 'string',
  //           alias: 'Prospect Company Website',
  //         },
  //         {
  //           name: 'ctype_deal_registration__prospect_contact_email',
  //           sql: 'ctype_deal_registration__prospect_contact_email',
  //           type: 'string',
  //           alias: 'Prospect Contact Email',
  //         },
  //         {
  //           name: 'ctype_deal_registration__prospect_contact_name',
  //           sql: 'ctype_deal_registration__prospect_contact_name',
  //           type: 'string',
  //           alias: 'Prospect Contact Name',
  //         },
  //         {
  //           name: 'ctype_deal_registration__sub_region',
  //           sql: 'ctype_deal_registration__sub_region',
  //           type: 'string',
  //           alias: 'Region ',
  //         },
  //         {
  //           name: 'ctype_deal_registration__subregion',
  //           sql: 'ctype_deal_registration__subregion',
  //           type: 'string',
  //           alias: 'Subregion',
  //         },
  //         {
  //           name: 'ctype_Events__campaign_category',
  //           sql: 'ctype_Events__campaign_category',
  //           type: 'string',
  //           alias: 'Campaign Category',
  //         },
  //         {
  //           name: 'ctype_Events__event_end_date',
  //           sql: 'ctype_Events__event_end_date',
  //           type: 'time',
  //           alias: 'Event End Date',
  //         },
  //         {
  //           name: 'ctype_Events__event_owner',
  //           sql: 'ctype_Events__event_owner',
  //           type: 'string',
  //           alias: 'Event Owner',
  //         },
  //         {
  //           name: 'ctype_Events__event_start_date',
  //           sql: 'ctype_Events__event_start_date',
  //           type: 'time',
  //           alias: 'Event Start Date',
  //         },
  //         {
  //           name: 'ctype_Events__events_test_ref',
  //           sql: 'ctype_Events__events_test_ref',
  //           type: 'string',
  //           alias: 'events test ref',
  //         },
  //         {
  //           name: 'ctype_Events__external_budget',
  //           sql: 'ctype_Events__external_budget',
  //           type: 'number',
  //           alias: 'External Budget',
  //         },
  //         {
  //           name: 'ctype_Events__internal_budget',
  //           sql: 'ctype_Events__internal_budget',
  //           type: 'number',
  //           alias: 'Internal Budget',
  //         },
  //         {
  //           name: 'ctype_Events__is_devrev_event',
  //           sql: 'ctype_Events__is_devrev_event',
  //           type: 'boolean',
  //           alias: 'Is DevRev Event',
  //         },
  //         {
  //           name: 'ctype_Events__mode',
  //           sql: 'ctype_Events__mode',
  //           type: 'string',
  //           alias: 'Mode',
  //         },
  //         {
  //           name: 'ctype_Events__organized_by',
  //           sql: 'ctype_Events__organized_by',
  //           type: 'string',
  //           alias: 'Organized by',
  //         },
  //         {
  //           name: 'ctype_Events__pipeline_generated',
  //           sql: 'ctype_Events__pipeline_generated',
  //           type: 'number',
  //           alias: 'Pipeline Generated',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_Events__representatives',
  //           sql: 'ctype_Events__representatives',
  //           type: 'string_array',
  //           alias: 'Representatives',
  //         },
  //         {
  //           name: 'ctype_Events__source',
  //           sql: 'ctype_Events__source',
  //           type: 'string',
  //           alias: 'Source',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_Events__sponsors',
  //           sql: 'ctype_Events__sponsors',
  //           type: 'string_array',
  //           alias: 'Sponsors',
  //         },
  //         {
  //           name: 'ctype_Events__sub_source',
  //           sql: 'ctype_Events__sub_source',
  //           type: 'string',
  //           alias: 'Sub-Source',
  //         },
  //         {
  //           name: 'ctype_Events__total_budget',
  //           sql: 'ctype_Events__total_budget',
  //           type: 'number',
  //           alias: 'Total Budget',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__allow_attachments_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__allow_attachments_cfid',
  //           type: 'boolean',
  //           alias: 'allow_attachments',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__allow_channelback_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__allow_channelback_cfid',
  //           type: 'boolean',
  //           alias: 'allow_channelback',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__brand_id_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__brand_id_cfid',
  //           type: 'number',
  //           alias: 'brand_id',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__collaborator_ids',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__collaborator_ids',
  //           type: 'string_array',
  //           alias: 'Collaborators',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__created_at_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__created_at_cfid',
  //           type: 'time',
  //           alias: 'created_at',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__ext_object_type',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__ext_object_type',
  //           type: 'string',
  //           alias: 'External Object Type',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__external_id_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__external_id_cfid',
  //           type: 'string',
  //           alias: 'external_id',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__fields_modified_during_import',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__fields_modified_during_import',
  //           type: 'string_array',
  //           alias: 'Fields modified during import',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__from_messaging_channel_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__from_messaging_channel_cfid',
  //           type: 'boolean',
  //           alias: 'from_messaging_channel',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__generated_timestamp_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__generated_timestamp_cfid',
  //           type: 'time',
  //           alias: 'generated_timestamp',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__group_id_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__group_id_cfid',
  //           type: 'number',
  //           alias: 'group_id',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__has_incidents_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__has_incidents_cfid',
  //           type: 'boolean',
  //           alias: 'has_incidents',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__is_public_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__is_public_cfid',
  //           type: 'boolean',
  //           alias: 'is_public',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__original_reporters',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__original_reporters',
  //           type: 'string_array',
  //           alias: 'Reporters',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__original_rev_org',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__original_rev_org',
  //           type: 'string',
  //           alias: 'Customer',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__raw_subject_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__raw_subject_cfid',
  //           type: 'string',
  //           alias: 'raw_subject',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__recipient_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__recipient_cfid',
  //           type: 'string',
  //           alias: 'recipient',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__ro_source_item',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__ro_source_item',
  //           type: 'string',
  //           alias: 'External Source',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__ticket_form_id_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__ticket_form_id_cfid',
  //           type: 'number',
  //           alias: 'ticket_form_id',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__url_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__url_cfid',
  //           type: 'string',
  //           alias: 'url',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x12840984514707_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x12840984514707_cfid',
  //           type: 'string',
  //           alias: 'Hold Status',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x1500007715801_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x1500007715801_cfid',
  //           type: 'string',
  //           alias: 'ATI Type',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x1500007831982_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x1500007831982_cfid',
  //           type: 'string',
  //           alias: 'Event ID',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x16770809925651_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x16770809925651_cfid',
  //           type: 'boolean',
  //           alias: 'Do Not Close',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x19445665382931_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x19445665382931_cfid',
  //           type: 'boolean',
  //           alias: 'Escalated',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360000203467_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360000203467_cfid',
  //           type: 'string',
  //           alias: 'Partner: Sales Contact',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360000212308_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360000212308_cfid',
  //           type: 'string',
  //           alias: 'Partner Name',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360000220288_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360000220288_cfid',
  //           type: 'string',
  //           alias: 'Env Setup Type',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360000222327_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360000222327_cfid',
  //           type: 'string',
  //           alias: 'Additional Comments',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360000229528_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360000229528_cfid',
  //           type: 'string',
  //           alias: 'Remind me before',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360000245507_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360000245507_cfid',
  //           type: 'string',
  //           alias: ' zIPS + z3A Licenses',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360000245527_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360000245527_cfid',
  //           type: 'string',
  //           alias: 'zDefend Licenses',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360002877873_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360002877873_cfid',
  //           type: 'string',
  //           alias: 'Approval Status',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360003033353_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360003033353_cfid',
  //           type: 'string',
  //           alias: 'Booking Year',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360003070794_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360003070794_cfid',
  //           type: 'string',
  //           alias: 'Term',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360003637633_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360003637633_cfid',
  //           type: 'string',
  //           alias: 'End Customer :  Admin Details',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360004164014_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360004164014_cfid',
  //           type: 'string_array',
  //           alias: 'Root Cause',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360004926333_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360004926333_cfid',
  //           type: 'string',
  //           alias: 'Request Type',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360005403033_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360005403033_cfid',
  //           type: 'string',
  //           alias: 'End User - Full Name',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360005403153_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360005403153_cfid',
  //           type: 'string',
  //           alias: 'End Customer: Administrator Email Address',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360006394633_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360006394633_cfid',
  //           type: 'string',
  //           alias: 'Services Purchased',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360006484754_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360006484754_cfid',
  //           type: 'string',
  //           alias: 'Support Purchased',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360007214193_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360007214193_cfid',
  //           type: 'string',
  //           alias: 'Cancellation Reason',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360007291334_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360007291334_cfid',
  //           type: 'string',
  //           alias: 'Cancellation Date',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360009928013_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360009928013_cfid',
  //           type: 'string',
  //           alias: 'zConsole URL',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360009928193_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360009928193_cfid',
  //           type: 'string',
  //           alias: 'Customer Success Representative',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360009978873_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360009978873_cfid',
  //           type: 'string',
  //           alias: 'VPC Name',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360010017554_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360010017554_cfid',
  //           type: 'string',
  //           alias: 'End Customer: Region',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360011850853_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360011850853_cfid',
  //           type: 'string',
  //           alias: 'Proof of Concept Impact ?',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360011929414_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360011929414_cfid',
  //           type: 'string',
  //           alias: 'Internal Status',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360011934634_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360011934634_cfid',
  //           type: 'string',
  //           alias: 'Problem Sub-Type',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360012820673_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360012820673_cfid',
  //           type: 'string',
  //           alias: 'Tenant Name',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360019252873_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360019252873_cfid',
  //           type: 'string',
  //           alias: 'Customer Name',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360021277473_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360021277473_cfid',
  //           type: 'string',
  //           alias: 'Channel',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360021277493_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360021277493_cfid',
  //           type: 'string',
  //           alias: 'Order Type',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360021289714_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360021289714_cfid',
  //           type: 'string',
  //           alias: 'zIPS Licenses',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360021289734_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360021289734_cfid',
  //           type: 'boolean',
  //           alias: 'z3A License',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360021522734_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360021522734_cfid',
  //           type: 'string',
  //           alias: 'Sub-Partner Name',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360033488674_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360033488674_cfid',
  //           type: 'string_array',
  //           alias: 'Ticket Region',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360036191933_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360036191933_cfid',
  //           type: 'string',
  //           alias: 'zShield Licenses',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360036262514_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360036262514_cfid',
  //           type: 'string',
  //           alias: 'zScan Apps Licensed',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360046636694_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x360046636694_cfid',
  //           type: 'string',
  //           alias: 'KPE MTD Licenses',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x4417555758995_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x4417555758995_cfid',
  //           type: 'string',
  //           alias: 'Product Name',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x4417589587987_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x4417589587987_cfid',
  //           type: 'string',
  //           alias: 'whiteCryption agent',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x4417734769043_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x4417734769043_cfid',
  //           type: 'string',
  //           alias: 'zIPS Test Case Types',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x4619649774611_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x4619649774611_cfid',
  //           type: 'string',
  //           alias: 'Product Version',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80938687_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80938687_cfid',
  //           type: 'string',
  //           alias: 'Target Delivery Date',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80942287_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80942287_cfid',
  //           type: 'string',
  //           alias: 'App Version ',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80949067_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80949067_cfid',
  //           type: 'string',
  //           alias: 'End Customer: Company Name',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80949087_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80949087_cfid',
  //           type: 'string',
  //           alias: 'End Customer: Physical Address',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80949527_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80949527_cfid',
  //           type: 'string',
  //           alias: 'Term Start Date',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80949547_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80949547_cfid',
  //           type: 'string',
  //           alias: 'Term End Date',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80949587_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80949587_cfid',
  //           type: 'string',
  //           alias: 'Order Fulfilled',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80953107_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80953107_cfid',
  //           type: 'string',
  //           alias: 'Requested Delivery Date',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80966647_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80966647_cfid',
  //           type: 'string',
  //           alias: 'Internal Tracking Id',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80971487_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x80971487_cfid',
  //           type: 'string',
  //           alias: 'Feature Status',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x81275168_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x81275168_cfid',
  //           type: 'string',
  //           alias: 'Case Type',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x81315948_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x81315948_cfid',
  //           type: 'string',
  //           alias: 'Business Impact',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x81335888_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x81335888_cfid',
  //           type: 'string',
  //           alias: 'Zimperium Console URL',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x81342748_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x81342748_cfid',
  //           type: 'string',
  //           alias: 'End Customer: Administrator Name',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x81342948_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x81342948_cfid',
  //           type: 'string',
  //           alias: 'zIPS Only Licenses ',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x81343048_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__x81343048_cfid',
  //           type: 'string',
  //           alias: 'Zimperium Sales Contact: Email Address',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__allow_attachments_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__allow_attachments_cfid',
  //           type: 'boolean',
  //           alias: 'allow_attachments',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__allow_channelback_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__allow_channelback_cfid',
  //           type: 'boolean',
  //           alias: 'allow_channelback',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__brand_id_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__brand_id_cfid',
  //           type: 'number',
  //           alias: 'brand_id',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__collaborator_ids',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__collaborator_ids',
  //           type: 'string_array',
  //           alias: 'Collaborators',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__created_at_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__created_at_cfid',
  //           type: 'time',
  //           alias: 'created_at',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__ext_object_type',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__ext_object_type',
  //           type: 'string',
  //           alias: 'External Object Type',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__fields_modified_during_import',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__fields_modified_during_import',
  //           type: 'string_array',
  //           alias: 'Fields modified during import',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__from_messaging_channel_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__from_messaging_channel_cfid',
  //           type: 'boolean',
  //           alias: 'from_messaging_channel',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__generated_timestamp_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__generated_timestamp_cfid',
  //           type: 'time',
  //           alias: 'generated_timestamp',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__group_id_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__group_id_cfid',
  //           type: 'number',
  //           alias: 'group_id',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__has_incidents_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__has_incidents_cfid',
  //           type: 'boolean',
  //           alias: 'has_incidents',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__is_public_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__is_public_cfid',
  //           type: 'boolean',
  //           alias: 'is_public',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__original_reporters',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__original_reporters',
  //           type: 'string_array',
  //           alias: 'Reporters',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__original_rev_org',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__original_rev_org',
  //           type: 'string',
  //           alias: 'Customer',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__raw_subject_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__raw_subject_cfid',
  //           type: 'string',
  //           alias: 'raw_subject',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__recipient_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__recipient_cfid',
  //           type: 'string',
  //           alias: 'recipient',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__ro_source_item',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__ro_source_item',
  //           type: 'string',
  //           alias: 'External Source',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__ticket_form_id_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__ticket_form_id_cfid',
  //           type: 'number',
  //           alias: 'ticket_form_id',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__url_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__url_cfid',
  //           type: 'string',
  //           alias: 'url',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x12840984514707_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x12840984514707_cfid',
  //           type: 'string',
  //           alias: 'Hold Status',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x16770809925651_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x16770809925651_cfid',
  //           type: 'boolean',
  //           alias: 'Do Not Close',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x19445665382931_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x19445665382931_cfid',
  //           type: 'boolean',
  //           alias: 'Escalated',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360000203467_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360000203467_cfid',
  //           type: 'string',
  //           alias: 'Partner: Sales Contact',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360000212308_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360000212308_cfid',
  //           type: 'string',
  //           alias: 'Partner Name',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360000220288_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360000220288_cfid',
  //           type: 'string',
  //           alias: 'Env Setup Type',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360002877873_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360002877873_cfid',
  //           type: 'string',
  //           alias: 'Approval Status',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360003033353_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360003033353_cfid',
  //           type: 'string',
  //           alias: 'Booking Year',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360004164014_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360004164014_cfid',
  //           type: 'string_array',
  //           alias: 'Root Cause',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360006394633_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360006394633_cfid',
  //           type: 'string',
  //           alias: 'Services Purchased',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360006484754_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360006484754_cfid',
  //           type: 'string',
  //           alias: 'Support Purchased',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360009978873_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360009978873_cfid',
  //           type: 'string',
  //           alias: 'VPC Name',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360010017554_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360010017554_cfid',
  //           type: 'string',
  //           alias: 'End Customer: Region',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360011850853_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360011850853_cfid',
  //           type: 'string',
  //           alias: 'Proof of Concept Impact ?',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360011934634_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360011934634_cfid',
  //           type: 'string',
  //           alias: 'Problem Sub-Type',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360012820673_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360012820673_cfid',
  //           type: 'string',
  //           alias: 'Tenant Name',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360019252873_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360019252873_cfid',
  //           type: 'string',
  //           alias: 'Customer Name',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360021277473_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360021277473_cfid',
  //           type: 'string',
  //           alias: 'Channel',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360021277493_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360021277493_cfid',
  //           type: 'string',
  //           alias: 'Order Type',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360021289714_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360021289714_cfid',
  //           type: 'string',
  //           alias: 'zIPS Licenses',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360021289734_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360021289734_cfid',
  //           type: 'boolean',
  //           alias: 'z3A License',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360021522734_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360021522734_cfid',
  //           type: 'string',
  //           alias: 'Sub-Partner Name',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360033488674_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x360033488674_cfid',
  //           type: 'string_array',
  //           alias: 'Ticket Region',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x4417555758995_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x4417555758995_cfid',
  //           type: 'string',
  //           alias: 'Product Name',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x4417589587987_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x4417589587987_cfid',
  //           type: 'string',
  //           alias: 'whiteCryption agent',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x4619649774611_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x4619649774611_cfid',
  //           type: 'string',
  //           alias: 'Product Version',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80942287_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80942287_cfid',
  //           type: 'string',
  //           alias: 'App Version ',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80949067_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80949067_cfid',
  //           type: 'string',
  //           alias: 'End Customer: Company Name',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80949087_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80949087_cfid',
  //           type: 'string',
  //           alias: 'End Customer: Physical Address',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80949527_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80949527_cfid',
  //           type: 'string',
  //           alias: 'Term Start Date',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80949547_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80949547_cfid',
  //           type: 'string',
  //           alias: 'Term End Date',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80949587_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80949587_cfid',
  //           type: 'string',
  //           alias: 'Order Fulfilled',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80966647_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80966647_cfid',
  //           type: 'string',
  //           alias: 'Internal Tracking Id',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80971487_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x80971487_cfid',
  //           type: 'string',
  //           alias: 'Feature Status',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x81275168_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x81275168_cfid',
  //           type: 'string',
  //           alias: 'Case Type',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x81315948_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x81315948_cfid',
  //           type: 'string',
  //           alias: 'Business Impact',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x81335888_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__x81335888_cfid',
  //           type: 'string',
  //           alias: 'DuplicateField Console URL',
  //         },
  //         {
  //           name: 'ctype_hell__test',
  //           sql: 'ctype_hell__test',
  //           type: 'string',
  //           alias: 'test',
  //         },
  //         {
  //           name: 'ctype_hell__text_default_field',
  //           sql: 'ctype_hell__text_default_field',
  //           type: 'string',
  //           alias: 'text default field',
  //         },
  //         {
  //           name: 'ctype_clone_ticket___clone_field_dropdown',
  //           sql: 'ctype_clone_ticket___clone_field_dropdown',
  //           type: 'string',
  //           alias: 'clone field dropdown',
  //         },
  //         {
  //           name: 'ctype_test_subtype_sep__new_field_2',
  //           sql: 'ctype_test_subtype_sep__new_field_2',
  //           type: 'string',
  //           alias: 'field_harsh',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_test_subtype_sep__new_field_id_list_3',
  //           sql: 'ctype_test_subtype_sep__new_field_id_list_3',
  //           type: 'string_array',
  //           alias: 'field_id_list_harsh',
  //         },
  //         {
  //           name: 'ctype_vgtestyesno__booltestyesno',
  //           sql: 'ctype_vgtestyesno__booltestyesno',
  //           type: 'boolean',
  //           alias: 'booltestyesno',
  //         },
  //         {
  //           name: 'ctype_asd__test_asd',
  //           sql: 'ctype_asd__test_asd',
  //           type: 'string',
  //           alias: 'test asd',
  //         },
  //         {
  //           name: 'ctype_Microservice__git_template_name',
  //           sql: 'ctype_Microservice__git_template_name',
  //           type: 'string',
  //           alias: 'Github Template Name',
  //         },
  //         {
  //           name: 'ctype_Microservice__micro_service_name',
  //           sql: 'ctype_Microservice__micro_service_name',
  //           type: 'string',
  //           alias: 'Microservice Name',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_Microservice__platform',
  //           sql: 'ctype_Microservice__platform',
  //           type: 'string_array',
  //           alias: 'Platform',
  //         },
  //         {
  //           name: 'ctype_Microservice__read_only',
  //           sql: 'ctype_Microservice__read_only',
  //           type: 'string',
  //           alias: 'read only',
  //         },
  //         {
  //           name: 'ctype_helohelohelohelohelo__dfkdkl',
  //           sql: 'ctype_helohelohelohelohelo__dfkdkl',
  //           type: 'number',
  //           alias: 'dfkdkl',
  //         },
  //         {
  //           name: 'ctype_helohelohelohelohelo__hello_1122',
  //           sql: 'ctype_helohelohelohelohelo__hello_1122',
  //           type: 'string',
  //           alias: 'hello hello folks',
  //         },
  //         {
  //           name: 'ctype_helohelohelohelohelo__hello_12',
  //           sql: 'ctype_helohelohelohelohelo__hello_12',
  //           type: 'string',
  //           alias: 'hello_12',
  //         },
  //         {
  //           name: 'ctype_helohelohelohelohelo__hello_123',
  //           sql: 'ctype_helohelohelohelohelo__hello_123',
  //           type: 'string',
  //           alias: 'hello_123',
  //         },
  //         {
  //           name: 'ctype_helohelohelohelohelo__hello_12x',
  //           sql: 'ctype_helohelohelohelohelo__hello_12x',
  //           type: 'string',
  //           alias: 'hello_12x',
  //         },
  //         {
  //           name: 'ctype_helohelohelohelohelo__hello_12y',
  //           sql: 'ctype_helohelohelohelohelo__hello_12y',
  //           type: 'string',
  //           alias: 'hello_12y',
  //         },
  //         {
  //           name: 'ctype_helohelohelohelohelo__hello_wed',
  //           sql: 'ctype_helohelohelohelohelo__hello_wed',
  //           type: 'string',
  //           alias: 'hello_wed',
  //         },
  //         {
  //           name: 'ctype_helohelohelohelohelo__hello_world',
  //           sql: 'ctype_helohelohelohelohelo__hello_world',
  //           type: 'string',
  //           alias: 'hello_world',
  //         },
  //         {
  //           name: 'ctype_dddddddd__dkay_march_23',
  //           sql: 'ctype_dddddddd__dkay_march_23',
  //           type: 'string',
  //           alias: 'dkay_march_23',
  //         },
  //         {
  //           name: 'ctype_dddddddd__dkay_march_26',
  //           sql: 'ctype_dddddddd__dkay_march_26',
  //           type: 'string',
  //           alias: 'dkay_march_26',
  //         },
  //         {
  //           name: 'ctype_dddddddd__scope_approval',
  //           sql: 'ctype_dddddddd__scope_approval',
  //           type: 'string',
  //           alias: 'Scope Approval',
  //         },
  //         {
  //           name: 'ctype_dddddddd__uat_approval',
  //           sql: 'ctype_dddddddd__uat_approval',
  //           type: 'string',
  //           alias: 'UAT approval',
  //         },
  //         {
  //           name: 'ctype_defaults__customer_uat',
  //           sql: 'ctype_defaults__customer_uat',
  //           type: 'time',
  //           alias: 'customer uat',
  //         },
  //         {
  //           name: 'ctype_defaults__default',
  //           sql: 'ctype_defaults__default',
  //           type: 'string',
  //           alias: 'default',
  //         },
  //         {
  //           name: 'ctype_defaults__default_enum',
  //           sql: 'ctype_defaults__default_enum',
  //           type: 'string',
  //           alias: 'default enum',
  //         },
  //         {
  //           name: 'ctype_defaults__enum_testing',
  //           sql: 'ctype_defaults__enum_testing',
  //           type: 'string',
  //           alias: 'enum testing',
  //         },
  //         {
  //           name: 'ctype_defaults__uat_date',
  //           sql: 'ctype_defaults__uat_date',
  //           type: 'time',
  //           alias: 'uat date',
  //         },
  //         {
  //           name: 'ctype_bug__abc',
  //           sql: 'ctype_bug__abc',
  //           type: 'string',
  //           alias: 'abc',
  //         },
  //         {
  //           name: 'ctype_bug__dependent_test',
  //           sql: 'ctype_bug__dependent_test',
  //           type: 'string',
  //           alias: 'something',
  //         },
  //         {
  //           name: 'ctype_event_request__approver',
  //           sql: 'ctype_event_request__approver',
  //           type: 'string',
  //           alias: 'Approver',
  //         },
  //         {
  //           name: 'ctype_event_request__budget',
  //           sql: 'ctype_event_request__budget',
  //           type: 'number',
  //           alias: 'Budget',
  //         },
  //         {
  //           name: 'ctype_event_request__event_date',
  //           sql: 'ctype_event_request__event_date',
  //           type: 'time',
  //           alias: 'Event Date',
  //         },
  //         {
  //           name: 'ctype_event_request__event_location',
  //           sql: 'ctype_event_request__event_location',
  //           type: 'string',
  //           alias: 'Event Location',
  //         },
  //         {
  //           name: 'ctype_event_request__event_name',
  //           sql: 'ctype_event_request__event_name',
  //           type: 'string',
  //           alias: 'Event Name',
  //         },
  //         {
  //           name: 'ctype_event_request__event_owner',
  //           sql: 'ctype_event_request__event_owner',
  //           type: 'string',
  //           alias: 'Event Owner',
  //         },
  //         {
  //           name: 'ctype_event_request__event_type',
  //           sql: 'ctype_event_request__event_type',
  //           type: 'string',
  //           alias: 'Event Type',
  //         },
  //         {
  //           name: 'ctype_event_request__link_to_event',
  //           sql: 'ctype_event_request__link_to_event',
  //           type: 'string',
  //           alias: 'Link To Event',
  //         },
  //         {
  //           name: 'ctype_event_request__region',
  //           sql: 'ctype_event_request__region',
  //           type: 'string',
  //           alias: 'Region',
  //         },
  //         {
  //           name: 'ctype_event_request__requested_by',
  //           sql: 'ctype_event_request__requested_by',
  //           type: 'string',
  //           alias: 'Requested by',
  //         },
  //         {
  //           name: 'ctype_event_request__status',
  //           sql: 'ctype_event_request__status',
  //           type: 'string',
  //           alias: 'Status',
  //         },
  //         {
  //           name: 'ctype_event_request__target_pipeline',
  //           sql: 'ctype_event_request__target_pipeline',
  //           type: 'number',
  //           alias: 'Target Pipeline',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_event_request__target_segment',
  //           sql: 'ctype_event_request__target_segment',
  //           type: 'string_array',
  //           alias: 'Target Segment',
  //         },
  //         {
  //           name: 'ctype_mfz_subtype_check__enum_field',
  //           sql: 'ctype_mfz_subtype_check__enum_field',
  //           type: 'string',
  //           alias: 'Custom Enum field',
  //         },
  //         {
  //           name: 'ctype_mfz_subtype_check__text_field',
  //           sql: 'ctype_mfz_subtype_check__text_field',
  //           type: 'string',
  //           alias: 'Custom text field',
  //         },
  //         {
  //           name: 'ctype_mfz_subtype_check__user_field',
  //           sql: 'ctype_mfz_subtype_check__user_field',
  //           type: 'string',
  //           alias: 'Custom user field',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_mfz_subtype_check__user_list_field',
  //           sql: 'ctype_mfz_subtype_check__user_list_field',
  //           type: 'string_array',
  //           alias: 'Custom user list field',
  //         },
  //         {
  //           name: 'ctype_mfz_subtype_check__workspace_field',
  //           sql: 'ctype_mfz_subtype_check__workspace_field',
  //           type: 'string',
  //           alias: 'Custom workspace field',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_mfz_subtype_check__workspace_list_field',
  //           sql: 'ctype_mfz_subtype_check__workspace_list_field',
  //           type: 'string_array',
  //           alias: 'Custom workspace list field',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__allow_attachments_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__allow_attachments_cfid',
  //           type: 'boolean',
  //           alias: 'allow_attachments',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__allow_channelback_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__allow_channelback_cfid',
  //           type: 'boolean',
  //           alias: 'allow_channelback',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__brand_id_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__brand_id_cfid',
  //           type: 'number',
  //           alias: 'brand_id',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__collaborator_ids',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__collaborator_ids',
  //           type: 'string_array',
  //           alias: 'Collaborators',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__created_at_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__created_at_cfid',
  //           type: 'time',
  //           alias: 'created_at',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__ext_object_type',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__ext_object_type',
  //           type: 'string',
  //           alias: 'External Object Type',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__fields_modified_during_import',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__fields_modified_during_import',
  //           type: 'string_array',
  //           alias: 'Fields modified during import',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__from_messaging_channel_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__from_messaging_channel_cfid',
  //           type: 'boolean',
  //           alias: 'from_messaging_channel',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__generated_timestamp_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__generated_timestamp_cfid',
  //           type: 'time',
  //           alias: 'generated_timestamp',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__group_id_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__group_id_cfid',
  //           type: 'number',
  //           alias: 'group_id',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__has_incidents_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__has_incidents_cfid',
  //           type: 'boolean',
  //           alias: 'has_incidents',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__is_public_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__is_public_cfid',
  //           type: 'boolean',
  //           alias: 'is_public',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__original_reporters',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__original_reporters',
  //           type: 'string_array',
  //           alias: 'Reporters',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__original_rev_org',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__original_rev_org',
  //           type: 'string',
  //           alias: 'Customer',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__raw_subject_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__raw_subject_cfid',
  //           type: 'string',
  //           alias: 'raw_subject',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__recipient_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__recipient_cfid',
  //           type: 'string',
  //           alias: 'recipient',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__ro_source_item',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__ro_source_item',
  //           type: 'string',
  //           alias: 'External Source',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__ticket_form_id_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__ticket_form_id_cfid',
  //           type: 'number',
  //           alias: 'ticket_form_id',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__url_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__url_cfid',
  //           type: 'string',
  //           alias: 'url',
  //         },
  //         {
  //           modifier: {
  //             shouldUnnestGroupBy: false,
  //           },
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__x360033488674_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__x360033488674_cfid',
  //           type: 'string_array',
  //           alias: 'Ticket Region',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__x4417589587987_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__x4417589587987_cfid',
  //           type: 'string',
  //           alias: 'whiteCryption agent',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__x80971487_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__x80971487_cfid',
  //           type: 'string',
  //           alias: 'Feature Status',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__x81275168_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__x81275168_cfid',
  //           type: 'string',
  //           alias: 'Case Type',
  //         },
  //         {
  //           name: 'ctype_mandatory_field_subtype__some_text_field',
  //           sql: 'ctype_mandatory_field_subtype__some_text_field',
  //           type: 'string',
  //           alias: 'Some text field',
  //         },
  //         {
  //           name: 'ctype_review_subtype__date',
  //           sql: 'ctype_review_subtype__date',
  //           type: 'time',
  //           alias: 'Date',
  //         },
  //         {
  //           name: 'ctype_review_subtype__play_store_review_url',
  //           sql: 'ctype_review_subtype__play_store_review_url',
  //           type: 'string',
  //           alias: 'Review URL',
  //         },
  //         {
  //           name: 'ctype_review_subtype__rating',
  //           sql: 'ctype_review_subtype__rating',
  //           type: 'string',
  //           alias: 'Rating',
  //         },
  //         {
  //           name: 'ctype_review_subtype__reply_date',
  //           sql: 'ctype_review_subtype__reply_date',
  //           type: 'time',
  //           alias: 'Reply date',
  //         },
  //         {
  //           name: 'ctype_review_subtype__reply_text',
  //           sql: 'ctype_review_subtype__reply_text',
  //           type: 'string',
  //           alias: 'Reply text',
  //         },
  //         {
  //           name: 'ctype_review_subtype__upvote_count',
  //           sql: 'ctype_review_subtype__upvote_count',
  //           type: 'number',
  //           alias: 'Upvote Count',
  //         },
  //         {
  //           name: 'ctype_jkjkjkjkjkjk__dgg',
  //           sql: 'ctype_jkjkjkjkjkjk__dgg',
  //           type: 'string',
  //           alias: 'dgg',
  //         },
  //         {
  //           name: 'ctype_jkjkjkjkjkjk__dggd',
  //           sql: 'ctype_jkjkjkjkjkjk__dggd',
  //           type: 'number',
  //           alias: 'dggd',
  //         },
  //         {
  //           name: 'ctype_jkjkjkjkjkjk__hello',
  //           sql: 'ctype_jkjkjkjkjkjk__hello',
  //           type: 'string',
  //           alias: 'hello',
  //         },
  //         {
  //           name: 'ctype_jkjkjkjkjkjk__hello_acl',
  //           sql: 'ctype_jkjkjkjkjkjk__hello_acl',
  //           type: 'string',
  //           alias: 'hellO_acl',
  //         },
  //         {
  //           name: 'ctype_huryyyyyyyyyyy__all_internal_selected',
  //           sql: 'ctype_huryyyyyyyyyyy__all_internal_selected',
  //           type: 'string',
  //           alias: 'all_external_selected',
  //         },
  //         {
  //           name: 'ctype_huryyyyyyyyyyy__all_selected',
  //           sql: 'ctype_huryyyyyyyyyyy__all_selected',
  //           type: 'string',
  //           alias: 'all_selected',
  //         },
  //         {
  //           name: 'ctype_huryyyyyyyyyyy__test_acl_2',
  //           sql: 'ctype_huryyyyyyyyyyy__test_acl_2',
  //           type: 'string',
  //           alias: 'test_acl_2',
  //         },
  //         {
  //           name: 'ctype_aaa__fkjdsfjkdj',
  //           sql: 'ctype_aaa__fkjdsfjkdj',
  //           type: 'string',
  //           alias: 'fkjdsfjkdj',
  //         },
  //         {
  //           name: 'ctype_aaa__test_acl_march',
  //           sql: 'ctype_aaa__test_acl_march',
  //           type: 'string',
  //           alias: 'test_acl_march',
  //         },
  //         {
  //           name: 'ctype_lolololololol__hell_acl_2',
  //           sql: 'ctype_lolololololol__hell_acl_2',
  //           type: 'string',
  //           alias: 'hell_acl_2',
  //         },
  //         {
  //           name: 'ctype_lolololololol__hello_acl',
  //           sql: 'ctype_lolololololol__hello_acl',
  //           type: 'string',
  //           alias: 'hello_acl',
  //         },
  //         {
  //           name: 'ctype_lolololololol__hello_acl_3',
  //           sql: 'ctype_lolololololol__hello_acl_3',
  //           type: 'string',
  //           alias: 'hello_acl_3',
  //         },
  //         {
  //           name: 'ctype_zzz__dds',
  //           sql: 'ctype_zzz__dds',
  //           type: 'string',
  //           alias: 'DDS',
  //         },
  //         {
  //           name: 'ctype_zzz__hello',
  //           sql: 'ctype_zzz__hello',
  //           type: 'string',
  //           alias: 'hello ',
  //         },
  //         {
  //           name: 'ctype_zzz__hello_2',
  //           sql: 'ctype_zzz__hello_2',
  //           type: 'string',
  //           alias: 'hello_2',
  //         },
  //         {
  //           name: 'ctype_zzz__hello_3',
  //           sql: 'ctype_zzz__hello_3',
  //           type: 'string',
  //           alias: 'hello_3',
  //         },
  //         {
  //           name: 'ctype_zzz__hello_4',
  //           sql: 'ctype_zzz__hello_4',
  //           type: 'string',
  //           alias: 'hello_4',
  //         },
  //         {
  //           name: 'ctype_zzz__sdkjdsfjk',
  //           sql: 'ctype_zzz__sdkjdsfjk',
  //           type: 'string',
  //           alias: 'SDKJDSFJK',
  //         },
  //         {
  //           name: 'ctype_ggg__dkay_march_1234',
  //           sql: 'ctype_ggg__dkay_march_1234',
  //           type: 'string',
  //           alias: 'dkay_march_1234',
  //         },
  //         {
  //           name: 'ctype_ggg__dkay_march_2',
  //           sql: 'ctype_ggg__dkay_march_2',
  //           type: 'string',
  //           alias: 'dkay_march_2',
  //         },
  //         {
  //           name: 'ctype_ggg__dkay_test',
  //           sql: 'ctype_ggg__dkay_test',
  //           type: 'string',
  //           alias: 'dkay_test',
  //         },
  //         {
  //           name: 'ctype_ooooo__dkay_march_3',
  //           sql: 'ctype_ooooo__dkay_march_3',
  //           type: 'string',
  //           alias: 'dkay_march_3',
  //         },
  //         {
  //           name: 'ctype_ooooo__dkay_march_4',
  //           sql: 'ctype_ooooo__dkay_march_4',
  //           type: 'string',
  //           alias: 'dkay_march_4',
  //         },
  //         {
  //           name: 'ctype_ooooo__dkay_march_5',
  //           sql: 'ctype_ooooo__dkay_march_5',
  //           type: 'string',
  //           alias: 'dkay_march_5',
  //         },
  //         {
  //           name: 'ctype_ooooo__dkay_march_7',
  //           sql: 'ctype_ooooo__dkay_march_7',
  //           type: 'string',
  //           alias: 'dkay_march_7',
  //         },
  //         {
  //           name: 'ctype_sync_engine_test__field1',
  //           sql: 'ctype_sync_engine_test__field1',
  //           type: 'string',
  //           alias: 'field1',
  //         },
  //         {
  //           name: 'ctype_sync_engine_test__field2_bool',
  //           sql: 'ctype_sync_engine_test__field2_bool',
  //           type: 'boolean',
  //           alias: 'field2_bool',
  //         },
  //         {
  //           name: 'ctype_priviliges_testing__read_only_field',
  //           sql: 'ctype_priviliges_testing__read_only_field',
  //           type: 'string',
  //           alias: 'read only field',
  //         },
  //         {
  //           name: 'ctype_priviliges_testing__read_write_field',
  //           sql: 'ctype_priviliges_testing__read_write_field',
  //           type: 'string',
  //           alias: 'read/write field',
  //         },
  //         {
  //           name: 'ctype_dashboard_test__pulkit',
  //           sql: 'ctype_dashboard_test__pulkit',
  //           type: 'boolean',
  //           alias: 'pulkit',
  //         },
  //         {
  //           name: 'ctype_tttt__dropdown',
  //           sql: 'ctype_tttt__dropdown',
  //           type: 'string',
  //           alias: 'dropdown',
  //         },
  //       ],
  //       joins: [
  //         {
  //           sql: 'dim_ticket.sla_tracker_id = dim_sla_tracker.id',
  //         },
  //         {
  //           sql: 'dim_ticket.id = dim_survey_response.object',
  //         },
  //         {
  //           sql: 'dim_ticket.id = dim_link_issue_target.source_id',
  //         },
  //         {
  //           sql: 'dim_ticket.id = dim_link_conversation_source.target_id',
  //         },
  //         {
  //           sql: 'dim_ticket.rev_oid = dim_revo.id',
  //         },
  //         {
  //           sql: 'dim_ticket.applies_to_part_id = dim_part.id',
  //         },
  //       ],
  //       measures: [
  //         {
  //           name: 'id_count',
  //           function: {
  //             type: 'count',
  //           },
  //           sql: 'count(dim_ticket.id)',
  //           type: 'string',
  //           alias: 'Ticket Id',
  //         },
  //         {
  //           name: 'created_date_max',
  //           function: {
  //             type: 'max',
  //           },
  //           sql: 'max(dim_ticket.created_date)',
  //           type: 'time',
  //           alias: 'Created date',
  //         },
  //         {
  //           name: 'actual_close_date_max',
  //           function: {
  //             type: 'max',
  //           },
  //           sql: 'max(dim_ticket.actual_close_date)',
  //           type: 'time',
  //           alias: 'Closed date',
  //         },
  //         {
  //           name: 'sla_tracker_id_count',
  //           function: {
  //             type: 'count',
  //           },
  //           sql: 'count(dim_ticket.sla_tracker_id)',
  //           type: 'string',
  //           alias: 'Sla Tracker Id',
  //         },
  //         {
  //           name: 'resolution_time',
  //           sql: "case WHEN actual_close_date > created_date THEN date_diff('minutes', created_date, actual_close_date) ELSE null END ",
  //           type: 'number',
  //           alias: 'Resolution Time',
  //         },
  //         {
  //           name: 'surveys_aggregation_json_measure',
  //           function: {
  //             type: 'median',
  //           },
  //           sql: "median(list_aggregate(CAST(json_extract_string(dim_ticket.surveys_aggregation_json, '$[*].minimum') AS integer[]), 'min'))",
  //           type: 'number',
  //           alias: 'CSAT Rating',
  //         },
  //         {
  //           name: 'tnt__account_id',
  //           sql: 'tnt__account_id',
  //           type: 'string',
  //           alias: 'account id',
  //         },
  //         {
  //           name: 'tnt__actual_effort_spent',
  //           sql: 'tnt__actual_effort_spent',
  //           type: 'number',
  //           alias: 'Actual Effort Spent',
  //         },
  //         {
  //           name: 'tnt__capability_part',
  //           sql: 'tnt__capability_part',
  //           type: 'string',
  //           alias: 'Capability part',
  //         },
  //         {
  //           name: 'tnt__custom_id_field',
  //           sql: 'tnt__custom_id_field',
  //           type: 'string',
  //           alias: 'custom id field',
  //         },
  //         {
  //           name: 'tnt__date_field',
  //           sql: 'tnt__date_field',
  //           type: 'time',
  //           alias: 'date_field',
  //         },
  //         {
  //           name: 'tnt__estimated_effort',
  //           sql: 'tnt__estimated_effort',
  //           type: 'number',
  //           alias: 'Estimated Effort',
  //         },
  //         {
  //           name: 'tnt__fruit',
  //           sql: 'tnt__fruit',
  //           type: 'string',
  //           alias: 'Fruit',
  //         },
  //         {
  //           name: 'tnt__id_field',
  //           sql: 'tnt__id_field',
  //           type: 'string',
  //           alias: 'id field',
  //         },
  //         {
  //           name: 'tnt__issue_score',
  //           sql: 'tnt__issue_score',
  //           type: 'number',
  //           alias: 'Issue Score',
  //         },
  //         {
  //           name: 'tnt__numeric_field',
  //           sql: 'tnt__numeric_field',
  //           type: 'number',
  //           alias: 'numeric field',
  //         },
  //         {
  //           name: 'tnt__parent_part',
  //           sql: 'tnt__parent_part',
  //           type: 'string',
  //           alias: 'Parent part',
  //         },
  //         {
  //           name: 'tnt__part_product',
  //           sql: 'tnt__part_product',
  //           type: 'string',
  //           alias: 'part product',
  //         },
  //         {
  //           name: 'tnt__rank',
  //           sql: 'tnt__rank',
  //           type: 'number',
  //           alias: 'Rank',
  //         },
  //         {
  //           name: 'tnt__remaining_effort',
  //           sql: 'tnt__remaining_effort',
  //           type: 'number',
  //           alias: 'Remaining Effort',
  //         },
  //         {
  //           name: 'tnt__stray_user',
  //           sql: 'tnt__stray_user',
  //           type: 'string',
  //           alias: 'stray user',
  //         },
  //         {
  //           name: 'tnt__stray_users',
  //           sql: 'tnt__stray_users',
  //           type: 'string',
  //           alias: 'stray users',
  //         },
  //         {
  //           name: 'tnt__test',
  //           sql: 'tnt__test',
  //           type: 'number',
  //           alias: 'Test',
  //         },
  //         {
  //           name: 'tnt__test_capability_part',
  //           sql: 'tnt__test_capability_part',
  //           type: 'string',
  //           alias: 'Test Capability Part',
  //         },
  //         {
  //           name: 'tnt__test_product_part',
  //           sql: 'tnt__test_product_part',
  //           type: 'string',
  //           alias: 'Test Product part',
  //         },
  //         {
  //           name: 'tnt__ticket_custom_part',
  //           sql: 'tnt__ticket_custom_part',
  //           type: 'string',
  //           alias: 'ticket custom part',
  //         },
  //         {
  //           name: 'tnt__workspace_custom',
  //           sql: 'tnt__workspace_custom',
  //           type: 'string',
  //           alias: 'workspace_custom',
  //         },
  //         {
  //           name: 'ctype_deal_registration__estimated_deal_value',
  //           sql: 'ctype_deal_registration__estimated_deal_value',
  //           type: 'number',
  //           alias: 'Estimated Deal Value',
  //         },
  //         {
  //           name: 'ctype_deal_registration__expected_close_date',
  //           sql: 'ctype_deal_registration__expected_close_date',
  //           type: 'time',
  //           alias: 'Expected Close Date',
  //         },
  //         {
  //           name: 'ctype_Events__event_end_date',
  //           sql: 'ctype_Events__event_end_date',
  //           type: 'time',
  //           alias: 'Event End Date',
  //         },
  //         {
  //           name: 'ctype_Events__event_owner',
  //           sql: 'ctype_Events__event_owner',
  //           type: 'string',
  //           alias: 'Event Owner',
  //         },
  //         {
  //           name: 'ctype_Events__event_start_date',
  //           sql: 'ctype_Events__event_start_date',
  //           type: 'time',
  //           alias: 'Event Start Date',
  //         },
  //         {
  //           name: 'ctype_Events__events_test_ref',
  //           sql: 'ctype_Events__events_test_ref',
  //           type: 'string',
  //           alias: 'events test ref',
  //         },
  //         {
  //           name: 'ctype_Events__external_budget',
  //           sql: 'ctype_Events__external_budget',
  //           type: 'number',
  //           alias: 'External Budget',
  //         },
  //         {
  //           name: 'ctype_Events__internal_budget',
  //           sql: 'ctype_Events__internal_budget',
  //           type: 'number',
  //           alias: 'Internal Budget',
  //         },
  //         {
  //           name: 'ctype_Events__pipeline_generated',
  //           sql: 'ctype_Events__pipeline_generated',
  //           type: 'number',
  //           alias: 'Pipeline Generated',
  //         },
  //         {
  //           name: 'ctype_Events__total_budget',
  //           sql: 'ctype_Events__total_budget',
  //           type: 'number',
  //           alias: 'Total Budget',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__brand_id_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__brand_id_cfid',
  //           type: 'number',
  //           alias: 'brand_id',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__created_at_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__created_at_cfid',
  //           type: 'time',
  //           alias: 'created_at',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__generated_timestamp_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__generated_timestamp_cfid',
  //           type: 'time',
  //           alias: 'generated_timestamp',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__group_id_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__group_id_cfid',
  //           type: 'number',
  //           alias: 'group_id',
  //         },
  //         {
  //           name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__ticket_form_id_cfid',
  //           sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__ticket_form_id_cfid',
  //           type: 'number',
  //           alias: 'ticket_form_id',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__brand_id_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__brand_id_cfid',
  //           type: 'number',
  //           alias: 'brand_id',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__created_at_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__created_at_cfid',
  //           type: 'time',
  //           alias: 'created_at',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__generated_timestamp_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__generated_timestamp_cfid',
  //           type: 'time',
  //           alias: 'generated_timestamp',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__group_id_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__group_id_cfid',
  //           type: 'number',
  //           alias: 'group_id',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__ticket_form_id_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__ticket_form_id_cfid',
  //           type: 'number',
  //           alias: 'ticket_form_id',
  //         },
  //         {
  //           name: 'ctype_helohelohelohelohelo__dfkdkl',
  //           sql: 'ctype_helohelohelohelohelo__dfkdkl',
  //           type: 'number',
  //           alias: 'dfkdkl',
  //         },
  //         {
  //           name: 'ctype_defaults__customer_uat',
  //           sql: 'ctype_defaults__customer_uat',
  //           type: 'time',
  //           alias: 'customer uat',
  //         },
  //         {
  //           name: 'ctype_defaults__uat_date',
  //           sql: 'ctype_defaults__uat_date',
  //           type: 'time',
  //           alias: 'uat date',
  //         },
  //         {
  //           name: 'ctype_event_request__approver',
  //           sql: 'ctype_event_request__approver',
  //           type: 'string',
  //           alias: 'Approver',
  //         },
  //         {
  //           name: 'ctype_event_request__budget',
  //           sql: 'ctype_event_request__budget',
  //           type: 'number',
  //           alias: 'Budget',
  //         },
  //         {
  //           name: 'ctype_event_request__event_date',
  //           sql: 'ctype_event_request__event_date',
  //           type: 'time',
  //           alias: 'Event Date',
  //         },
  //         {
  //           name: 'ctype_event_request__event_owner',
  //           sql: 'ctype_event_request__event_owner',
  //           type: 'string',
  //           alias: 'Event Owner',
  //         },
  //         {
  //           name: 'ctype_event_request__requested_by',
  //           sql: 'ctype_event_request__requested_by',
  //           type: 'string',
  //           alias: 'Requested by',
  //         },
  //         {
  //           name: 'ctype_event_request__target_pipeline',
  //           sql: 'ctype_event_request__target_pipeline',
  //           type: 'number',
  //           alias: 'Target Pipeline',
  //         },
  //         {
  //           name: 'ctype_mfz_subtype_check__user_field',
  //           sql: 'ctype_mfz_subtype_check__user_field',
  //           type: 'string',
  //           alias: 'Custom user field',
  //         },
  //         {
  //           name: 'ctype_mfz_subtype_check__workspace_field',
  //           sql: 'ctype_mfz_subtype_check__workspace_field',
  //           type: 'string',
  //           alias: 'Custom workspace field',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__brand_id_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__brand_id_cfid',
  //           type: 'number',
  //           alias: 'brand_id',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__created_at_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__created_at_cfid',
  //           type: 'time',
  //           alias: 'created_at',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__generated_timestamp_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__generated_timestamp_cfid',
  //           type: 'time',
  //           alias: 'generated_timestamp',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__group_id_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__group_id_cfid',
  //           type: 'number',
  //           alias: 'group_id',
  //         },
  //         {
  //           name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__ticket_form_id_cfid',
  //           sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__ticket_form_id_cfid',
  //           type: 'number',
  //           alias: 'ticket_form_id',
  //         },
  //         {
  //           name: 'ctype_review_subtype__date',
  //           sql: 'ctype_review_subtype__date',
  //           type: 'time',
  //           alias: 'Date',
  //         },
  //         {
  //           name: 'ctype_review_subtype__reply_date',
  //           sql: 'ctype_review_subtype__reply_date',
  //           type: 'time',
  //           alias: 'Reply date',
  //         },
  //         {
  //           name: 'ctype_review_subtype__upvote_count',
  //           sql: 'ctype_review_subtype__upvote_count',
  //           type: 'number',
  //           alias: 'Upvote Count',
  //         },
  //         {
  //           name: 'ctype_jkjkjkjkjkjk__dggd',
  //           sql: 'ctype_jkjkjkjkjkjk__dggd',
  //           type: 'number',
  //           alias: 'dggd',
  //         },
  //         {
  //           name: 'id_count___function__count',
  //           function: {
  //             type: 'count',
  //           },
  //           sql: 'count(dim_ticket.id)',
  //           type: 'string',
  //           alias: 'Ticket Id',
  //         },
  //       ],
  //       name: 'dim_ticket',
  //       sql: 'select * from devrev.dim_ticket',
  //     },
  //   ] as any;
  //   const resolutionConfig = {
  //     columnConfigs: [
  //       {
  //         joinColumn: 'id',
  //         name: 'dim_ticket.tags_json',
  //         isArrayType: true,
  //         resolutionColumns: ['name'],
  //         source: 'dim_tag',
  //       },
  //     ],
  //     tableSchemas: [
  //       {
  //         dimensions: [
  //           {
  //             name: 'created_by_id',
  //             sql: 'created_by_id',
  //             type: 'string',
  //             alias: 'Created by',
  //           },
  //           {
  //             name: 'dev_oid',
  //             sql: 'dev_oid',
  //             type: 'string',
  //             alias: 'Dev organization ID',
  //           },
  //           {
  //             name: 'modified_date',
  //             sql: 'modified_date',
  //             type: 'time',
  //             alias: 'Modified date',
  //           },
  //           {
  //             name: 'object_type',
  //             sql: 'object_type',
  //             type: 'string',
  //             alias: 'Object type',
  //           },
  //           {
  //             name: 'tag_type',
  //             sql: 'tag_type',
  //             type: 'number',
  //             alias: 'Values enabled',
  //           },
  //           {
  //             modifier: {
  //               shouldUnnestGroupBy: false,
  //             },
  //             name: 'allowed_values',
  //             sql: 'allowed_values',
  //             type: 'string_array',
  //             alias: 'Allowed values',
  //           },
  //           {
  //             name: 'is_deleted',
  //             sql: 'is_deleted',
  //             type: 'boolean',
  //             alias: 'Is deleted',
  //           },
  //           {
  //             name: 'modified_by_id',
  //             sql: 'modified_by_id',
  //             type: 'string',
  //             alias: 'Modified by',
  //           },
  //           {
  //             name: 'access_level',
  //             sql: 'access_level',
  //             type: 'number',
  //             alias: 'Access level',
  //           },
  //           {
  //             name: 'display_id',
  //             sql: 'display_id',
  //             type: 'string',
  //             alias: 'Display ID',
  //           },
  //           {
  //             name: 'id',
  //             sql: 'id',
  //             type: 'string',
  //             alias: 'ID',
  //           },
  //           {
  //             name: 'name',
  //             sql: 'name',
  //             type: 'string',
  //             alias: 'Tag name',
  //           },
  //           {
  //             name: 'object_version',
  //             sql: 'object_version',
  //             type: 'number',
  //             alias: 'Object Version',
  //           },
  //           {
  //             name: 'created_date',
  //             sql: 'created_date',
  //             type: 'time',
  //             alias: 'Created date',
  //           },
  //         ],
  //         measures: [],
  //         name: 'dim_tag',
  //         sql: 'SELECT * FROM devrev.dim_tag',
  //       },
  //     ],
  //   } as any;
  //   const columnProjections = [
  //     'dim_ticket.tags_json',
  //     'dim_ticket.id_count___function__count',
  //   ];

  //   const resultQuery = await cubeQueryToSQLWithResolution({
  //     query,
  //     tableSchemas,
  //     resolutionConfig,
  //     columnProjections,
  //   });

  //   const expectedQuery = `select * exclude(__row_id) from (SELECT MAX(__resolved_query."Ticket Id") AS "Ticket Id" ,  ARRAY_AGG(DISTINCT __resolved_query."Tags - Tag name") AS "Tags - Tag name" ,   "__row_id" FROM (SELECT __resolved_query."__row_id" AS "__row_id", * FROM (SELECT  "Tags - Tag name",  "Ticket Id",  "__row_id" FROM (SELECT __unnested_base_query."Ticket Id" AS "Ticket Id", __unnested_base_query."__row_id" AS "__row_id", * FROM (SELECT  "Ticket Id",  "Tags",  "__row_id" FROM (SELECT __base_query."Ticket Id" AS "Ticket Id", unnest(__base_query."Tags") AS "Tags", row_number() OVER () AS "__row_id", * FROM (SELECT count("ID") AS "Ticket Id" ,   "Tags" FROM (SELECT dim_ticket.created_date AS "Created date", 'ticket' AS "Work Type", json_extract_string(dim_ticket.stage_json, '$.stage_id') AS "Stage", CAST(json_extract_string(dim_ticket.tags_json, '$[*].tag_id') AS VARCHAR[]) AS "Tags", dim_ticket.id AS "ID", * FROM (select * from devrev.dim_ticket) AS dim_ticket) AS dim_ticket WHERE (((("Created date" >= '2025-08-02T09:30:00.000Z') AND ("Created date" <= '2025-10-31T10:29:59.999Z')) AND ("Work Type" IN ('ticket')) AND (Stage IN ('don:core:dvrv-us-1:devo/787:custom_stage/514', 'don:core:dvrv-us-1:devo/787:custom_stage/512', 'don:core:dvrv-us-1:devo/787:custom_stage/501', 'don:core:dvrv-us-1:devo/787:custom_stage/485', 'don:core:dvrv-us-1:devo/787:custom_stage/440', 'don:core:dvrv-us-1:devo/787:custom_stage/25', 'don:core:dvrv-us-1:devo/787:custom_stage/24', 'don:core:dvrv-us-1:devo/787:custom_stage/22', 'don:core:dvrv-us-1:devo/787:custom_stage/21', 'don:core:dvrv-us-1:devo/787:custom_stage/19', 'don:core:dvrv-us-1:devo/787:custom_stage/13', 'don:core:dvrv-us-1:devo/787:custom_stage/10', 'don:core:dvrv-us-1:devo/787:custom_stage/8', 'don:core:dvrv-us-1:devo/787:custom_stage/7', 'don:core:dvrv-us-1:devo/787:custom_stage/6', 'don:core:dvrv-us-1:devo/787:custom_stage/4')))) GROUP BY Tags LIMIT 50000) AS __base_query) AS __base_query) AS __unnested_base_query LEFT JOIN (SELECT __unnested_base_query__dim_ticket__tags_json.name AS "Tags - Tag name", * FROM (SELECT * FROM devrev.dim_tag) AS __unnested_base_query__dim_ticket__tags_json) AS __unnested_base_query__dim_ticket__tags_json  ON __unnested_base_query."Tags"=__unnested_base_query__dim_ticket__tags_json.id) AS MEERKAT_GENERATED_TABLE) AS __resolved_query) AS __resolved_query GROUP BY __row_id)`;
  //   expect(resultQuery).toBe(expectedQuery);
  // });
  //   it('Should handle only scalar field resolution without unnesting', async () => {
  //     const query: Query = {
  //       measures: ['tickets.count'],
  //       dimensions: [
  //         'tickets.id',
  //         'tickets.owners',
  //         'tickets.tags',
  //         'tickets.created_by',
  //       ],
  //     };

  //     const resolutionConfig: ResolutionConfig = {
  //       columnConfigs: [
  //         {
  //           name: 'tickets.created_by',
  //           isArrayType: false,
  //           source: 'created_by_lookup',
  //           joinColumn: 'id',
  //           resolutionColumns: ['name'],
  //         },
  //       ],
  //       tableSchemas: [CREATED_BY_LOOKUP_SCHEMA],
  //     };

  //     const sql = await cubeQueryToSQLWithResolutionWithArray({
  //       query,
  //       tableSchemas: [TICKETS_TABLE_SCHEMA],
  //       resolutionConfig,
  //     });

  //     console.log('Phase 1 SQL (multiple arrays):', sql);

  //     // Verify row_id is included
  //     expect(sql).toContain('row_id');

  //     // Both arrays should be unnested
  //     expect(sql.match(/unnest/g)?.length).toBeGreaterThanOrEqual(0);

  //     // Execute the SQL to verify it works
  //     const result = (await duckdbExec(sql)) as any[];

  //     expect(result.length).toBe(7);

  //     // Each row should have a row_id
  //     expect(result[0]).toHaveProperty('__row_id');
  //     expect(result[0]).toHaveProperty('tickets__count');
  //     expect(result[0]).toHaveProperty('tickets__id');
  //     expect(result[0]).toHaveProperty('tickets__owners');
  //     expect(result[0]).toHaveProperty('tickets__tags');
  //     expect(result[0]).toHaveProperty('tickets__created_by - name');
  //   });

  //   it('Should return regular SQL when no array fields need resolution', async () => {
  //     const query: Query = {
  //       measures: ['tickets.count'],
  //       dimensions: ['tickets.id', 'tickets.created_by'],
  //     };

  //     const resolutionConfig: ResolutionConfig = {
  //       columnConfigs: [],
  //       tableSchemas: [],
  //     };

  //     const sql = await cubeQueryToSQLWithResolutionWithArray({
  //       query,
  //       tableSchemas: [TICKETS_TABLE_SCHEMA],
  //       resolutionConfig,
  //     });

  //     console.log('SQL without resolution:', sql);

  //     // Should not have row_id or unnest when no array resolution is needed
  //     expect(sql).not.toContain('row_id');
  //     expect(sql).not.toContain('unnest');
  //   });
});

// describe('cubeQueryToSQLWithResolutionWithArray - Phase 2: Resolution', () => {
//   jest.setTimeout(1000000);

//   beforeAll(async () => {
//     // Tables are already created in Phase 1 tests
//   });

//   it('Should resolve array values by joining with lookup tables', async () => {
//     const query: Query = {
//       measures: ['tickets.count'],
//       dimensions: ['tickets.id', 'tickets.owners'],
//     };

//     const resolutionConfig: ResolutionConfig = {
//       columnConfigs: [
//         {
//           name: 'tickets.owners',
//           isArrayType: true,
//           source: 'owners_lookup',
//           joinColumn: 'id',
//           resolutionColumns: ['display_name', 'email'],
//         },
//       ],
//       tableSchemas: [OWNERS_LOOKUP_SCHEMA],
//     };

//     const sql = await cubeQueryToSQLWithResolutionWithArray({
//       query,
//       tableSchemas: [TICKETS_TABLE_SCHEMA],
//       resolutionConfig,
//     });

//     console.log('Phase 2 SQL (with resolution):', sql);

//     // Verify the SQL includes row_id
//     expect(sql).toContain('row_id');

//     // Verify the SQL has joins to resolution tables
//     expect(sql).toContain('owners_lookup');

//     // Verify it includes resolution columns
//     expect(sql).toContain('display_name');
//     expect(sql).toContain('email');

//     // Execute the SQL to verify it works
//     const result = (await duckdbExec(sql)) as any[];
//     console.log(
//       'Phase 2 Result (first 3 rows):',
//       JSON.stringify(result.slice(0, 3), null, 2)
//     );

//     // The result should have 5 unnested + resolved rows
//     expect(result.length).toBe(5);

//     // Each row should have resolution columns
//     expect(result[0]).toHaveProperty('__unnested_base_query__row_id');
//     expect(result[0]).toHaveProperty('tickets__owners__display_name');
//     expect(result[0]).toHaveProperty('tickets__owners__email');

//     // Verify actual resolved values
//     const owner1Rows = result.filter(
//       (r: any) => r['__unnested_base_query__tickets__owners'] === 'owner1'
//     );
//     expect(owner1Rows[0]['tickets__owners__display_name']).toBe('Alice Smith');
//     expect(owner1Rows[0]['tickets__owners__email']).toBe('alice@example.com');
//   });
// });

// describe('cubeQueryToSQLWithResolutionWithArray - Phase 3: Re-aggregation', () => {
//   jest.setTimeout(1000000);

//   beforeAll(async () => {
//     // Tables are already created in Phase 1 tests
//   });

//   it('Should re-aggregate unnested rows back to original count with resolved arrays', async () => {
//     const query: Query = {
//       measures: ['tickets.count'],
//       dimensions: ['tickets.id', 'tickets.owners'],
//     };

//     const resolutionConfig: ResolutionConfig = {
//       columnConfigs: [
//         {
//           name: 'tickets.owners',
//           isArrayType: true,
//           source: 'owners_lookup',
//           joinColumn: 'id',
//           resolutionColumns: ['display_name', 'email'],
//         },
//       ],
//       tableSchemas: [OWNERS_LOOKUP_SCHEMA],
//     };

//     const sql = await cubeQueryToSQLWithResolutionWithArray({
//       query,
//       tableSchemas: [TICKETS_TABLE_SCHEMA],
//       resolutionConfig,
//     });

//     console.log('Phase 3 SQL (re-aggregated):', sql);

//     // Verify the SQL includes aggregation functions
//     expect(sql).toContain('GROUP BY');
//     expect(sql).toContain('row_id');

//     // Execute the SQL to verify it works
//     const result = (await duckdbExec(sql)) as any[];
//     console.log('Phase 3 Result:', JSON.stringify(result, null, 2));

//     // Should have 3 rows (back to original count)
//     expect(result.length).toBe(3);

//     // Each row should have aggregated data
//     expect(result[0]).toHaveProperty('__aggregation_base__row_id');
//     expect(result[0]).toHaveProperty('__aggregation_base__tickets__count');
//     expect(result[0]).toHaveProperty('__aggregation_base__tickets__id');

//     // Should have arrays with resolved values
//     expect(result[0]).toHaveProperty(
//       '__aggregation_base__tickets__owners__display_name'
//     );
//     expect(result[0]).toHaveProperty(
//       '__aggregation_base__tickets__owners__email'
//     );

//     // Verify the resolved arrays contain the correct values
//     // Ticket 1 has owners: owner1, owner2
//     const ticket1 = result.find(
//       (r: any) => r['__aggregation_base__tickets__id'] === 1
//     );
//     expect(ticket1).toBeDefined();

//     // The display names should be aggregated into an array
//     const displayNames =
//       ticket1['__aggregation_base__tickets__owners__display_name'];
//     console.log('Ticket 1 display names:', displayNames);

//     // Note: The actual format depends on how cubeQueryToSQL handles ARRAY_AGG
//     // It should be an array containing the resolved values
//   });
// });
