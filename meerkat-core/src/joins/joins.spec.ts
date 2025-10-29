import { getUsedTableSchema } from '../get-used-table-schema/get-used-table-schema';
import { Query, TableSchema } from '../types/cube-types';
import {
  checkLoopInJoinPath,
  createDirectedGraph,
  generateSqlQuery,
  getCombinedTableSchema,
} from './joins';

describe('Table schema functions', () => {
  it('should create a directed graph from the table schema', () => {
    const sqlQueryMap = {
      table1: 'select * from table1',
      table2: 'select * from table2',
      table3: 'select * from table3',
    };
    const tableSchema = [
      {
        name: 'table1',
        sql: 'select * from table1',
        joins: [{ sql: 'table1.id = table2.id' }],
      },
      {
        name: 'table2',
        sql: 'select * from table2',
        joins: [{ sql: 'table2.id = table3.id' }],
      },
      { name: 'table3', sql: 'select * from table3', joins: [] },
    ];
    const directedGraph = createDirectedGraph(tableSchema, sqlQueryMap);

    expect(directedGraph).toEqual({
      table1: { table2: { id: 'table1.id = table2.id' } },
      table2: { table3: { id: 'table2.id = table3.id' } },
    });
  });

  it('should ignore a directed graph edge from the table schema if not present in query map', () => {
    const sqlQueryMap = {
      table1: 'select * from table1',
      table2: 'select * from table2',
    };
    const tableSchema = [
      {
        name: 'table1',
        sql: 'select * from table1',
        joins: [
          { sql: 'table1.id = table2.id' },
          { sql: 'table1.id = table4.id' },
        ],
      },
      {
        name: 'table2',
        sql: 'select * from table2',
        joins: [{ sql: 'table2.id = table3.id' }],
      },
      { name: 'table3', sql: 'select * from table3', joins: [] },
    ];
    const directedGraph = createDirectedGraph(tableSchema, sqlQueryMap);

    expect(directedGraph).toEqual({
      table1: { table2: { id: 'table1.id = table2.id' } },
    });
  });

  it('should correctly generate a SQL query from the provided join path, table schema SQL map, and directed graph', () => {
    const joinPaths = [
      [
        { left: 'table1', right: 'table2', on: 'id' },
        { left: 'table2', right: 'table3', on: 'id' },
      ],
    ];
    const directedGraph = {
      table1: { table2: { id: 'table1.id = table2.id' } },
      table2: { table3: { id: 'table2.id = table3.id' } },
    };
    const tableSchemaSqlMap = {
      table1: 'select * from table1',
      table2: 'select * from table2',
      table3: 'select * from table3',
    };
    const sqlQuery = generateSqlQuery(
      joinPaths,
      tableSchemaSqlMap,
      directedGraph
    );

    expect(sqlQuery).toBe(
      'select * from table1 LEFT JOIN (select * from table2) AS table2  ON table1.id = table2.id LEFT JOIN (select * from table3) AS table3  ON table2.id = table3.id'
    );
  });

  describe('checkLoopInJoinPath', () => {
    it('should return false if there is no loop in the join path', () => {
      const joinPath = [
        [
          { left: 'table1', right: 'table2', on: 'id' },
          { left: 'table2', right: 'table3', on: 'id' },
        ],
      ];
      expect(checkLoopInJoinPath(joinPath)).toBe(false);
    });
    it('should return true if there is a loop in the join path', () => {
      const joinPath = [
        [
          { left: 'table1', right: 'table2', on: 'id' },
          { left: 'table2', right: 'table3', on: 'id' },
          { left: 'table3', right: 'table1', on: 'id' },
        ],
      ];
      expect(checkLoopInJoinPath(joinPath)).toBe(true);
    });
    it('should return false for single node', () => {
      const joinPath = [[{ left: 'table1' }, { left: 'table1' }]];
      expect(checkLoopInJoinPath(joinPath)).toBe(false);
    });
  });

  describe('getCombinedTableSchema', () => {
    it('should return single table schema when only one table is provided', async () => {
      const tableSchema = [
        {
          name: 'orders',
          sql: 'select * from orders',
          measures: [
            { name: 'total_amount', sql: 'SUM(amount)' },
            { name: 'order_count', sql: 'COUNT(*)' },
          ],
          dimensions: [
            { name: 'order_date', sql: 'created_at' },
            { name: 'status', sql: 'order_status' },
          ],
          joins: [],
        },
      ];
      const cubeQuery = {
        joinPaths: [],
      };
      const result = await getCombinedTableSchema(tableSchema, cubeQuery);
      expect(result).toEqual(tableSchema[0]);
    });

    it('should combine multiple table schemas correctly', async () => {
      const tableSchema = [
        {
          name: 'orders',
          sql: 'select * from orders',
          measures: [
            { name: 'total_amount', sql: 'SUM(amount)' },
            { name: 'order_count', sql: 'COUNT(*)' },
            { name: 'avg_order_value', sql: 'AVG(amount)' },
          ],
          dimensions: [
            { name: 'order_date', sql: 'created_at' },
            { name: 'status', sql: 'order_status' },
            { name: 'payment_method', sql: 'payment_type' },
          ],
          joins: [{ sql: 'orders.customer_id = customers.id' }],
        },
        {
          name: 'customers',
          sql: 'select * from customers',
          measures: [
            { name: 'customer_count', sql: 'COUNT(DISTINCT id)' },
            { name: 'total_customers', sql: 'COUNT(*)' },
            { name: 'avg_customer_age', sql: 'AVG(age)' },
          ],
          dimensions: [
            { name: 'join_date', sql: 'created_at' },
            { name: 'customer_type', sql: 'type' },
            { name: 'region', sql: 'region' },
          ],
          joins: [],
        },
      ];
      const cubeQuery = {
        joinPaths: [
          [
            {
              left: 'orders',
              right: 'customers',
              on: 'customer_id',
            },
          ],
        ],
      };
      const result = await getCombinedTableSchema(tableSchema, cubeQuery);
      expect(result).toEqual({
        name: 'MEERKAT_GENERATED_TABLE',
        sql: 'select * from orders LEFT JOIN (select * from customers) AS customers  ON orders.customer_id = customers.id',
        measures: [
          { name: 'total_amount', sql: 'SUM(amount)' },
          { name: 'order_count', sql: 'COUNT(*)' },
          { name: 'avg_order_value', sql: 'AVG(amount)' },
          { name: 'customer_count', sql: 'COUNT(DISTINCT id)' },
          { name: 'total_customers', sql: 'COUNT(*)' },
          { name: 'avg_customer_age', sql: 'AVG(age)' },
        ],
        dimensions: [
          { name: 'order_date', sql: 'created_at' },
          { name: 'status', sql: 'order_status' },
          { name: 'payment_method', sql: 'payment_type' },
          { name: 'join_date', sql: 'created_at' },
          { name: 'customer_type', sql: 'type' },
          { name: 'region', sql: 'region' },
        ],
        joins: [],
      });
    });

    it('should throw error when loop is detected in join paths', async () => {
      const tableSchema = [
        {
          name: 'orders',
          sql: 'select * from orders',
          measures: [{ name: 'total_amount', sql: 'SUM(amount)' }],
          dimensions: [{ name: 'order_id', sql: 'id' }],
          joins: [{ sql: 'orders.customer_id = customers.id' }],
        },
        {
          name: 'customers',
          sql: 'select * from customers',
          measures: [{ name: 'customer_count', sql: 'COUNT(*)' }],
          dimensions: [{ name: 'customer_id', sql: 'id' }],
          joins: [{ sql: 'customers.order_id = orders.id' }],
        },
      ];
      const cubeQuery = {
        joinPaths: [
          [
            { left: 'orders', right: 'customers', on: 'id' },
            { left: 'customers', right: 'orders', on: 'id' },
          ],
        ],
      };
      await expect(
        getCombinedTableSchema(tableSchema, cubeQuery)
      ).rejects.toThrow(/A loop was detected in the joins/);
    });

    it('should handle empty measures and dimensions', async () => {
      const tableSchema = [
        {
          name: 'table1',
          sql: 'select * from table1',
          measures: [],
          dimensions: [],
          joins: [{ sql: 'table1.id = table2.id' }],
        },
        {
          name: 'table2',
          sql: 'select * from table2',
          measures: [],
          dimensions: [],
          joins: [],
        },
      ];
      const cubeQuery = {
        joinPaths: [[{ left: 'table1', right: 'table2', on: 'id' }]],
      };
      const result = await getCombinedTableSchema(tableSchema, cubeQuery);
      expect(result).toEqual({
        name: 'MEERKAT_GENERATED_TABLE',
        sql: 'select * from table1 LEFT JOIN (select * from table2) AS table2  ON table1.id = table2.id',
        measures: [],
        dimensions: [],
        joins: [],
      });
    });

    it('should filter table schema based on filters, measures, and dimensions', () => {
      const tableSchema: TableSchema[] = [
        {
          name: 'products',
          sql: 'SELECT * FROM products',
          measures: [
            {
              name: 'total_sales',
              sql: 'SUM(sales)',
              type: 'string',
            },
            {
              name: 'inventory_count',
              sql: 'SUM(stock)',
              type: 'string',
            },
          ],
          dimensions: [
            {
              name: 'category',
              sql: 'category',
              type: 'string',
            },
            {
              name: 'brand',
              sql: 'brand_name',
              type: 'string',
            },
          ],
          joins: [],
        },
        {
          name: 'sales',
          sql: 'SELECT * FROM sales',
          measures: [
            {
              name: 'revenue',
              sql: 'SUM(amount)',
              type: 'string',
            },
            {
              name: 'transaction_count',
              sql: 'COUNT(*)',
              type: 'string',
            },
          ],
          dimensions: [
            {
              name: 'sale_date',
              sql: 'created_at',
              type: 'string',
            },
            {
              name: 'store_location',
              sql: 'location',
              type: 'string',
            },
          ],
          joins: [],
        },
        {
          name: 'inventory',
          sql: 'SELECT * FROM inventory',
          measures: [
            {
              name: 'stock_value',
              sql: 'SUM(value)',
              type: 'string',
            },
            {
              name: 'reorder_count',
              sql: 'COUNT(reorder_flag)',
              type: 'string',
            },
          ],
          dimensions: [
            {
              name: 'warehouse',
              sql: 'warehouse_id',
              type: 'string',
            },
            {
              name: 'stock_status',
              sql: 'status',
              type: 'string',
            },
          ],
          joins: [],
        },
      ];
      const cubeQuery: Query = {
        measures: ['products.total_sales'],
        dimensions: ['sales.store_location'],
        filters: [
          {
            member: 'inventory.stock_status',
            operator: 'equals',
            values: ['LOW'],
          },
        ],
      };
      const result = getUsedTableSchema(tableSchema, cubeQuery);
      expect(result).toEqual(tableSchema);
    });

    it('should filter table schema based on filters, measures, dimensions, order, and joinPaths', () => {
      const tableSchema: TableSchema[] = [
        {
          name: 'users',
          sql: 'SELECT * FROM users',
          measures: [
            {
              name: 'user_count',
              sql: 'COUNT(DISTINCT id)',
              type: 'string',
            },
            {
              name: 'active_users',
              sql: 'SUM(is_active)',
              type: 'string',
            },
          ],
          dimensions: [
            {
              name: 'age_group',
              sql: 'age_bracket',
              type: 'string',
            },
            {
              name: 'user_type',
              sql: 'type',
              type: 'string',
            },
          ],
          joins: [],
        },
        {
          name: 'sessions',
          sql: 'SELECT * FROM sessions',
          measures: [
            {
              name: 'session_duration',
              sql: 'AVG(duration)',
              type: 'string',
            },
            {
              name: 'session_count',
              sql: 'COUNT(*)',
              type: 'string',
            },
          ],
          dimensions: [
            {
              name: 'device_type',
              sql: 'device',
              type: 'string',
            },
            {
              name: 'platform',
              sql: 'os',
              type: 'string',
            },
          ],
          joins: [],
        },
        {
          name: 'events',
          sql: 'SELECT * FROM events',
          measures: [
            {
              name: 'event_count',
              sql: 'COUNT(*)',
              type: 'string',
            },
            {
              name: 'unique_events',
              sql: 'COUNT(DISTINCT event_type)',
              type: 'string',
            },
          ],
          dimensions: [
            {
              name: 'event_category',
              sql: 'category',
              type: 'string',
            },
            {
              name: 'event_source',
              sql: 'source',
              type: 'string',
            },
          ],
          joins: [],
        },
        {
          name: 'conversions',
          sql: 'SELECT * FROM conversions',
          measures: [
            {
              name: 'conversion_rate',
              sql: 'AVG(is_converted)',
              type: 'string',
            },
            {
              name: 'total_value',
              sql: 'SUM(value)',
              type: 'string',
            },
          ],
          dimensions: [
            {
              name: 'conversion_type',
              sql: 'type',
              type: 'string',
            },
            {
              name: 'funnel_stage',
              sql: 'stage',
              type: 'string',
            },
          ],
          joins: [],
        },
      ];
      const cubeQuery: Query = {
        measures: ['users.user_count'],
        dimensions: ['sessions.device_type'],
        filters: [
          {
            member: 'events.event_category',
            operator: 'equals',
            values: ['purchase'],
          },
        ],
        order: {
          'conversions.conversion_rate': 'asc',
        },
        joinPaths: [
          [
            {
              left: 'users',
              right: 'sessions',
              on: 'user_id',
            },
          ],
        ],
      };
      const result = getUsedTableSchema(tableSchema, cubeQuery);
      expect(result).toEqual(tableSchema);
    });

    it('should return only single table if other tables are not getting used from schema', () => {
      const tableSchemas: TableSchema[] = [
        {
          dimensions: [
            {
              name: 'rev_oid',
              sql: 'dim_ticket.rev_oid',
              type: 'string',
            },
            {
              name: 'title',
              sql: 'dim_ticket.title',
              type: 'string',
            },
            {
              name: 'last_internal_comment_date',
              sql: 'dim_ticket.last_internal_comment_date',
              type: 'time',
            },
            {
              name: 'work_type',
              sql: "'ticket'",
              type: 'time',
            },
            {
              name: 'is_spam',
              sql: 'dim_ticket.is_spam',
              type: 'boolean',
            },
            {
              name: 'object_type',
              sql: 'dim_ticket.object_type',
              type: 'string',
            },
            {
              name: 'modified_by_id',
              sql: 'dim_ticket.modified_by_id',
              type: 'string',
            },
            {
              name: 'source_channel',
              sql: 'dim_ticket.source_channel',
              type: 'string',
            },
            {
              modifier: {
                shouldUnnestGroupBy: true,
              },
              name: 'channels',
              sql: 'dim_ticket.channels',
              type: 'number_array',
            },
            {
              name: 'created_by_id',
              sql: 'dim_ticket.created_by_id',
              type: 'string',
            },
            {
              name: 'created_date',
              sql: 'dim_ticket.created_date',
              type: 'time',
            },
            {
              name: 'target_close_date',
              sql: 'dim_ticket.target_close_date',
              type: 'time',
            },
            {
              name: 'applies_to_part_id',
              sql: 'dim_ticket.applies_to_part_id',
              type: 'string',
            },
            {
              name: 'subtype',
              sql: 'dim_ticket.subtype',
              type: 'string',
            },
            {
              name: 'actual_close_date',
              sql: 'dim_ticket.actual_close_date',
              type: 'time',
            },
            {
              name: 'reported_by_id',
              sql: 'dim_ticket.reported_by_id',
              type: 'string',
            },
            {
              modifier: {
                shouldUnnestGroupBy: true,
              },
              name: 'reported_by_ids',
              sql: 'dim_ticket.reported_by_ids',
              type: 'string_array',
            },
            {
              name: 'needs_response',
              sql: 'dim_ticket.needs_response',
              type: 'boolean',
            },
            {
              name: 'group',
              sql: 'dim_ticket.group',
              type: 'string',
            },
            {
              name: 'modified_date',
              sql: 'dim_ticket.modified_date',
              type: 'time',
            },
            {
              modifier: {
                shouldUnnestGroupBy: true,
              },
              name: 'owned_by_ids',
              sql: 'dim_ticket.owned_by_ids',
              type: 'string_array',
            },
            {
              name: 'id',
              sql: 'dim_ticket.id',
              type: 'string',
            },
            {
              name: 'sla_tracker_id',
              sql: 'dim_ticket.sla_tracker_id',
              type: 'string',
            },
            {
              name: 'severity',
              sql: 'dim_ticket.severity',
              type: 'number',
            },
            {
              name: 'stage_json',
              sql: "json_extract_string(dim_ticket.stage_json, '$.stage_id')",
              type: 'string',
            },
            {
              name: 'sla_id',
              sql: 'dim_ticket.sla_id',
              type: 'string',
            },
            {
              modifier: {
                shouldUnnestGroupBy: true,
              },
              name: 'links_json',
              sql: "list_distinct(CAST(json_extract_string(dim_ticket.links_json, '$[*].target_object_type') AS VARCHAR[]))",
              type: 'string_array',
            },
            {
              modifier: {
                shouldUnnestGroupBy: true,
              },
              name: 'tags_json',
              sql: "CAST(json_extract_string(dim_ticket.tags_json, '$[*].tag_id') AS VARCHAR[])",
              type: 'string_array',
            },
            {
              name: 'surveys_aggregation_json',
              sql: "list_aggregate(CAST(json_extract_string(dim_ticket.surveys_aggregation_json, '$[*].minimum') AS integer[]), 'min')",
              type: 'number',
            },
            {
              name: 'sla_summary_target_time',
              sql: "cast(json_extract_string(dim_ticket.sla_summary, '$.target_time') as timestamp)",
              type: 'time',
            },
            {
              name: 'staged_info',
              sql: "cast(json_extract_string(dim_ticket.staged_info, '$.is_staged') as boolean)",
              type: 'boolean',
            },
          ],
          joins: [
            {
              sql: 'dim_ticket.sla_tracker_id = dim_sla_tracker.id',
            },
            {
              sql: 'dim_ticket.id = dim_survey_response.object',
            },
            {
              sql: 'dim_ticket.id = dim_link_issue_target.source_id',
            },
            {
              sql: 'dim_ticket.id = dim_link_conversation_source.target_id',
            },
            {
              sql: 'dim_ticket.rev_oid = dim_revo.id',
            },
            {
              sql: 'dim_ticket.applies_to_part_id = dim_part.id',
            },
          ],
          measures: [
            {
              name: 'id_count',
              function: {
                type: 'count',
              },
              sql: 'count(dim_ticket.id)',
              type: 'string',
            },
            {
              name: 'created_date_max',
              function: {
                type: 'max',
              },
              sql: 'max(dim_ticket.created_date)',
              type: 'time',
            },
            {
              name: 'actual_close_date_max',
              function: {
                type: 'max',
              },
              sql: 'max(dim_ticket.actual_close_date)',
              type: 'time',
            },
            {
              name: 'sla_tracker_id_count',
              function: {
                type: 'count',
              },
              sql: 'count(dim_ticket.sla_tracker_id)',
              type: 'string',
            },
            {
              name: 'resolution_time',
              sql: "case WHEN actual_close_date > created_date THEN date_diff('minutes', created_date, actual_close_date) ELSE null END ",
              type: 'number',
            },
            {
              name: 'surveys_aggregation_json_measure',
              function: {
                type: 'median',
              },
              sql: "median(list_aggregate(CAST(json_extract_string(dim_ticket.surveys_aggregation_json, '$[*].minimum') AS integer[]), 'min'))",
              type: 'number',
            },
            {
              name: 'tnt__account_id',
              sql: 'tnt__account_id',
              type: 'string',
            },
            {
              name: 'tnt__actual_effort_spent',
              sql: 'tnt__actual_effort_spent',
              type: 'number',
            },
            {
              name: 'tnt__capability_part',
              sql: 'tnt__capability_part',
              type: 'string',
            },
            {
              name: 'tnt__custom_id_field',
              sql: 'tnt__custom_id_field',
              type: 'string',
            },
            {
              name: 'tnt__date_field',
              sql: 'tnt__date_field',
              type: 'time',
            },
            {
              name: 'tnt__estimated_effort',
              sql: 'tnt__estimated_effort',
              type: 'number',
            },
            {
              name: 'tnt__fruit',
              sql: 'tnt__fruit',
              type: 'string',
            },
            {
              name: 'tnt__id_field',
              sql: 'tnt__id_field',
              type: 'string',
            },
            {
              name: 'tnt__issue_score',
              sql: 'tnt__issue_score',
              type: 'number',
            },
            {
              name: 'tnt__numeric_field',
              sql: 'tnt__numeric_field',
              type: 'number',
            },
            {
              name: 'tnt__parent_part',
              sql: 'tnt__parent_part',
              type: 'string',
            },
            {
              name: 'tnt__part_product',
              sql: 'tnt__part_product',
              type: 'string',
            },
            {
              name: 'tnt__rank',
              sql: 'tnt__rank',
              type: 'number',
            },
            {
              name: 'tnt__remaining_effort',
              sql: 'tnt__remaining_effort',
              type: 'number',
            },
            {
              name: 'tnt__stray_user',
              sql: 'tnt__stray_user',
              type: 'string',
            },
            {
              name: 'tnt__stray_users',
              sql: 'tnt__stray_users',
              type: 'string',
            },
            {
              name: 'tnt__test',
              sql: 'tnt__test',
              type: 'number',
            },
            {
              name: 'tnt__test_capability_part',
              sql: 'tnt__test_capability_part',
              type: 'string',
            },
            {
              name: 'tnt__test_product_part',
              sql: 'tnt__test_product_part',
              type: 'string',
            },
            {
              name: 'tnt__ticket_custom_part',
              sql: 'tnt__ticket_custom_part',
              type: 'string',
            },
            {
              name: 'tnt__workspace_custom',
              sql: 'tnt__workspace_custom',
              type: 'string',
            },
            {
              name: 'ctype_deal_registration__estimated_deal_value',
              sql: 'ctype_deal_registration__estimated_deal_value',
              type: 'number',
            },
            {
              name: 'ctype_deal_registration__expected_close_date',
              sql: 'ctype_deal_registration__expected_close_date',
              type: 'time',
            },
            {
              name: 'ctype_Events__event_end_date',
              sql: 'ctype_Events__event_end_date',
              type: 'time',
            },
            {
              name: 'ctype_Events__event_owner',
              sql: 'ctype_Events__event_owner',
              type: 'string',
            },
            {
              name: 'ctype_Events__event_start_date',
              sql: 'ctype_Events__event_start_date',
              type: 'time',
            },
            {
              name: 'ctype_Events__events_test_ref',
              sql: 'ctype_Events__events_test_ref',
              type: 'string',
            },
            {
              name: 'ctype_Events__external_budget',
              sql: 'ctype_Events__external_budget',
              type: 'number',
            },
            {
              name: 'ctype_Events__internal_budget',
              sql: 'ctype_Events__internal_budget',
              type: 'number',
            },
            {
              name: 'ctype_Events__pipeline_generated',
              sql: 'ctype_Events__pipeline_generated',
              type: 'number',
            },
            {
              name: 'ctype_Events__total_budget',
              sql: 'ctype_Events__total_budget',
              type: 'number',
            },
            {
              name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__brand_id_cfid',
              sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__brand_id_cfid',
              type: 'number',
            },
            {
              name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__created_at_cfid',
              sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__created_at_cfid',
              type: 'time',
            },
            {
              name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__generated_timestamp_cfid',
              sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__generated_timestamp_cfid',
              type: 'time',
            },
            {
              name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__group_id_cfid',
              sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__group_id_cfid',
              type: 'number',
            },
            {
              name: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__ticket_form_id_cfid',
              sql: 'ctype_svxsdhkjcbdsgcbvsdjgchgdshckweds__ticket_form_id_cfid',
              type: 'number',
            },
            {
              name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__brand_id_cfid',
              sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__brand_id_cfid',
              type: 'number',
            },
            {
              name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__created_at_cfid',
              sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__created_at_cfid',
              type: 'time',
            },
            {
              name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__generated_timestamp_cfid',
              sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__generated_timestamp_cfid',
              type: 'time',
            },
            {
              name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__group_id_cfid',
              sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__group_id_cfid',
              type: 'number',
            },
            {
              name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__ticket_form_id_cfid',
              sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwhe33cnrsy2__ticket_form_id_cfid',
              type: 'number',
            },
            {
              name: 'ctype_helohelohelohelohelo__dfkdkl',
              sql: 'ctype_helohelohelohelohelo__dfkdkl',
              type: 'number',
            },
            {
              name: 'ctype_defaults__customer_uat',
              sql: 'ctype_defaults__customer_uat',
              type: 'time',
            },
            {
              name: 'ctype_defaults__uat_date',
              sql: 'ctype_defaults__uat_date',
              type: 'time',
            },
            {
              name: 'ctype_event_request__approver',
              sql: 'ctype_event_request__approver',
              type: 'string',
            },
            {
              name: 'ctype_event_request__budget',
              sql: 'ctype_event_request__budget',
              type: 'number',
            },
            {
              name: 'ctype_event_request__event_date',
              sql: 'ctype_event_request__event_date',
              type: 'time',
            },
            {
              name: 'ctype_event_request__event_owner',
              sql: 'ctype_event_request__event_owner',
              type: 'string',
            },
            {
              name: 'ctype_event_request__requested_by',
              sql: 'ctype_event_request__requested_by',
              type: 'string',
            },
            {
              name: 'ctype_event_request__target_pipeline',
              sql: 'ctype_event_request__target_pipeline',
              type: 'number',
            },
            {
              name: 'ctype_mfz_subtype_check__user_field',
              sql: 'ctype_mfz_subtype_check__user_field',
              type: 'string',
            },
            {
              name: 'ctype_mfz_subtype_check__workspace_field',
              sql: 'ctype_mfz_subtype_check__workspace_field',
              type: 'string',
            },
            {
              name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__brand_id_cfid',
              sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__brand_id_cfid',
              type: 'number',
            },
            {
              name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__created_at_cfid',
              sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__created_at_cfid',
              type: 'time',
            },
            {
              name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__generated_timestamp_cfid',
              sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__generated_timestamp_cfid',
              type: 'time',
            },
            {
              name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__group_id_cfid',
              sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__group_id_cfid',
              type: 'number',
            },
            {
              name: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__ticket_form_id_cfid',
              sql: 'ctype_pjsy4zdfonxx66tjnxwgk4tjoxyx65djmnxyk5dtfzwvkzltoruy63q__ticket_form_id_cfid',
              type: 'number',
            },
            {
              name: 'ctype_review_subtype__date',
              sql: 'ctype_review_subtype__date',
              type: 'time',
            },
            {
              name: 'ctype_review_subtype__reply_date',
              sql: 'ctype_review_subtype__reply_date',
              type: 'time',
            },
            {
              name: 'ctype_review_subtype__upvote_count',
              sql: 'ctype_review_subtype__upvote_count',
              type: 'number',
            },
            {
              name: 'ctype_jkjkjkjkjkjk__dggd',
              sql: 'ctype_jkjkjkjkjkjk__dggd',
              type: 'number',
            },
            {
              name: 'id_count___function__count',
              function: {
                type: 'count',
              },
              sql: 'count(dim_ticket.id)',
              type: 'string',
            },
          ],
          name: 'dim_ticket',
          sql: "SELECT count(dim_ticket__id) AS dim_ticket__id_count___function__count ,   dim_part__id,  dim_ticket__channels FROM (SELECT dim_ticket.created_date AS dim_ticket__created_date, 'ticket' AS dim_ticket__work_type, json_extract_string(dim_ticket.stage_json, '$.stage_id') AS dim_ticket__stage_json, array[unnest(dim_ticket.channels)] AS dim_ticket__channels, dim_ticket.id AS dim_ticket__id, * FROM (select * from devrev.dim_ticket) AS dim_ticket LEFT JOIN (SELECT dim_part.id AS dim_part__id, * FROM (select id, object_type, created_by_id, created_date, modified_by_id, modified_date, part_category, part_type, owned_by_ids, tags_json, delivered_as from devrev.dim_feature \n union  \n select id, object_type, created_by_id, created_date, modified_by_id, modified_date, part_category, part_type, owned_by_ids, tags_json, delivered_as from devrev.dim_product \n union \n select id, object_type, created_by_id, created_date, modified_by_id, modified_date, part_category, part_type, owned_by_ids, tags_json, delivered_as from devrev.dim_capability \n union \n select id, object_type, created_by_id, created_date, modified_by_id, modified_date, part_category, part_type, owned_by_ids, tags_json, delivered_as from devrev.dim_enhancement\n) AS dim_part) AS dim_part  ON dim_ticket.applies_to_part_id = dim_part.id) AS MEERKAT_GENERATED_TABLE WHERE ((((dim_ticket__created_date >= '2025-07-31T12:30:00.000Z') AND (dim_ticket__created_date <= '2025-10-29T13:29:59.999Z')) AND (dim_ticket__work_type IN ('ticket')) AND (dim_ticket__stage_json IN ('don:core:dvrv-us-1:devo/787:custom_stage/514', 'don:core:dvrv-us-1:devo/787:custom_stage/512', 'don:core:dvrv-us-1:devo/787:custom_stage/501', 'don:core:dvrv-us-1:devo/787:custom_stage/485', 'don:core:dvrv-us-1:devo/787:custom_stage/440', 'don:core:dvrv-us-1:devo/787:custom_stage/25', 'don:core:dvrv-us-1:devo/787:custom_stage/24', 'don:core:dvrv-us-1:devo/787:custom_stage/22', 'don:core:dvrv-us-1:devo/787:custom_stage/21', 'don:core:dvrv-us-1:devo/787:custom_stage/19', 'don:core:dvrv-us-1:devo/787:custom_stage/13', 'don:core:dvrv-us-1:devo/787:custom_stage/10', 'don:core:dvrv-us-1:devo/787:custom_stage/8', 'don:core:dvrv-us-1:devo/787:custom_stage/7', 'don:core:dvrv-us-1:devo/787:custom_stage/6', 'don:core:dvrv-us-1:devo/787:custom_stage/4')))) GROUP BY dim_part__id, dim_ticket__channels",
        },
        {
          dimensions: [
            {
              name: 'dim_part__id',
              sql: 'dim_part__id',
              type: 'string',
            },
            {
              name: 'id',
              sql: 'dim_part.id',
              type: 'string',
            },
            {
              name: 'object_type',
              sql: 'dim_part.object_type',
              type: 'string',
            },
            {
              name: 'created_by_id',
              sql: 'dim_part.created_by_id',
              type: 'string',
            },
            {
              name: 'created_date',
              sql: 'dim_part.created_date',
              type: 'time',
            },
            {
              name: 'modified_by_id',
              sql: 'dim_part.modified_by_id',
              type: 'string',
            },
            {
              name: 'modified_date',
              sql: 'dim_part.modified_date',
              type: 'time',
            },
            {
              name: 'part_category',
              sql: 'dim_part.part_category',
              type: 'number',
            },
            {
              name: 'part_type',
              sql: 'dim_part.part_type',
              type: 'number',
            },
            {
              modifier: {
                shouldUnnestGroupBy: true,
              },
              name: 'owned_by_ids',
              sql: 'dim_part.owned_by_ids',
              type: 'string_array',
            },
            {
              modifier: {
                shouldUnnestGroupBy: true,
              },
              name: 'tags_json',
              sql: "CAST(json_extract_string(dim_part.tags_json, '$[*].tag_id') AS VARCHAR[])",
              type: 'string_array',
            },
            {
              name: 'delivered_as',
              sql: 'dim_part.delivered_as',
              type: 'number',
            },
          ],
          measures: [
            {
              name: 'unique_ids_count',
              sql: 'COUNT(*)',
              type: 'number',
            },
          ],
          name: 'dim_part',
          sql: "SELECT count(dim_ticket__id) AS dim_ticket__id_count___function__count ,   dim_part__id,  dim_ticket__channels FROM (SELECT dim_ticket.created_date AS dim_ticket__created_date, 'ticket' AS dim_ticket__work_type, json_extract_string(dim_ticket.stage_json, '$.stage_id') AS dim_ticket__stage_json, array[unnest(dim_ticket.channels)] AS dim_ticket__channels, dim_ticket.id AS dim_ticket__id, * FROM (select * from devrev.dim_ticket) AS dim_ticket LEFT JOIN (SELECT dim_part.id AS dim_part__id, * FROM (select id, object_type, created_by_id, created_date, modified_by_id, modified_date, part_category, part_type, owned_by_ids, tags_json, delivered_as from devrev.dim_feature \n union  \n select id, object_type, created_by_id, created_date, modified_by_id, modified_date, part_category, part_type, owned_by_ids, tags_json, delivered_as from devrev.dim_product \n union \n select id, object_type, created_by_id, created_date, modified_by_id, modified_date, part_category, part_type, owned_by_ids, tags_json, delivered_as from devrev.dim_capability \n union \n select id, object_type, created_by_id, created_date, modified_by_id, modified_date, part_category, part_type, owned_by_ids, tags_json, delivered_as from devrev.dim_enhancement\n) AS dim_part) AS dim_part  ON dim_ticket.applies_to_part_id = dim_part.id) AS MEERKAT_GENERATED_TABLE WHERE ((((dim_ticket__created_date >= '2025-07-31T12:30:00.000Z') AND (dim_ticket__created_date <= '2025-10-29T13:29:59.999Z')) AND (dim_ticket__work_type IN ('ticket')) AND (dim_ticket__stage_json IN ('don:core:dvrv-us-1:devo/787:custom_stage/514', 'don:core:dvrv-us-1:devo/787:custom_stage/512', 'don:core:dvrv-us-1:devo/787:custom_stage/501', 'don:core:dvrv-us-1:devo/787:custom_stage/485', 'don:core:dvrv-us-1:devo/787:custom_stage/440', 'don:core:dvrv-us-1:devo/787:custom_stage/25', 'don:core:dvrv-us-1:devo/787:custom_stage/24', 'don:core:dvrv-us-1:devo/787:custom_stage/22', 'don:core:dvrv-us-1:devo/787:custom_stage/21', 'don:core:dvrv-us-1:devo/787:custom_stage/19', 'don:core:dvrv-us-1:devo/787:custom_stage/13', 'don:core:dvrv-us-1:devo/787:custom_stage/10', 'don:core:dvrv-us-1:devo/787:custom_stage/8', 'don:core:dvrv-us-1:devo/787:custom_stage/7', 'don:core:dvrv-us-1:devo/787:custom_stage/6', 'don:core:dvrv-us-1:devo/787:custom_stage/4')))) GROUP BY dim_part__id, dim_ticket__channels",
        },
      ];
      const cubeQuery: Query = {
        dimensions: ['dim_part.dim_part__id'],
        measures: ['dim_part.unique_ids_count'],
        order: {
          'dim_part.unique_ids_count': 'desc',
        },
        limit: 5,
        offset: 0,
        filters: [
          {
            and: [],
          },
        ],
      };
      const result = getCombinedTableSchema(tableSchemas, cubeQuery);
      expect(result).toEqual({
        dimensions: [
          {
            name: 'dim_part__id',
            sql: 'dim_part__id',
            type: 'string',
          },
          {
            name: 'id',
            sql: 'dim_part.id',
            type: 'string',
          },
          {
            name: 'object_type',
            sql: 'dim_part.object_type',
            type: 'string',
          },
          {
            name: 'created_by_id',
            sql: 'dim_part.created_by_id',
            type: 'string',
          },
          {
            name: 'created_date',
            sql: 'dim_part.created_date',
            type: 'time',
          },
          {
            name: 'modified_by_id',
            sql: 'dim_part.modified_by_id',
            type: 'string',
          },
          {
            name: 'modified_date',
            sql: 'dim_part.modified_date',
            type: 'time',
          },
          {
            name: 'part_category',
            sql: 'dim_part.part_category',
            type: 'number',
          },
          {
            name: 'part_type',
            sql: 'dim_part.part_type',
            type: 'number',
          },
          {
            modifier: {
              shouldUnnestGroupBy: true,
            },
            name: 'owned_by_ids',
            sql: 'dim_part.owned_by_ids',
            type: 'string_array',
          },
          {
            modifier: {
              shouldUnnestGroupBy: true,
            },
            name: 'tags_json',
            sql: "CAST(json_extract_string(dim_part.tags_json, '$[*].tag_id') AS VARCHAR[])",
            type: 'string_array',
          },
          {
            name: 'delivered_as',
            sql: 'dim_part.delivered_as',
            type: 'number',
          },
        ],
        measures: [
          {
            name: 'unique_ids_count',
            sql: 'COUNT(*)',
            type: 'number',
          },
        ],
        name: 'dim_part',
        sql: "SELECT count(dim_ticket__id) AS dim_ticket__id_count___function__count ,   dim_part__id,  dim_ticket__channels FROM (SELECT dim_ticket.created_date AS dim_ticket__created_date, 'ticket' AS dim_ticket__work_type, json_extract_string(dim_ticket.stage_json, '$.stage_id') AS dim_ticket__stage_json, array[unnest(dim_ticket.channels)] AS dim_ticket__channels, dim_ticket.id AS dim_ticket__id, * FROM (select * from devrev.dim_ticket) AS dim_ticket LEFT JOIN (SELECT dim_part.id AS dim_part__id, * FROM (select id, object_type, created_by_id, created_date, modified_by_id, modified_date, part_category, part_type, owned_by_ids, tags_json, delivered_as from devrev.dim_feature \n union  \n select id, object_type, created_by_id, created_date, modified_by_id, modified_date, part_category, part_type, owned_by_ids, tags_json, delivered_as from devrev.dim_product \n union \n select id, object_type, created_by_id, created_date, modified_by_id, modified_date, part_category, part_type, owned_by_ids, tags_json, delivered_as from devrev.dim_capability \n union \n select id, object_type, created_by_id, created_date, modified_by_id, modified_date, part_category, part_type, owned_by_ids, tags_json, delivered_as from devrev.dim_enhancement\n) AS dim_part) AS dim_part  ON dim_ticket.applies_to_part_id = dim_part.id) AS MEERKAT_GENERATED_TABLE WHERE ((((dim_ticket__created_date >= '2025-07-31T12:30:00.000Z') AND (dim_ticket__created_date <= '2025-10-29T13:29:59.999Z')) AND (dim_ticket__work_type IN ('ticket')) AND (dim_ticket__stage_json IN ('don:core:dvrv-us-1:devo/787:custom_stage/514', 'don:core:dvrv-us-1:devo/787:custom_stage/512', 'don:core:dvrv-us-1:devo/787:custom_stage/501', 'don:core:dvrv-us-1:devo/787:custom_stage/485', 'don:core:dvrv-us-1:devo/787:custom_stage/440', 'don:core:dvrv-us-1:devo/787:custom_stage/25', 'don:core:dvrv-us-1:devo/787:custom_stage/24', 'don:core:dvrv-us-1:devo/787:custom_stage/22', 'don:core:dvrv-us-1:devo/787:custom_stage/21', 'don:core:dvrv-us-1:devo/787:custom_stage/19', 'don:core:dvrv-us-1:devo/787:custom_stage/13', 'don:core:dvrv-us-1:devo/787:custom_stage/10', 'don:core:dvrv-us-1:devo/787:custom_stage/8', 'don:core:dvrv-us-1:devo/787:custom_stage/7', 'don:core:dvrv-us-1:devo/787:custom_stage/6', 'don:core:dvrv-us-1:devo/787:custom_stage/4')))) GROUP BY dim_part__id, dim_ticket__channels",
      });
    });
  });
});
