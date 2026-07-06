import { Query, TableSchema } from '@devrev/meerkat-core';

/**
 * Builds a schema shaped like the production `dim_issue` cube: ~360 dimensions
 * (plain columns, json_extract_string on custom_fields, string_array members
 * with shouldUnnestGroupBy, computed CASE/epoch expressions) plus ~72 measures,
 * with a small fixed projection. Used by the wide-schema perf specs.
 */
const DIM_TEMPLATES = [
  (i: number) => ({ name: `col_${i}`, sql: `col_${i}`, type: 'string' }),
  (i: number) => ({
    name: `json_${i}`,
    sql: `json_extract_string(custom_fields, '$.ctype__field_${i}')`,
    type: 'string',
  }),
  (i: number) => ({
    modifier: { shouldUnnestGroupBy: false },
    name: `arr_${i}`,
    sql: `CAST(json_extract_string(custom_fields, '$.arr_${i}') AS VARCHAR[])`,
    type: 'string_array',
  }),
  (i: number) => ({
    modifier: { shouldUnnestGroupBy: false },
    name: `links_${i}`,
    sql: `json_extract_string(links_json, '$[*].f_${i}')`,
    type: 'string_array',
  }),
  (i: number) => ({
    name: `computed_${i}`,
    sql: `case WHEN actual_close_date > created_date THEN epoch_ms(actual_close_date) - epoch_ms(created_date) ELSE null END`,
    type: 'number',
  }),
];

export const buildDimIssueFixture = (): { schema: TableSchema; query: Query } => {
  const dimensions: any[] = [];
  for (let i = 0; i < 360; i += 1) {
    dimensions.push(DIM_TEMPLATES[i % DIM_TEMPLATES.length](i));
  }
  dimensions.push({ name: 'created_date', sql: 'created_date', type: 'time' });
  dimensions.push({ name: 'id', sql: 'id', type: 'string' });
  dimensions.push({ name: 'space_id', sql: 'space_id', type: 'string' });
  dimensions.push({ name: 'subtype', sql: 'subtype', type: 'string' });
  dimensions.push({ name: 'title', sql: 'title', type: 'string' });
  dimensions.push({ name: 'display_id', sql: 'display_id', type: 'string' });
  dimensions.push({ name: 'target_close_date', sql: 'target_close_date', type: 'time' });
  dimensions.push({ name: 'links_json', sql: 'links_json', type: 'string' });
  dimensions.push({ name: 'sla_summary', sql: 'sla_summary', type: 'string' });
  dimensions.push({
    modifier: { shouldUnnestGroupBy: false },
    name: 'owned_by_ids',
    sql: 'CAST(owned_by_ids AS VARCHAR[])',
    type: 'string_array',
  });
  dimensions.push({
    modifier: { shouldUnnestGroupBy: false },
    name: 'custom_schema_fragment_ids',
    sql: 'CAST(custom_schema_fragment_ids AS VARCHAR[])',
    type: 'string_array',
  });
  dimensions.push({
    modifier: { shouldUnnestGroupBy: false },
    name: 'links_json_$0_target_object_type',
    sql: "json_extract_string(links_json, '$[*].target_object_type')",
    type: 'string_array',
  });
  dimensions.push({
    name: 'priority_uenum_json',
    sql: "CAST(json_extract_string(priority_uenum_json, '$.id') AS INT)",
    type: 'string',
  });
  dimensions.push({
    name: 'stage_json_$0_stage_id',
    sql: "json_extract_string(stage_json, '$.stage_id')",
    type: 'string',
  });
  dimensions.push({ name: '__fdl_row_order__', sql: '__fdl_row_order__', type: 'number' });

  const measures: any[] = [{ name: 'count_star', sql: 'COUNT(*)', type: 'number' }];
  for (let i = 0; i < 71; i += 1) {
    measures.push({
      name: `m_json_${i}`,
      sql: `json_extract_string(custom_fields, '$.ctype__m_${i}')`,
      type: 'number',
    });
  }

  const schema: TableSchema = {
    name: 'dim_issue',
    sql: 'SELECT * FROM dim_issue',
    measures,
    dimensions,
  };

  const query = {
    dimensions: [
      'dim_issue.created_date',
      'dim_issue.id',
      'dim_issue.links_json_$0_target_object_type',
      'dim_issue.owned_by_ids',
      'dim_issue.priority_uenum_json',
      'dim_issue.sla_summary',
      'dim_issue.space_id',
      'dim_issue.stage_json_$0_stage_id',
      'dim_issue.subtype',
      'dim_issue.target_close_date',
      'dim_issue.title',
      'dim_issue.links_json',
      'dim_issue.custom_schema_fragment_ids',
      'dim_issue.display_id',
    ],
    measures: [],
    filters: [{ and: [] }],
    order: { 'dim_issue.__fdl_row_order__': 'asc' },
  } as unknown as Query;

  return { schema, query };
};
