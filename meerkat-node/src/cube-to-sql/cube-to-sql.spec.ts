import { duckdbExec } from '../duckdb-exec';
import { cubeQueryToSQL } from './cube-to-sql';
describe('cube-to-sql', () => {
  it('should work', async () => {
    const temp = await duckdbExec(
      ` SELECT json_deserialize_sql('{"statements":[{"node":{"type":"SELECT_NODE","modifiers":[],"cte_map":{"map":[]},"select_list":[{"class":"STAR","type":"STAR","alias":"","relation_name":"","exclude_list":[],"replace_list":[],"columns":false,"expr":null}],"from_table":{"type":"BASE_TABLE","alias":"","sample":null,"schema_name":"","table_name":"REPLACE_BASE_TABLE","column_name_alias":[],"catalog_name":""},"where_clause":{"class":"COMPARISON","type":"COMPARE_EQUAL","alias":"","left":{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["owned_by"]},"right":{"class":"CONSTANT","type":"VALUE_CONSTANT","alias":"","value":{"type":{"id":"DECIMAL","type_info":{"type":"DECIMAL_TYPE_INFO","alias":"","width":3,"scale":0}},"is_null":false,"value":null}}},"group_expressions":[],"group_sets":[],"aggregate_handling":"STANDARD_HANDLING","having":null,"sample":null,"qualify":null}}]}');`
    );
    expect(temp).toBeTruthy();
  });

  it('Generate SQL from cube query', async () => {
    const body = {
      table_schema: {
        cube: 'select * from dim_issue',
        measures: [
          {
            sql: 'base.test',
            type: 'number',
          },
        ],
        dimensions: [
          {
            sql: 'base.actual_close_date',
            type: 'time',
          },
          {
            sql: 'base.type',
            type: 'string',
          },
          {
            sql: 'base.applies_to_part',
            type: 'string',
          },
          {
            sql: 'base.modified_date',
            type: 'time',
          },
          {
            sql: 'base.owned_by',
            type: 'string',
          },
        ],
      },
      cube: {
        filters: [
          {
            and: [
              {
                member: 'modified_date',
                operator: 'inDateRange',
                values: [
                  '2023-09-21T14:20:37.951Z',
                  '2023-09-14T14:20:37.951Z',
                ],
              },
              {
                or: [
                  {
                    member: 'owned_by',
                    operator: 'equals',
                    values: ['don:identity:dvrv-us-1:devo/0:devu/102'],
                  },
                ],
              },
              {
                or: [
                  {
                    member: 'type',
                    operator: 'equals',
                    values: ['issue'],
                  },
                ],
              },
            ],
          },
        ],
      },
    };
    const sql = await cubeQueryToSQL(body.cube, body.table_schema);
    expect(sql).toBe(
      `SELECT * FROM (select * from dim_issue) WHERE (((modified_date >= '2023-09-21T14:20:37.951Z') AND (modified_date <= '2023-09-14T14:20:37.951Z')) AND ((owned_by = 'don:identity:dvrv-us-1:devo/0:devu/102')) AND ((\"type\" = 'issue')))`
    );
  });
});
