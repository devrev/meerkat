import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';
import {
  CREATE_TEST_TABLE,
  INPUT_DATA_QUERY,
  TABLE_SCHEMA,
  TEST_DATA,
} from './test-data';
describe('cube-to-sql', () => {
  beforeAll(async () => {
    // Create orders table
    await duckdbExec(CREATE_TEST_TABLE);

    // Insert data into orders table
    await duckdbExec(INPUT_DATA_QUERY);
  });

  for (const data of TEST_DATA) {
    it(`Testing ${data.testName}`, async () => {
      const sql = await cubeQueryToSQL({
        query: data.cubeInput,
        tableSchemas: [TABLE_SCHEMA],
      });
      expect(sql).toEqual(data.expectedSQL);
      console.info(`SQL for ${data.testName}: `, sql);
      //TODO: Remove order by
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));
      const formattedOutput = parsedOutput.map((row) => {
        if (!row.order_date) {
          return row;
        }
        return {
          ...row,
          order_date: new Date(row.order_date).toISOString(),
          orders__order_date: row.orders__order_date
            ? new Date(row.orders__order_date).toISOString()
            : undefined,
        };
      });
      const expectedOutput = data.expectedOutput.map((row) => {
        if (!row.order_date) {
          return row;
        }
        return {
          ...row,
          order_date: new Date(row.order_date).toISOString(),
          orders__order_date: row.orders__order_date
            ? new Date(row.orders__order_date).toISOString()
            : undefined,
        };
      });

      /**
       * Compare the output with the expected output
       */
      expect(formattedOutput).toStrictEqual(expectedOutput);
      /**
       * Compare expect SQL with the generated SQL
       */
      expect(sql).toBe(data.expectedSQL);
    });
  }

  it('Should order the projected value', async () => {
    const query = {
      measures: ['orders.total_order_amount'],
      filters: [],
      dimensions: ['orders.customer_id'],
      order: {
        'orders.total_order_amount': 'desc',
      },
      limit: 2,
    };
    const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA] });
    console.info(`SQL for Simple Cube Query: `, sql);
    const output = await duckdbExec(sql);
    const parsedOutput = JSON.parse(JSON.stringify(output));
    console.info('parsedOutput', parsedOutput);
    expect(parsedOutput[0].orders__total_order_amount).toBeGreaterThan(
      parsedOutput[1].orders__total_order_amount
    );
  });

  it('Without filter query generator with empty and', async () => {
    const query = {
      measures: ['*'],
      filters: [
        {
          and: [],
        },
      ],
      dimensions: [],
    };
    const sql = await cubeQueryToSQL({
      query: query,
      tableSchemas: [TABLE_SCHEMA],
    });
    console.info(`SQL for Simple Cube Query: `, sql);
    expect(sql).toEqual(
      'SELECT orders.* FROM (SELECT * FROM (select * from orders) AS orders) AS orders'
    );
  });

  it('Without filter query generator with empty or', async () => {
    const query = {
      measures: ['*'],
      filters: [
        {
          or: [],
        },
      ],
      dimensions: [],
    };
    const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA] });
    console.info(`SQL for Simple Cube Query: `, sql);
    expect(sql).toEqual(
      'SELECT orders.* FROM (SELECT * FROM (select * from orders) AS orders) AS orders'
    );
  });

  it('Without filter query generator with empty and', async () => {
    const { query, sql, tableSchemas } = {
      query: {
        dimensions: ['enhanhancement_state_info.owners'],
        limit: 10,
        measures: ['enhanhancement_state_info.unique_ids_count'],
        filters: [
          {
            and: [],
          },
        ],
      },
      sql: "SELECT COUNT(DISTINCT id) AS enhanhancement_state_info__unique_ids_count, enhanhancement_state_info__owners  FROM (SELECT * FROM (WITH date_vals AS ( SELECT * FROM generate_series(DATE '2020-01-01', DATE '2040-12-31', INTERVAL 1 DAY) AS date_series(record_date) ), max_date_val AS ( SELECT MAX(record_date) AS max_date FROM date_vals AS enhanhancement_state_info WHERE record_date <= CURRENT_TIMESTAMP AND TRUE), enhanhancement_state_info AS ( SELECT *, modified_date AS record_date FROM system.enhanhancement_state_tracker ), final_enhancement_state AS ( SELECT id, MAX_BY(state, modified_date) AS final_state FROM enhanhancement_state_info AS s CROSS JOIN max_date_val AS m WHERE s.modified_date <= m.max_date GROUP BY 1 ), owners AS ( SELECT id, UNNEST(owned_by_ids) AS owner FROM system.dim_enhancement), issues_info AS ( SELECT *, UNNEST(code_changes) AS code_change_id FROM system.capital_allocation_issues_latest_daily ), contribution_view AS ( SELECT record_date, type, i.part_id FROM system.capital_allocation_daily AS cad LEFT JOIN issues_info AS i ON i.code_change_id = cad.id WHERE object_type = 'code_change' UNION ALL SELECT record_date, type, i.part_id FROM system.capital_allocation_daily AS cad LEFT JOIN system.capital_allocation_issues_latest_daily AS i ON i.issue_id = cad.id WHERE object_type = 'issue' ), last_date_worked_on AS ( SELECT part_id AS id, MAX(ca.record_date) AS worked_date FROM contribution_view AS ca LEFT JOIN system.capi_category_table AS c ON ca.type = c.type CROSS JOIN max_date_val AS cd WHERE c.category = 'code' AND part_id LIKE '%enhancement%' AND ca.record_date <= cd.max_date GROUP BY 1) SELECT o.id, COALESCE(o.target_close_date, tcds[-1]) AS target_close_date_val, UNNEST(o.owned_by_ids) AS user, o.id AS part_id, d.*, m.max_date AS modified_date, la.worked_date, len(tcds) AS tcds_count, CASE WHEN tcds[1] < target_close_date AND max_date > target_close_date THEN DATE_SUB('second', tcds[1], max_date) WHEN tcds[1] < target_close_date AND max_date <= target_close_date THEN DATE_SUB('second', tcds[1], target_close_date) WHEN tcds[1] >= target_close_date AND max_date > target_close_date THEN DATE_SUB('second', target_close_date, max_date) ELSE NULL END AS delay_time FROM system.dim_enhancement AS o JOIN final_enhancement_state AS f ON f.id = o.id LEFT JOIN system.enhanhancement_tcd_tracker AS pbt ON pbt.id = o.id CROSS JOIN max_date_val AS m LEFT JOIN owners ON o.id = owners.id LEFT JOIN system.capital_allocation_devu_extension AS d ON d.user_id = owners.owner LEFT JOIN last_date_worked_on AS la ON la.id = o.id WHERE f.final_state = 'in-progress' AND delay_time IS NOT NULL AND target_close_date IS NOT NULL) AS enhanhancement_state_info) AS enhanhancement_state_info GROUP BY enhanhancement_state_info__owners LIMIT 10",
      tableSchemas: [
        {
          dimensions: [
            {
              name: 'record_date',
              sql: 'modified_date',
              type: 'time',
            },
            {
              name: 'part_id',
              sql: 'part_id',
              type: 'string',
            },
            {
              name: 'user_id',
              sql: 'user',
              type: 'string',
            },
            {
              modifier: {
                shouldUnnestGroupBy: true,
              },
              name: 'group_ids',
              sql: 'group_ids',
              type: 'string_array',
            },
            {
              name: 'title',
              sql: 'title',
              type: 'string',
            },
            {
              name: 'location',
              sql: 'location',
              type: 'string',
            },
            {
              name: 'employment_status',
              sql: 'employment_status',
              type: 'string',
            },
            {
              name: 'experience_start_date',
              sql: 'experience_start_date',
              type: 'time',
            },
            {
              name: 'persona',
              sql: 'job_title',
              type: 'string',
            },
            {
              modifier: {
                shouldUnnestGroupBy: true,
              },
              name: 'skills',
              sql: 'skills',
              type: 'string_array',
            },
            {
              name: 'enhancement_category',
              sql: 'enhancement_category',
              type: 'string',
            },
          ],
          measures: [
            {
              name: 'unique_ids_count',
              sql: 'COUNT(DISTINCT id)',
              type: 'number',
            },
            {
              name: 'last_coded',
              sql: 'ANY_VALUE(worked_date)',
              type: 'time',
            },
            {
              name: 'target_date',
              sql: 'ANY_VALUE(target_close_date_val)',
              type: 'time',
            },
            {
              name: 'delay_time',
              sql: 'ANY_VALUE(delay_time)',
              type: 'number',
            },
            {
              name: 'tcds_count',
              sql: 'ANY_VALUE(tcds_count)',
              type: 'number',
            },
            {
              name: 'owners',
              sql: 'ARRAY_AGG(DISTINCT user)',
              type: 'string',
            },
          ],
          name: 'enhanhancement_state_info',
          sql: "WITH date_vals AS ( SELECT * FROM generate_series(DATE '2020-01-01', DATE '2040-12-31', INTERVAL 1 DAY) AS date_series(record_date) ), max_date_val AS ( SELECT MAX(record_date) AS max_date FROM date_vals AS enhanhancement_state_info WHERE record_date <= CURRENT_TIMESTAMP AND ${FILTER_PARAMS.enhanhancement_state_info.record_date.filter('record_date')}), enhanhancement_state_info AS ( SELECT *, modified_date AS record_date FROM system.enhanhancement_state_tracker ), final_enhancement_state AS ( SELECT id, MAX_BY(state, modified_date) AS final_state FROM enhanhancement_state_info AS s CROSS JOIN max_date_val AS m WHERE s.modified_date <= m.max_date GROUP BY 1 ), owners AS ( SELECT id, UNNEST(owned_by_ids) AS owner FROM system.dim_enhancement), issues_info AS ( SELECT *, UNNEST(code_changes) AS code_change_id FROM system.capital_allocation_issues_latest_daily ), contribution_view AS ( SELECT record_date, type, i.part_id FROM system.capital_allocation_daily AS cad LEFT JOIN issues_info AS i ON i.code_change_id = cad.id WHERE object_type = 'code_change' UNION ALL SELECT record_date, type, i.part_id FROM system.capital_allocation_daily AS cad LEFT JOIN system.capital_allocation_issues_latest_daily AS i ON i.issue_id = cad.id WHERE object_type = 'issue' ), last_date_worked_on AS ( SELECT part_id AS id, MAX(ca.record_date) AS worked_date FROM contribution_view AS ca LEFT JOIN system.capi_category_table AS c ON ca.type = c.type CROSS JOIN max_date_val AS cd WHERE c.category = 'code' AND part_id LIKE '%enhancement%' AND ca.record_date <= cd.max_date GROUP BY 1) SELECT o.id, COALESCE(o.target_close_date, tcds[-1]) AS target_close_date_val, UNNEST(o.owned_by_ids) AS user, o.id AS part_id, d.*, m.max_date AS modified_date, la.worked_date, len(tcds) AS tcds_count, CASE WHEN tcds[1] < target_close_date AND max_date > target_close_date THEN DATE_SUB('second', tcds[1], max_date) WHEN tcds[1] < target_close_date AND max_date <= target_close_date THEN DATE_SUB('second', tcds[1], target_close_date) WHEN tcds[1] >= target_close_date AND max_date > target_close_date THEN DATE_SUB('second', target_close_date, max_date) ELSE NULL END AS delay_time FROM system.dim_enhancement AS o JOIN final_enhancement_state AS f ON f.id = o.id LEFT JOIN system.enhanhancement_tcd_tracker AS pbt ON pbt.id = o.id CROSS JOIN max_date_val AS m LEFT JOIN owners ON o.id = owners.id LEFT JOIN system.capital_allocation_devu_extension AS d ON d.user_id = owners.owner LEFT JOIN last_date_worked_on AS la ON la.id = o.id WHERE f.final_state = 'in-progress' AND delay_time IS NOT NULL AND target_close_date IS NOT NULL",
        },
      ],
    };
    const resultSql = await cubeQueryToSQL({
      query,
      tableSchemas: tableSchemas,
    });
    console.info(`SQL for Simple Cube Query: `, resultSql);
    expect(resultSql).toEqual(sql);
  });
});
