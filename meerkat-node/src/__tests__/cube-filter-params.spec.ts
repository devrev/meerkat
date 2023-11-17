import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';
const SCHEMA = {
  name: 'orders',
  sql: "select * from orders WHERE ${FILTER_PARAMS.orders.status.filter('status')}",
  measures: [],
  dimensions: [
    {
      name: 'id',
      sql: 'id',
      type: 'number',
    },
    {
      name: 'date',
      sql: 'date',
      type: 'string',
    },
    {
      name: 'status',
      sql: 'status',
      type: 'string',
    },
    {
      name: 'amount',
      sql: 'amount',
      type: 'number',
    },
  ],
};

const SCHEMA2 = {
  name: 'support_insights_ticket_metrics_summary',
  sql: "SELECT DISTINCT ON (id) id, title, stage_id, account_id, severity_name, created_date, created_by_id, primary_part_id, tag_ids, group_id, sla_stage, rev_oid, record_hour, DATE_TRUNC('day', record_hour) AS record_date, UNNEST(owned_by_ids) as owned_by_ids FROM support_insights_ticket_metrics_summary WHERE state != 'closed' AND ${FILTER_PARAMS.support_insights_ticket_metrics_summary.record_date.filter('record_date')} ORDER BY record_date DESC",
  measures: [],
  dimensions: [
    {
      name: 'record_date',
      sql: 'date',
      type: 'string',
    },
  ],
};
describe('filter-param-tests', () => {
  beforeAll(async () => {
    await duckdbExec(`CREATE TABLE orders(
        id INTEGER PRIMARY KEY,
        date DATE,
        status VARCHAR,
        amount DECIMAL
    );`);
    await duckdbExec(`INSERT INTO orders(id, date, status, amount)
      VALUES (1, DATE '2022-01-21', 'completed', 99.99),
             (2, DATE '2022-02-10', 'completed', 200.50),
             (3, DATE '2022-01-25', 'completed', 150.00),
             (4, DATE '2022-03-01', 'cancelled', 40.00),
             (5, DATE '2022-01-28', 'completed', 80.75),
             (6, DATE '2022-02-15', 'pending', 120.00),
             (7, DATE '2022-04-01', 'completed', 210.00);`);
  });

  it('Should apply filter params to base SQL temp', async () => {
    const query = {
      measures: ['*'],
      filters: [
        {
          member: 'support_insights_ticket_metrics_summary.record_date',
          operator: 'equals',
          values: ['2022-01-25'],
        },
      ],
      dimensions: [],
    };
    const sql = await cubeQueryToSQL(query, SCHEMA2);
    console.info('SQL: ', sql);
    // const output: any = await duckdbExec(sql);
    // expect(output).toHaveLength(1);
    // expect(output[0].id).toBe(6);
  });

  it('Should apply filter params to base SQL', async () => {
    const query = {
      measures: ['*'],
      filters: [
        {
          member: 'orders.status',
          operator: 'equals',
          values: ['pending'],
        },
      ],
      dimensions: [],
    };
    const sql = await cubeQueryToSQL(query, SCHEMA);
    console.info('SQL: ', sql);
    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(1);
    expect(output[0].id).toBe(6);
  });

  it('Should apply multiple filter params to base SQL', async () => {
    const query = {
      measures: ['*'],
      filters: [
        {
          or: [
            {
              member: 'orders.status',
              operator: 'equals',
              values: ['pending'],
            },
            {
              member: 'orders.status',
              operator: 'equals',
              values: ['cancelled'],
            },
            {
              member: 'orders.amount',
              operator: 'gt',
              values: ['40'],
            },
          ],
        },
      ],
      dimensions: [],
    };
    const sql = await cubeQueryToSQL(query, SCHEMA);
    console.info('SQL: ', sql);
    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(2);
    expect(output[0].id).toBe(6);
  });

  it('Should apply true filter if filters are not matching', async () => {
    const query = {
      measures: ['*'],
      filters: [],
      dimensions: [],
    };

    const sql = await cubeQueryToSQL(query, SCHEMA);
    console.info('SQL: ', sql);
    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(7);
  });
});
