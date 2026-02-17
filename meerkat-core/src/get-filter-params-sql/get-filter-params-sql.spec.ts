import { Database } from 'duckdb';
import { TableSchema } from '../types/cube-types';
import { getFilterParamsSQL } from './get-filter-params-sql';

const getQueryOutput = async (sql: string) => {
  const db = new Database(':memory:');
  return new Promise((resolve, reject) => {
    db.all(sql, (err, res) => {
      if (err) {
        reject(err);
      }
      resolve(res);
    });
  });
};

const TABLE_SCHEMA: TableSchema = {
  name: 'orders',
  sql: "select * from orders WHERE ${FILTER_PARAMS.orders.status.filter('status')}",
  measures: [],
  dimensions: [
    { name: 'id', sql: 'id', type: 'number' },
    { name: 'date', sql: 'date', type: 'string' },
    { name: 'status', sql: 'status', type: 'string' },
    { name: 'amount', sql: 'amount', type: 'number' },
  ],
};

describe('getFilterParamsSQL', () => {
  it('should find filter params when there are filters of base filter type', async () => {
    const result = await getFilterParamsSQL({
      filterType: 'BASE_FILTER',
      query: {
        measures: ['*'],
        filters: [
          {
            and: [
              { member: 'orders.amount', operator: 'notSet' },
              { member: 'orders.status', operator: 'set' },
            ],
          },
        ],
        dimensions: [],
      },
      tableSchema: TABLE_SCHEMA,
      getQueryOutput,
    });
    expect(result).toEqual([
      {
        matchKey: "${FILTER_PARAMS.orders.status.filter('status')}",
        memberKey: 'orders.status',
        sql: 'SELECT * FROM REPLACE_BASE_TABLE WHERE ((orders.status IS NOT NULL))',
      },
    ]);
  });

  it('should not find filter params when there are no filters of base filter type', async () => {
    const result = await getFilterParamsSQL({
      filterType: 'BASE_FILTER',
      query: {
        measures: ['*'],
        filters: [],
        dimensions: [],
      },
      tableSchema: TABLE_SCHEMA,
      getQueryOutput,
    });
    expect(result).toEqual([]);
  });

  it('should find filter params when there are filters of projection filter type', async () => {
    const result = await getFilterParamsSQL({
      filterType: 'PROJECTION_FILTER',
      query: {
        measures: ['*'],
        filters: [
          {
            and: [
              { member: 'orders.amount', operator: 'notSet' },
              { member: 'orders.status', operator: 'set' },
            ],
          },
        ],
        dimensions: [],
      },
      tableSchema: TABLE_SCHEMA,
      getQueryOutput,
    });
    expect(result).toEqual([
      {
        matchKey: "${FILTER_PARAMS.orders.status.filter('status')}",
        memberKey: 'orders.status',
        sql: 'SELECT * FROM REPLACE_BASE_TABLE WHERE ((orders__status IS NOT NULL))',
      },
    ]);
  });

  it('should not find filter params when there are no filters', async () => {
    const result = await getFilterParamsSQL({
      filterType: 'PROJECTION_FILTER',
      query: {
        measures: ['*'],
        filters: [],
        dimensions: [],
      },
      tableSchema: TABLE_SCHEMA,
      getQueryOutput,
    });
    expect(result).toEqual([]);
  });

  it('should apply aliases', async () => {
    const tableSchema: TableSchema = {
      ...TABLE_SCHEMA,
      dimensions: [
        {
          name: 'status',
          sql: 'status',
          type: 'string',
          alias: 'Order Status',
        },
        {
          name: 'amount',
          sql: 'amount',
          type: 'number',
          alias: 'Order Amount',
        },
      ],
    };

    const result = await getFilterParamsSQL({
      filterType: 'PROJECTION_FILTER',
      query: {
        measures: ['*'],
        filters: [
          {
            and: [
              { member: 'orders.amount', operator: 'notSet' },
              { member: 'orders.status', operator: 'set' },
            ],
          },
        ],
        dimensions: [],
      },
      tableSchema: tableSchema,
      getQueryOutput,
    });
    expect(result).toEqual([
      {
        matchKey: "${FILTER_PARAMS.orders.status.filter('status')}",
        memberKey: 'orders.status',
        sql: 'SELECT * FROM REPLACE_BASE_TABLE WHERE (("Order Status" IS NOT NULL))',
      },
    ]);
  });
});
