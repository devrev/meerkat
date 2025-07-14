import { Database } from 'duckdb';
import { TableSchema } from '../types/cube-types';
import { getFinalBaseSQL } from './get-final-base-sql';

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
  measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
  dimensions: [
    { name: 'id', sql: 'id', type: 'number' },
    { name: 'date', sql: 'date', type: 'string' },
    { name: 'status', sql: 'status', type: 'string' },
    { name: 'amount', sql: 'amount', type: 'number' },
  ],
};
describe('get final base sql', () => {
  it('should not return measures in the projected base sql when filter param passed', async () => {
    const result = await getFinalBaseSQL({
      query: {
        measures: ['orders.count'],
        filters: [
          {
            and: [
              { member: 'orders.amount', operator: 'notSet' },
              { member: 'orders.status', operator: 'set' },
            ],
          },
        ],
        dimensions: ['orders.status'],
      },
      tableSchema: TABLE_SCHEMA,
      getQueryOutput,
    });
    expect(result).toEqual(
      'SELECT *, amount AS orders__amount, status AS orders__status FROM (select * from orders WHERE  ((orders.status IS NOT NULL))) AS orders'
    );
  });
  it('should not return measures in the projected base sql when filter param not passed', async () => {
    const result = await getFinalBaseSQL({
      query: {
        measures: ['orders.count'],
        filters: [
          {
            and: [{ member: 'orders.amount', operator: 'notSet' }],
          },
        ],
        dimensions: ['orders.status'],
      },
      tableSchema: TABLE_SCHEMA,
      getQueryOutput,
    });
    expect(result).toEqual(
      'SELECT *, amount AS orders__amount, status AS orders__status FROM (select * from orders WHERE TRUE) AS orders'
    );
  });

  it('should use aliases', async () => {
    const result = await getFinalBaseSQL({
      query: {
        measures: ['orders.count'],
        filters: [
          {
            and: [
              { member: 'orders.amount', operator: 'notSet' },
              { member: 'orders.status', operator: 'set' },
            ],
          },
        ],
        dimensions: ['orders.status'],
      },
      tableSchema: TABLE_SCHEMA,
      aliases: {
        'orders.count': 'total orders',
        'orders.status': 'order status',
        'orders.amount': 'order amount',
      },
      getQueryOutput,
    });
    expect(result).toEqual(
      'SELECT *, amount AS "order amount", status AS "order status" FROM (select * from orders WHERE  ((orders.status IS NOT NULL))) AS orders'
    );
  });
});
