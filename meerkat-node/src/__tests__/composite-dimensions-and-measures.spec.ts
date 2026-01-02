import { Query } from 'meerkat-core/src/types/cube-types/query';
import { TableSchema } from 'meerkat-core/src/types/cube-types/table';
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

const TABLE_SCHEMA: TableSchema = {
  name: 'orders',
  sql: 'select * from orders',
  measures: [
    {
      name: 'total.order.amount',
      sql: 'SUM(order_amount)',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'customer.region.name',
      sql: "json_extract_string(customer_data, '$.region.name')",
      type: 'string',
    },
    {
      name: 'customer.preferences.category',
      sql: "json_extract_string(customer_data, '$.preferences.category')",
      type: 'string',
    },
    {
      name: 'customer.region.code',
      sql: "json_extract_string(customer_data, '$.region.code')",
      type: 'string',
    },
  ],
};

export const CREATE_TEST_TABLE = `
CREATE TABLE orders (
    order_id INTEGER,
    customer_data string,
    order_amount INTEGER
);
`;

export const INPUT_DATA_QUERY = `
INSERT INTO orders VALUES
(1, '{"region": {"name": "North", "code": "N1"}, "preferences": {"category": "electronics"}}', 100),
(2, '{"region": {"name": "North", "code": "N1"}, "preferences": {"category": "clothing"}}', 200),
(3, '{"region": {"name": "South", "code": "S1"}, "preferences": {"category": "electronics"}}', 300),
(4, '{"region": {"name": "South", "code": "S1"}, "preferences": {"category": "clothing"}}', 400)
`;

describe('composite-dimensions-and-measures', () => {
  beforeAll(async () => {
    // Create orders table
    await duckdbExec(CREATE_TEST_TABLE);

    // Insert data into orders table
    await duckdbExec(INPUT_DATA_QUERY);
  });

  describe('useDotNotation: false (default)', () => {
    it('should handle composite dimensions and measures with multiple dots in their names', async () => {
      // First, let's check the raw data
      const rawData = await duckdbExec('SELECT * FROM orders');
      console.log('Raw data:', JSON.stringify(rawData, null, 2));

      const query: Query = {
        measures: ['orders.total.order.amount'],
        dimensions: [
          'orders.customer.region.name',
          'orders.customer.preferences.category',
        ],
        filters: [
          {
            member: 'orders.customer.region.code',
            operator: 'equals',
            values: ['N1'],
          },
        ],
        order: {
          'orders.total.order.amount': 'desc',
        },
        limit: 2,
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA], options: { useDotNotation: false } });
      console.info(`SQL for Composite Dimensions and Measures: `, sql);

      const output = await duckdbExec(sql);

      expect(output).toEqual([
        {
          orders__customer__preferences__category: 'clothing',
          orders__customer__region__name: 'North',
          orders__total__order__amount: BigInt(200),
        },
        {
          orders__customer__preferences__category: 'electronics',
          orders__customer__region__name: 'North',
          orders__total__order__amount: BigInt(100),
        },
      ]);
    });
  });

  describe('useDotNotation: true', () => {
    it('should handle composite dimensions and measures with multiple dots in their names', async () => {
      const query: Query = {
        measures: ['orders.total.order.amount'],
        dimensions: [
          'orders.customer.region.name',
          'orders.customer.preferences.category',
        ],
        filters: [
          {
            member: 'orders.customer.region.code',
            operator: 'equals',
            values: ['N1'],
          },
        ],
        order: {
          'orders.total.order.amount': 'desc',
        },
        limit: 2,
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA], options: { useDotNotation: true } });
      console.info(`SQL for Composite Dimensions and Measures (dot notation): `, sql);

      const output = await duckdbExec(sql);

      expect(output).toEqual([
        {
          'orders.customer.preferences.category': 'clothing',
          'orders.customer.region.name': 'North',
          'orders.total.order.amount': BigInt(200),
        },
        {
          'orders.customer.preferences.category': 'electronics',
          'orders.customer.region.name': 'North',
          'orders.total.order.amount': BigInt(100),
        },
      ]);
    });
  });
});
