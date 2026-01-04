import { Query } from '@devrev/meerkat-core';
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

const CREATE_TEST_TABLE = `CREATE TABLE orders (
    order_id INTEGER,
    customer_id VARCHAR,
    product_id VARCHAR,
    order_date DATE,
    order_amount FLOAT
);`;

const INPUT_DATA_QUERY = `INSERT INTO orders VALUES
(1, '1', '1', '2022-01-01', 50),
(2, '1', '2', '2022-01-02', 80),
(3, '2', '3', '2022-02-01', 25),
(4, '2', '1', '2022-03-01', 75),
(5, '3', '1', '2022-03-02', 100),
(6, '4', '2', '2022-04-01', 45),
(7, '4', '3', '2022-05-01', 90),
(8, '5', '1', '2022-05-02', 65),
(9, '5', '2', '2022-05-05', 85),
(10, '6', '3', '2022-06-01', 120),
(11, '6aa6', '3', '2024-06-01', 0);
`;

export const TABLE_SCHEMA = {
  name: 'orders',
  sql: 'select * from orders',
  measures: [
    {
      name: 'order_amount',
      sql: 'order_amount',
      type: 'number',
    },
    {
      name: 'total_order_amount',
      sql: 'SUM(order_amount)',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'order_date',
      sql: 'order_date',
      type: 'time',
    },
    {
      name: 'order_id',
      sql: 'order_id',
      type: 'number',
    },
    {
      name: 'customer_id',
      sql: 'customer_id',
      type: 'string',
    },
    {
      name: 'product_id',
      sql: 'product_id',
      type: 'string',
    },
    {
      name: 'order_month',
      sql: `DATE_TRUNC('month', order_date)`,
      type: 'string',
    },
  ],
};

describe('cube-to-sql', () => {
  beforeAll(async () => {
    //Create test table
    await duckdbExec(CREATE_TEST_TABLE);
    //Insert test data
    await duckdbExec(INPUT_DATA_QUERY);
    //Get SQL from cube query
  });

  describe('useDotNotation: false (default)', () => {
    it('Should apply having/where correctly key in measure and filter', async () => {
      const query: Query = {
        measures: ['orders.total_order_amount'],
        filters: [
          {
            member: 'orders.total_order_amount',
            operator: 'equals',
            values: ['100'],
          },
          {
            member: 'orders.customer_id',
            operator: 'equals',
            values: ['2'],
          },
        ],
        dimensions: ['orders.customer_id'],
        order: {
          'orders.total_order_amount': 'desc',
        },
        limit: 2,
      };
      const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA], options: { useDotNotation: false } });
      console.info(`SQL for Simple Cube Query: `, sql);
      expect(sql).toBe(
        `SELECT SUM(order_amount) AS orders__total_order_amount ,   orders__customer_id FROM (SELECT customer_id AS orders__customer_id, * FROM (select * from orders) AS orders) AS orders WHERE (orders__customer_id = '2') GROUP BY orders__customer_id HAVING (orders__total_order_amount = 100) ORDER BY orders__total_order_amount DESC LIMIT 2`
      );
      const output = await duckdbExec(sql);
      expect(output).toEqual([
        {
          orders__customer_id: '2',
          orders__total_order_amount: 100,
        },
      ]);
    });

    it('Should apply having/where correctly key in filter and not in measure', async () => {
      const query: Query = {
        measures: ['orders.total_order_amount'],
        filters: [
          {
            member: 'orders.order_amount',
            operator: 'equals',
            values: ['100'],
          },
        ],
        dimensions: ['orders.customer_id'],
        order: {
          'orders.total_order_amount': 'desc',
        },
        limit: 2,
      };
      const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA], options: { useDotNotation: false } });
      console.info(`SQL for Simple Cube Query: `, sql);
      expect(sql).toBe(
        `SELECT SUM(order_amount) AS orders__total_order_amount ,   orders__customer_id FROM (SELECT orders.order_amount AS orders__order_amount, customer_id AS orders__customer_id, * FROM (select * from orders) AS orders) AS orders WHERE (orders__order_amount = 100) GROUP BY orders__customer_id ORDER BY orders__total_order_amount DESC LIMIT 2`
      );
      const output = await duckdbExec(sql);
      expect(output).toEqual([
        {
          orders__customer_id: '3',
          orders__total_order_amount: 100,
        },
      ]);
    });
  });

  describe('useDotNotation: true', () => {
    it('Should apply having/where correctly key in measure and filter', async () => {
      const query: Query = {
        measures: ['orders.total_order_amount'],
        filters: [
          {
            member: 'orders.total_order_amount',
            operator: 'equals',
            values: ['100'],
          },
          {
            member: 'orders.customer_id',
            operator: 'equals',
            values: ['2'],
          },
        ],
        dimensions: ['orders.customer_id'],
        order: {
          'orders.total_order_amount': 'desc',
        },
        limit: 2,
      };
      const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA], options: { useDotNotation: true } });
      console.info(`SQL for Simple Cube Query (dot notation): `, sql);
      expect(sql).toBe(
        `SELECT SUM(order_amount) AS "orders.total_order_amount" ,   "orders.customer_id" FROM (SELECT customer_id AS "orders.customer_id", * FROM (select * from orders) AS orders) AS orders WHERE ("orders.customer_id" = '2') GROUP BY "orders.customer_id" HAVING ("orders.total_order_amount" = 100) ORDER BY "orders.total_order_amount" DESC LIMIT 2`
      );
      const output = await duckdbExec(sql);
      expect(output).toEqual([
        {
          'orders.customer_id': '2',
          'orders.total_order_amount': 100,
        },
      ]);
    });

    it('Should apply having/where correctly key in filter and not in measure', async () => {
      const query: Query = {
        measures: ['orders.total_order_amount'],
        filters: [
          {
            member: 'orders.order_amount',
            operator: 'equals',
            values: ['100'],
          },
        ],
        dimensions: ['orders.customer_id'],
        order: {
          'orders.total_order_amount': 'desc',
        },
        limit: 2,
      };
      const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA], options: { useDotNotation: true } });
      console.info(`SQL for Simple Cube Query (dot notation): `, sql);
      expect(sql).toBe(
        `SELECT SUM(order_amount) AS "orders.total_order_amount" ,   "orders.customer_id" FROM (SELECT orders.order_amount AS "orders.order_amount", customer_id AS "orders.customer_id", * FROM (select * from orders) AS orders) AS orders WHERE ("orders.order_amount" = 100) GROUP BY "orders.customer_id" ORDER BY "orders.total_order_amount" DESC LIMIT 2`
      );
      const output = await duckdbExec(sql);
      expect(output).toEqual([
        {
          'orders.customer_id': '3',
          'orders.total_order_amount': 100,
        },
      ]);
    });
  });
});
