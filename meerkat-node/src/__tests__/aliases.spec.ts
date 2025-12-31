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
      alias: 'Order Armount',
    },
    {
      name: 'total_order_amount',
      sql: 'SUM(order_amount)',
      type: 'number',
      alias: 'Total Order Amount',
    },
  ],
  dimensions: [
    {
      name: 'order_date',
      sql: 'order_date',
      type: 'time',
      alias: 'Order Date',
    },
    {
      name: 'order_id',
      sql: 'order_id',
      type: 'number',
      alias: 'Order ID',
    },
    {
      name: 'customer_id',
      sql: 'customer_id',
      type: 'string',
      alias: 'Customer ID',
    },
    {
      name: 'product_id',
      sql: 'product_id',
      type: 'string',
      alias: 'Product ID',
    },
    {
      name: 'order_month',
      sql: `DATE_TRUNC('month', order_date)`,
      type: 'string',
      alias: 'Order Month',
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
    it('Apply aliases for measures and dimensions', async () => {
      const query: Query = {
        measures: ['orders.total_order_amount'],
        dimensions: ['orders.customer_id'],
      };
      const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA], aliasConfig: { useDotNotation: false } });
      console.info(`SQL for Simple Cube Query: `, sql);
      expect(sql).toBe(
        `SELECT SUM(order_amount) AS "Total Order Amount" ,   "Customer ID" FROM (SELECT customer_id AS "Customer ID", * FROM (select * from orders) AS orders) AS orders GROUP BY "Customer ID"`
      );
      const output = await duckdbExec(sql);
      expect(output.length).toEqual(7);
    });

    it('Should apply aliases in filters', async () => {
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
      };
      const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA], aliasConfig: { useDotNotation: false } });
      console.info(`SQL for Simple Cube Query: `, sql);
      expect(sql).toBe(
        `SELECT SUM(order_amount) AS "Total Order Amount" ,   "Customer ID" FROM (SELECT customer_id AS "Customer ID", * FROM (select * from orders) AS orders) AS orders WHERE ("Customer ID" = '2') GROUP BY "Customer ID" HAVING ("Total Order Amount" = 100)`
      );
      const output = await duckdbExec(sql);
      expect(output).toEqual([
        {
          'Customer ID': '2',
          'Total Order Amount': 100,
        },
      ]);
    });

    it('Should apply aliases in order by', async () => {
      const query: Query = {
        measures: ['orders.total_order_amount'],
        filters: [
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
      };
      const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA], aliasConfig: { useDotNotation: false } });
      console.info(`SQL for Simple Cube Query: `, sql);
      expect(sql).toBe(
        `SELECT SUM(order_amount) AS "Total Order Amount" ,   "Customer ID" FROM (SELECT customer_id AS "Customer ID", * FROM (select * from orders) AS orders) AS orders WHERE ("Customer ID" = '2') GROUP BY "Customer ID" ORDER BY "Total Order Amount" DESC`
      );
      const output = await duckdbExec(sql);
      expect(output).toEqual([
        {
          'Customer ID': '2',
          'Total Order Amount': 100,
        },
      ]);
    });

    it('Should handle alias that conflicts with another column', async () => {
      const tableSchemaWithConflict = {
        ...TABLE_SCHEMA,
        dimensions: [
          {
            name: 'customer_id',
            sql: 'customer_id',
            type: 'string',
            alias: 'order_id', // This conflicts with the orderr_id dimension
          },
        ],
      };

      const query: Query = {
        measures: ['orders.total_order_amount'],
        filters: [
          {
            member: 'orders.customer_id',
            operator: 'equals',
            values: ['2'],
          },
        ],
        dimensions: ['orders.customer_id'],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [tableSchemaWithConflict],
        aliasConfig: { useDotNotation: false },
      });
      console.info(`SQL for Simple Cube Query: `, sql);
      expect(sql).toBe(
        `SELECT SUM(order_amount) AS "Total Order Amount" ,   "order_id" FROM (SELECT customer_id AS "order_id", * FROM (select * from orders) AS orders) AS orders WHERE (order_id = '2') GROUP BY order_id`
      );
      const output = await duckdbExec(sql);
      expect(output).toEqual([
        {
          order_id: '2',
          'Total Order Amount': 100,
        },
      ]);
    });
  });

  describe('useDotNotation: true', () => {
    it('Apply aliases for measures and dimensions', async () => {
      const query: Query = {
        measures: ['orders.total_order_amount'],
        dimensions: ['orders.customer_id'],
      };
      const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA], aliasConfig: { useDotNotation: true } });
      console.info(`SQL for Simple Cube Query (dot notation): `, sql);
      expect(sql).toBe(
        `SELECT SUM(order_amount) AS "Total Order Amount" ,   "Customer ID" FROM (SELECT customer_id AS "Customer ID", * FROM (select * from orders) AS orders) AS orders GROUP BY "Customer ID"`
      );
      const output = await duckdbExec(sql);
      expect(output.length).toEqual(7);
    });

    it('Should apply aliases in filters', async () => {
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
      };
      const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA], aliasConfig: { useDotNotation: true } });
      console.info(`SQL for Simple Cube Query (dot notation): `, sql);
      expect(sql).toBe(
        `SELECT SUM(order_amount) AS "Total Order Amount" ,   "Customer ID" FROM (SELECT customer_id AS "Customer ID", * FROM (select * from orders) AS orders) AS orders WHERE ("Customer ID" = '2') GROUP BY "Customer ID" HAVING ("Total Order Amount" = 100)`
      );
      const output = await duckdbExec(sql);
      expect(output).toEqual([
        {
          'Customer ID': '2',
          'Total Order Amount': 100,
        },
      ]);
    });

    it('Should apply aliases in order by', async () => {
      const query: Query = {
        measures: ['orders.total_order_amount'],
        filters: [
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
      };
      const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA], aliasConfig: { useDotNotation: true } });
      console.info(`SQL for Simple Cube Query (dot notation): `, sql);
      expect(sql).toBe(
        `SELECT SUM(order_amount) AS "Total Order Amount" ,   "Customer ID" FROM (SELECT customer_id AS "Customer ID", * FROM (select * from orders) AS orders) AS orders WHERE ("Customer ID" = '2') GROUP BY "Customer ID" ORDER BY "Total Order Amount" DESC`
      );
      const output = await duckdbExec(sql);
      expect(output).toEqual([
        {
          'Customer ID': '2',
          'Total Order Amount': 100,
        },
      ]);
    });

    it('Should handle alias that conflicts with another column', async () => {
      const tableSchemaWithConflict = {
        ...TABLE_SCHEMA,
        dimensions: [
          {
            name: 'customer_id',
            sql: 'customer_id',
            type: 'string',
            alias: 'order_id', // This conflicts with the orderr_id dimension
          },
        ],
      };

      const query: Query = {
        measures: ['orders.total_order_amount'],
        filters: [
          {
            member: 'orders.customer_id',
            operator: 'equals',
            values: ['2'],
          },
        ],
        dimensions: ['orders.customer_id'],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [tableSchemaWithConflict],
        aliasConfig: { useDotNotation: true },
      });
      console.info(`SQL for Simple Cube Query (dot notation): `, sql);
      expect(sql).toBe(
        `SELECT SUM(order_amount) AS "Total Order Amount" ,   "order_id" FROM (SELECT customer_id AS "order_id", * FROM (select * from orders) AS orders) AS orders WHERE (order_id = '2') GROUP BY order_id`
      );
      const output = await duckdbExec(sql);
      expect(output).toEqual([
        {
          order_id: '2',
          'Total Order Amount': 100,
        },
      ]);
    });
  });
});
