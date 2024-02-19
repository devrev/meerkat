import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

export const CREATE_TEST_TABLE_2 = `
CREATE TABLE customers (
    customer_id VARCHAR,
    customer_name VARCHAR,
    customer_email VARCHAR,
    customer_phone VARCHAR
);
`;

export const INPUT_DATA_QUERY_2 = `
INSERT INTO customers VALUES
('1', 'John Doe', 'johndoe@gmail.com', '1234567890'),
('2', 'Jane Doe', 'janedoe@gmail.com', '9876543210'),
('3', 'John Smith', 'johnsmith@gmail.com', '1234567892'); 
`;

export const TABLE_SCHEMA_2 = {
  name: 'customers',
  sql: 'select * from customers',
  measures: [
    {
      name: 'total_customer_count',
      sql: 'COUNT(customer_id)',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'customer_id',
      sql: 'customer_id',
      type: 'number',
    },
    {
      name: 'customer_name',
      sql: 'customer_name',
      type: 'string',
    },
    {
      name: 'customer_email',
      sql: 'customer_email',
      type: 'string',
    },
    {
      name: 'customer_phone',
      sql: 'customer_phone',
      type: 'string',
    },
  ],
  joins: [
    {
      sql: 'orders.customer_id = customers.customer_id',
    },
  ],
};

export const CREATE_TEST_TABLE = `
CREATE TABLE orders (
    order_id INTEGER,
    customer_id VARCHAR,
    product_id VARCHAR,
    order_date DATE,
    order_amount FLOAT
);
`;

export const INPUT_DATA_QUERY = `
INSERT INTO orders VALUES
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

describe('Joins', () => {
  beforeAll(async () => {
    await duckdbExec(CREATE_TEST_TABLE_2);
    await duckdbExec(CREATE_TEST_TABLE);
    await duckdbExec(INPUT_DATA_QUERY_2);
    await duckdbExec(INPUT_DATA_QUERY);
  });

  it('Test', async () => {
    const query = {
      measures: ['orders.total_order_amount'],
      filters: [
        {
          and: [
            {
              member: 'orders.order_amount',
              operator: 'gt',
              values: ['75'],
            },
            {
              member: 'customers.customer_name',
              operator: 'contains',
              values: ['Doe'],
            },
          ],
        },
      ],
      dimensions: ['orders.customer_id', 'customers.customer_name'],
      order: {
        'orders.total_order_amount': 'desc',
      },
      limit: 2,
    };
    const sql = await cubeQueryToSQL(query, [TABLE_SCHEMA_2, TABLE_SCHEMA]);
    console.info(`SQL for Simple Cube Query: `, sql);
    const output = await duckdbExec(sql);
    const parsedOutput = JSON.parse(JSON.stringify(output));
    console.info('parsedOutput', parsedOutput);
    // expect(parsedOutput[0].orders__total_order_amount).toBeGreaterThan(
    //   parsedOutput[1].orders__total_order_amount
    // );
  });
});
