import { TEST_DATA_WITH_SAFE_ALIAS } from './test-data-with-safe-alias';

export const CREATE_TEST_TABLE = `
CREATE TABLE orders (
    order_id INTEGER,
    customer_id VARCHAR,
    product_id VARCHAR,
    order_date DATE,
    order_amount FLOAT,
    vendors VARCHAR[]
);
`;

export const INPUT_DATA_QUERY = `
INSERT INTO orders VALUES
(1, '1', '1', '2022-01-01', 50, ['myntra', 'amazon', 'flipkart']),
(2, '1', '2', '2022-01-02', 80, ['myntra']),
(3, '2', '3', '2022-02-01', 25, []),
(4, '2', '1', '2022-03-01', 75, ['flipkart']),
(5, '3', '1', '2022-03-02', 100, ['myntra', 'amazon', 'flipkart']),
(6, '4', '2', '2022-04-01', 45, []),
(7, '4', '3', '2022-05-01', 90, ['myntra', 'flipkart']),
(8, '5', '1', '2022-05-02', 65, ['amazon', 'flipkart']),
(9, '5', '2', '2022-05-05', 85, []),
(10, '6', '3', '2022-06-01', 120, ['myntra', 'amazon']),
(11, '6aa6', '3', '2024-06-01', 0, ['amazon']),
(12, NULL, '3', '2024-07-01', 100, ['flipkart']),
(13, '7', '6', '2024-08-01', 100, ['swiggy''s']),
(14, '8', '1', '2024-09-01', 50, NULL);
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
    {
      name: 'vendors',
      sql: 'vendors',
      type: 'string_array',
    },
  ],
};

/**
 * Get test data with underscore notation aliases (e.g., orders__customer_id)
 */
export const getTestData = () => {
  return TEST_DATA_WITH_SAFE_ALIAS;
};
