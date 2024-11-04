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
    it('Should not append group by when no measures selected', async () => {
        const query: Query = {
        measures: [],
        dimensions: ['orders.customer_id'],
        };
        const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA] });
        console.info(`SQL for Simple Cube Query: `, sql);
        expect(sql).toBe(
            "SELECT  orders__customer_id FROM (SELECT *, customer_id AS orders__customer_id FROM (select * from orders) AS orders) AS orders"
        );
        const output = await duckdbExec(sql);
        expect(output).toEqual([
               {
                "orders__customer_id": "1",
              },
               {
                "orders__customer_id": "1",
              },
               {
                "orders__customer_id": "2",
              },
               {
                "orders__customer_id": "2",
              },
               {
                "orders__customer_id": "3",
              },
               {
                "orders__customer_id": "4",
              },
               {
                "orders__customer_id": "4",
              },
            ]
        );
    });
    it('Should append group by when some measures are selected', async () => {
        const query: Query = {
          measures: ['orders.total_order_amount'],
        dimensions: ['orders.customer_id'],
        };
        const sql = await cubeQueryToSQL({ query, tableSchemas: [TABLE_SCHEMA] });
        console.info(`SQL for Simple Cube Query: `, sql);
        expect(sql).toBe("SELECT SUM(order_amount) AS orders__total_order_amount ,   orders__customer_id FROM (SELECT *, customer_id AS orders__customer_id FROM (select * from orders) AS orders) AS orders GROUP BY orders__customer_id");
        const output = await duckdbExec(sql);
        expect(output).toEqual([
            {
                "orders__customer_id": "1",
                "orders__total_order_amount": 130,
            },
            {
                "orders__customer_id": "2",
                "orders__total_order_amount": 100,
            },
            {
                "orders__customer_id": "3",
                "orders__total_order_amount": 100,
            },
            {
                "orders__customer_id": "4",
                "orders__total_order_amount": 135,
            },
        ]);
    });
});
