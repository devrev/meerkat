import { cubeQueryToSQL } from "../cube-to-sql/cube-to-sql";
import { duckdbExec } from "../duckdb-exec";


const CREATE_TEST_TABLE = `CREATE TABLE orders (
    id VARCHAR,
    owned_by_id VARCHAR,
    stage VARCHAR,
    amount FLOAT
);`;

export const INPUT_DATA_QUERY = `INSERT INTO orders VALUES
('1', 'user_1', 'stage_a', 10),
(2, 'user_1', 'stage_a', 20),
(3, 'user_2', 'stage_a', 90),
(4, 'user_3', 'stage_a', 50),
(5, 'user_1', 'stage_a', 30),
(6, 'user_2', 'stage_a', 100),
(7, 'user_3', 'stage_b', 9000),
(8, 'user_1', 'stage_b', 5),
(9, 'user_2', 'stage_b', 8),
(10, 'user_3', 'stage_a', 60),
(11, 'user_1', 'stage_c', 80);
`;

describe('meerkat placeholder', () => {
    beforeAll(async () => {
        //Create test table
        await duckdbExec(CREATE_TEST_TABLE);
        //Insert test data
        await duckdbExec(INPUT_DATA_QUERY);
        //Get SQL from cube query
      });
    it ('should resolve query correctly', async () => {
        const query = {
          "dimensions": [
            "orders.owned_by_id",
            "orders.stage"
          ],
          "measures": [
              "orders.sum_amount",
              "orders.total_sum_amount"
          ],
          "order": {
              "orders__total_sum_amount": "desc"
          },
          "timeDimensions": [],
          "type": "sql",
        }
    
        const tableSchema =  {
          "dimensions": [
              {
                  "name": "id",
                  "sql": "id",
                  "type": "string"
              },
              {
                  "name": "owned_by_id",
                  "sql": "owned_by_id",
                  "type": "string"
              },
              {
                  "name": "stage",
                  "sql": "stage",
                  "type": "string"
              },
          ],
          "measures": [
              {
                  "name": "sum_amount",
                  "sql": "SUM(amount)",
                  "type": "number"
              },
              {
                  "name": "total_sum_amount",
                  "sql": "SUM(SUM(amount)) OVER (PARTITION BY {MEERKAT}.owned_by_id)",
                  "type": "number"
              }
          ],
          "name": "orders",
          "sql": `SELECT * FROM orders`
        }

        const baseQuery = `SELECT *  FROM orders`
        const baseOutput = await duckdbExec(baseQuery);
        console.log({baseOutput})
    
        const sql = await cubeQueryToSQL(query, tableSchema);
        console.info(`SQL for Simple Cube Query: `, sql);
        expect(sql).toEqual(
          `SELECT SUM(amount) AS orders__sum_amount ,  SUM(SUM(amount)) OVER (PARTITION BY orders__owned_by_id) AS orders__total_sum_amount ,   orders__owned_by_id,  orders__stage FROM (SELECT *, owned_by_id AS orders__owned_by_id, stage AS orders__stage FROM (SELECT * FROM orders) AS orders) AS orders GROUP BY orders__owned_by_id, orders__stage ORDER BY orders__total_sum_amount DESC`
        );
        const output = await duckdbExec(sql);
        expect(output).toEqual([
            {
              orders__sum_amount: 110,
              orders__total_sum_amount: 9110,
              orders__owned_by_id: 'user_3',
              orders__stage: 'stage_a'
            },
            {
              orders__sum_amount: 9000,
              orders__total_sum_amount: 9110,
              orders__owned_by_id: 'user_3',
              orders__stage: 'stage_b'
            },
            {
              orders__sum_amount: 190,
              orders__total_sum_amount: 198,
              orders__owned_by_id: 'user_2',
              orders__stage: 'stage_a'
            },
            {
              orders__sum_amount: 8,
              orders__total_sum_amount: 198,
              orders__owned_by_id: 'user_2',
              orders__stage: 'stage_b'
            },
            {
              orders__sum_amount: 60,
              orders__total_sum_amount: 145,
              orders__owned_by_id: 'user_1',
              orders__stage: 'stage_a'
            },
            {
              orders__sum_amount: 5,
              orders__total_sum_amount: 145,
              orders__owned_by_id: 'user_1',
              orders__stage: 'stage_b'
            },
            {
              orders__sum_amount: 80,
              orders__total_sum_amount: 145,
              orders__owned_by_id: 'user_1',
              orders__stage: 'stage_c'
            }
        ])
    })
})