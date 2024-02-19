import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';
import {
  CREATE_TEST_TABLE,
  CREATE_TEST_TABLE_2,
  INPUT_DATA_QUERY,
  INPUT_DATA_QUERY_2,
  TABLE_SCHEMA,
  TEST_DATA,
} from './test-data';
describe('cube-to-sql', () => {
  beforeAll(async () => {
    // Create orders table
    await duckdbExec(CREATE_TEST_TABLE);

    // Insert data into orders table
    await duckdbExec(INPUT_DATA_QUERY);

    // Create customers table
    await duckdbExec(CREATE_TEST_TABLE_2);

    // Insert data into customers table
    await duckdbExec(INPUT_DATA_QUERY_2);
  });

  for (const data of TEST_DATA) {
    it(`Testing ${data.testName}`, async () => {
      const sql = await cubeQueryToSQL(data.cubeInput, TABLE_SCHEMA);
      expect(sql).toEqual(data.expectedSQL);
      console.info(`SQL for ${data.testName}: `, sql);
      //TODO: Remove order by
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));
      const formattedOutput = parsedOutput.map((row) => {
        if (!row.order_date) {
          return row;
        }
        return {
          ...row,
          order_date: new Date(row.order_date).toISOString(),
          orders__order_date: row.orders__order_date
            ? new Date(row.orders__order_date).toISOString()
            : undefined,
        };
      });
      const expectedOutput = data.expectedOutput.map((row) => {
        if (!row.order_date) {
          return row;
        }
        return {
          ...row,
          order_date: new Date(row.order_date).toISOString(),
          orders__order_date: row.orders__order_date
            ? new Date(row.orders__order_date).toISOString()
            : undefined,
        };
      });

      /**
       * Compare the output with the expected output
       */
      expect(formattedOutput).toStrictEqual(expectedOutput);
      /**
       * Compare expect SQL with the generated SQL
       */
      expect(sql).toBe(data.expectedSQL);
    });
  }

  it('Should order the projected value', async () => {
    const query = {
      measures: ['orders.total_order_amount'],
      filters: [],
      dimensions: ['orders.customer_id'],
      order: {
        'orders.total_order_amount': 'desc',
      },
      limit: 2,
    };
    const sql = await cubeQueryToSQL(query, [TABLE_SCHEMA]);
    console.info(`SQL for Simple Cube Query: `, sql);
    const output = await duckdbExec(sql);
    const parsedOutput = JSON.parse(JSON.stringify(output));
    console.info('parsedOutput', parsedOutput);
    expect(parsedOutput[0].orders__total_order_amount).toBeGreaterThan(
      parsedOutput[1].orders__total_order_amount
    );
  });

  it('Without filter query generator with empty and', async () => {
    const query = {
      measures: ['*'],
      filters: [
        {
          and: [],
        },
      ],
      dimensions: [],
    };
    const sql = await cubeQueryToSQL(query, TABLE_SCHEMA);
    console.info(`SQL for Simple Cube Query: `, sql);
    expect(sql).toEqual(
      'SELECT orders.* FROM (SELECT * FROM (select * from orders) AS orders) AS orders'
    );
  });

  it('Without filter query generator with empty or', async () => {
    const query = {
      measures: ['*'],
      filters: [
        {
          or: [],
        },
      ],
      dimensions: [],
    };
    const sql = await cubeQueryToSQL(query, TABLE_SCHEMA);
    console.info(`SQL for Simple Cube Query: `, sql);
    expect(sql).toEqual(
      'SELECT orders.* FROM (SELECT * FROM (select * from orders) AS orders) AS orders'
    );
  });
});
