import {
  CREATE_TEST_TABLE,
  getTestData,
  INPUT_DATA_QUERY,
  TABLE_SCHEMA,
} from '../__fixtures__/test-data';
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

describe('cube-to-sql', () => {
  beforeAll(async () => {
    // Create orders table
    await duckdbExec(CREATE_TEST_TABLE);

    // Insert data into orders table
    await duckdbExec(INPUT_DATA_QUERY);
  });

  getTestData()
    .flat()
    .forEach((data) => {
      it(`Testing ${data.testName}`, async () => {
        const sql = await cubeQueryToSQL({
          query: data.cubeInput,
          tableSchemas: [TABLE_SCHEMA],
        });
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
    });

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
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [TABLE_SCHEMA],
    });
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
    const sql = await cubeQueryToSQL({
      query: query,
      tableSchemas: [TABLE_SCHEMA],
    });
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
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [TABLE_SCHEMA],
    });
    console.info(`SQL for Simple Cube Query: `, sql);
    expect(sql).toEqual(
      'SELECT orders.* FROM (SELECT * FROM (select * from orders) AS orders) AS orders'
    );
  });

  it('Should handle empty order', async () => {
    const query = {
      order: {},
      measures: ['*'],
      filters: [
        {
          or: [],
        },
      ],
      dimensions: [],
    };
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [TABLE_SCHEMA],
    });
    console.info(`SQL for Simple Cube Query: `, sql);
    expect(sql).toEqual(
      'SELECT orders.* FROM (SELECT * FROM (select * from orders) AS orders) AS orders'
    );
  });

  it('Should handle order by schema field not present in query dimensions or measures', async () => {
    // order_date exists in schema as a dimension, but is NOT in the query's dimensions or measures
    // Test that the order field is properly aliased in the inner SELECT
    const query = {
      measures: ['orders.total_order_amount'],
      filters: [],
      dimensions: ['orders.customer_id'],
      order: {
        'orders.order_date': 'desc',
      },
      limit: 5,
    };
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [TABLE_SCHEMA],
    });
    console.info(`SQL for order by field not in query: `, sql);
    // The order field should be projected in the inner query so ORDER BY works
    expect(sql).toContain('ORDER BY orders__order_date DESC');
    // The order field should be aliased in the inner SELECT
    expect(sql).toContain('order_date AS orders__order_date');
  });

  it('Should order by field not in dimensions and verify sorting', async () => {
    // Dimensions: customer_id - projected in inner query
    // Order by: order_date - NOT in dimensions, but should be projected for ORDER BY
    const query = {
      measures: [],
      filters: [],
      dimensions: ['orders.customer_id'],
      order: {
        'orders.order_date': 'desc',
      },
      limit: 5,
    };
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [TABLE_SCHEMA],
    });
    console.info(`SQL for dimensions order by different field: `, sql);
    // The order field should be projected in the inner query
    expect(sql).toContain('ORDER BY orders__order_date DESC');
    expect(sql).toContain('order_date AS orders__order_date');
    // The dimension should also be projected
    expect(sql).toContain('customer_id AS orders__customer_id');
    // Execute the query and verify ordering
    const output = await duckdbExec(sql);
    const parsedOutput = JSON.parse(JSON.stringify(output));
    console.info('Output:', parsedOutput);
    // Should return results
    expect(parsedOutput.length).toBeGreaterThan(0);
    expect(parsedOutput.length).toBeLessThanOrEqual(5);
    // Verify the results match source table data sorted by order_date descending
    // Source data (INPUT_DATA_QUERY) sorted by order_date DESC, limit 5:
    // (14, '8', '1', '2024-09-01', 50, NULL)
    // (13, '7', '6', '2024-08-01', 100, ['swiggy''s'])
    // (12, NULL, '3', '2024-07-01', 100, ['flipkart'])
    // (11, '6aa6', '3', '2024-06-01', 0, ['amazon'])
    // (10, '6', '3', '2022-06-01', 120, ['myntra', 'amazon'])
    const expectedCustomerIds = ['8', '7', null, '6aa6', '6'];
    // Verify customer_ids match expected order from source table (sorted by order_date desc)
    // This proves the ORDER BY is working correctly even though order_date is not in dimensions
    for (let i = 0; i < parsedOutput.length; i++) {
      expect(parsedOutput[i].orders__customer_id).toBe(expectedCustomerIds[i]);
    }
  });
});
