import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';
import {
  CREATE_TEST_TABLE,
  INPUT_DATA_QUERY,
  TABLE_SCHEMA,
  TEST_DATA,
} from './test-data';

describe('cube-to-sql', () => {
  beforeAll(async () => {
    // Create orders table
    await duckdbExec(CREATE_TEST_TABLE);

    // Insert data into orders table
    await duckdbExec(INPUT_DATA_QUERY);
  });

  describe('with isDotDelimiterEnabled: false (underscore format)', () => {
    TEST_DATA.flat().forEach((data) => {
      it(`Testing ${data.testName}`, async () => {
        const sql = await cubeQueryToSQL({
        options: { isDotDelimiterEnabled: false },
          query: data.cubeInput,
          tableSchemas: [TABLE_SCHEMA],
          options: { isDotDelimiterEnabled: false },
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
        options: { isDotDelimiterEnabled: false },
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { isDotDelimiterEnabled: false },
      });
      console.info(`SQL for ORDER BY (underscore format): `, sql);

      // Verify SQL uses underscore format
      expect(sql).toContain('orders__total_order_amount');
      expect(sql).toContain('orders__customer_id');
      expect(sql).not.toContain('"orders.total_order_amount"');
      expect(sql).not.toContain('"orders.customer_id"');

      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));
      console.info('parsedOutput (underscore format)', parsedOutput);

      // Verify output has underscore-delimited column names
      expect(parsedOutput[0].orders__total_order_amount).toBeGreaterThan(
        parsedOutput[1].orders__total_order_amount
      );
      expect('orders__customer_id' in parsedOutput[0]).toBe(true);
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
        options: { isDotDelimiterEnabled: false },
        query: query,
        tableSchemas: [TABLE_SCHEMA],
        options: { isDotDelimiterEnabled: false },
      });
      console.info(`SQL for empty and (underscore format): `, sql);
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
        options: { isDotDelimiterEnabled: false },
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { isDotDelimiterEnabled: false },
      });
      console.info(`SQL for empty or (underscore format): `, sql);
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
        options: { isDotDelimiterEnabled: false },
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { isDotDelimiterEnabled: false },
      });
      console.info(`SQL for empty order (underscore format): `, sql);
      expect(sql).toEqual(
        'SELECT orders.* FROM (SELECT * FROM (select * from orders) AS orders) AS orders'
      );
    });

    it('Should handle GROUP BY with measures and dimensions', async () => {
      const query = {
        measures: ['orders.total_order_amount'],
        dimensions: ['orders.customer_id'],
      };
      const sql = await cubeQueryToSQL({
        options: { isDotDelimiterEnabled: false },
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { isDotDelimiterEnabled: false },
      });
      console.info(`SQL with GROUP BY (underscore format): `, sql);

      // Verify GROUP BY uses underscore delimiter
      expect(sql).toContain('GROUP BY');
      expect(sql).toContain('orders__customer_id');
      expect(sql).not.toContain('"orders.customer_id"');

      // Verify the query executes successfully
      const output = await duckdbExec(sql);
      expect(output.length).toBeGreaterThan(0);
      expect('orders__customer_id' in output[0]).toBe(true);
    });

    it('Should handle filters', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            member: 'orders.customer_id',
            operator: 'equals',
            values: ['1'],
          },
        ],
        dimensions: [],
      };
      const sql = await cubeQueryToSQL({
        options: { isDotDelimiterEnabled: false },
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { isDotDelimiterEnabled: false },
      });
      console.info(`SQL with filter (underscore format): `, sql);

      // Verify the query uses underscore format for projected columns
      expect(sql).toContain('orders__customer_id');

      // Verify the query executes successfully
      const output = await duckdbExec(sql);
      expect(output.length).toBe(2); // Customer 1 has 2 orders
    });
  });

  describe('with isDotDelimiterEnabled: true (dot format)', () => {
    it('Should use dot delimiter in aliases', async () => {
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
        options: { isDotDelimiterEnabled: false },
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { isDotDelimiterEnabled: true },
      });
      console.info(`SQL with dot delimiter: `, sql);

      // Verify the SQL uses quoted dot delimiters instead of underscores
      expect(sql).toContain('"orders.total_order_amount"');
      expect(sql).toContain('"orders.customer_id"');
      expect(sql).not.toContain('orders__total_order_amount');
      expect(sql).not.toContain('orders__customer_id');

      // Verify the query executes successfully
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));
      console.info('parsedOutput with dot delimiter', parsedOutput);

      // Verify output has dot-delimited column names (use 'in' operator since dots in property names confuse Jest)
      expect('orders.total_order_amount' in parsedOutput[0]).toBe(true);
      expect('orders.customer_id' in parsedOutput[0]).toBe(true);
      expect(parsedOutput[0]['orders.total_order_amount']).toBeGreaterThan(
        parsedOutput[1]['orders.total_order_amount']
      );
    });

    it('Should handle GROUP BY with dot delimiter', async () => {
      const query = {
        measures: ['orders.total_order_amount'],
        dimensions: ['orders.customer_id'],
      };
      const sql = await cubeQueryToSQL({
        options: { isDotDelimiterEnabled: false },
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { isDotDelimiterEnabled: true },
      });
      console.info(`SQL with GROUP BY and dot delimiter: `, sql);

      // Verify GROUP BY uses quoted dot delimiter
      expect(sql).toContain('GROUP BY');
      expect(sql).toContain('"orders.customer_id"');
      expect(sql).not.toContain('orders__customer_id');

      // Verify the query executes successfully
      const output = await duckdbExec(sql);
      expect(output.length).toBeGreaterThan(0);
      expect('orders.customer_id' in output[0]).toBe(true);
    });

    it('Should handle filters with dot delimiter', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            member: 'orders.customer_id',
            operator: 'equals',
            values: ['1'],
          },
        ],
        dimensions: [],
      };
      const sql = await cubeQueryToSQL({
        options: { isDotDelimiterEnabled: false },
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { isDotDelimiterEnabled: true },
      });
      console.info(`SQL with filter and dot delimiter: `, sql);

      // Verify WHERE clause uses quoted dot delimiter
      expect(sql).toContain('"orders.customer_id"');
      expect(sql).not.toContain('orders__customer_id');

      // Verify the query executes successfully
      const output = await duckdbExec(sql);
      expect(output.length).toBe(2); // Customer 1 has 2 orders
    });

    it('Should preserve dot delimiter in ORDER BY', async () => {
      const query = {
        measures: ['orders.total_order_amount'],
        dimensions: ['orders.customer_id'],
        order: {
          'orders.total_order_amount': 'asc',
          'orders.customer_id': 'asc',
        },
      };
      const sql = await cubeQueryToSQL({
        options: { isDotDelimiterEnabled: false },
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { isDotDelimiterEnabled: true },
      });
      console.info(`SQL with ORDER BY and dot delimiter: `, sql);

      // Verify ORDER BY uses quoted dot delimiter
      expect(sql).toContain('ORDER BY');
      expect(sql).toContain('"orders.total_order_amount"');
      expect(sql).toContain('"orders.customer_id"');
      expect(sql).not.toContain('orders__total_order_amount');
      expect(sql).not.toContain('orders__customer_id');

      // Verify the query executes successfully
      const output = await duckdbExec(sql);
      expect(output.length).toBeGreaterThan(0);
    });

    it('Should handle empty filters with dot delimiter', async () => {
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
        options: { isDotDelimiterEnabled: false },
        query: query,
        tableSchemas: [TABLE_SCHEMA],
        options: { isDotDelimiterEnabled: true },
      });
      console.info(`SQL for empty and (dot format): `, sql);
      expect(sql).toEqual(
        'SELECT orders.* FROM (SELECT * FROM (select * from orders) AS orders) AS orders'
      );
    });

    it('Should handle empty order with dot delimiter', async () => {
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
        options: { isDotDelimiterEnabled: false },
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { isDotDelimiterEnabled: true },
      });
      console.info(`SQL for empty order (dot format): `, sql);
      expect(sql).toEqual(
        'SELECT orders.* FROM (SELECT * FROM (select * from orders) AS orders) AS orders'
      );
    });
  });
});
