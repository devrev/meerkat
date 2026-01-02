import {
  CREATE_TEST_TABLE,
  INPUT_DATA_QUERY,
  TABLE_SCHEMA,
} from '../__fixtures__/test-data';
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

describe('dot-notation-alias', () => {
  beforeAll(async () => {
    // Create orders table
    await duckdbExec(CREATE_TEST_TABLE);

    // Insert data into orders table
    await duckdbExec(INPUT_DATA_QUERY);
  });

  describe('cubeQueryToSQL with useDotNotation: true', () => {
    it('should generate SQL with dot notation aliases for dimensions', async () => {
      const query = {
        measures: [],
        dimensions: ['orders.customer_id'],
        filters: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { useDotNotation: true },
      });

      // The SQL should contain quoted dot notation alias
      expect(sql).toContain('"orders.customer_id"');
      console.info('SQL with dot notation:', sql);

      // Execute the query to verify it works
      const output = await duckdbExec(sql);
      expect(output).toBeDefined();
      expect(Array.isArray(output)).toBe(true);

      // Verify the output has the dot notation column name
      if (output.length > 0) {
        expect(Object.keys(output[0])).toContain('orders.customer_id');
      }
    });

    it('should generate SQL with dot notation aliases for measures', async () => {
      const query = {
        measures: ['orders.total_order_amount'],
        dimensions: ['orders.customer_id'],
        filters: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { useDotNotation: true },
      });

      // The SQL should contain quoted dot notation aliases
      expect(sql).toContain('"orders.customer_id"');
      expect(sql).toContain('"orders.total_order_amount"');
      console.info('SQL with dot notation (measures):', sql);

      // Execute the query to verify it works
      const output = await duckdbExec(sql);
      expect(output).toBeDefined();
      expect(Array.isArray(output)).toBe(true);

      // Verify the output has the dot notation column names
      if (output.length > 0) {
        expect(Object.keys(output[0])).toContain('orders.customer_id');
        expect(Object.keys(output[0])).toContain('orders.total_order_amount');
      }
    });

    it('should handle filters with dot notation', async () => {
      const query = {
        measures: ['orders.total_order_amount'],
        dimensions: ['orders.customer_id'],
        filters: [
          {
            member: 'orders.customer_id',
            operator: 'equals',
            values: ['1'],
          },
        ],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { useDotNotation: true },
      });

      // The SQL should contain quoted dot notation aliases
      expect(sql).toContain('"orders.customer_id"');
      console.info('SQL with dot notation (filters):', sql);

      // Execute the query to verify it works
      const output = await duckdbExec(sql);
      expect(output).toBeDefined();
      expect(Array.isArray(output)).toBe(true);

      // Verify the output filters correctly
      expect(output.length).toBeGreaterThan(0);
      output.forEach((row: Record<string, unknown>) => {
        expect(row['orders.customer_id']).toBe('1');
      });
    });

    it('should handle order by with dot notation', async () => {
      const query = {
        measures: ['orders.total_order_amount'],
        dimensions: ['orders.customer_id'],
        filters: [],
        order: {
          'orders.total_order_amount': 'desc',
        },
        limit: 3,
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { useDotNotation: true },
      });

      // The SQL should contain quoted dot notation aliases
      expect(sql).toContain('"orders.customer_id"');
      expect(sql).toContain('"orders.total_order_amount"');
      console.info('SQL with dot notation (order by):', sql);

      // Execute the query to verify it works
      const output = await duckdbExec(sql);
      expect(output).toBeDefined();
      expect(Array.isArray(output)).toBe(true);
      expect(output.length).toBeLessThanOrEqual(3);

      // Verify ordering (descending by total_order_amount)
      if (output.length > 1) {
        const amounts = output.map(
          (row: Record<string, unknown>) => row['orders.total_order_amount']
        );
        for (let i = 0; i < amounts.length - 1; i++) {
          expect(amounts[i]).toBeGreaterThanOrEqual(amounts[i + 1] as number);
        }
      }
    });

    it('should handle group by with dot notation', async () => {
      const query = {
        measures: ['orders.total_order_amount'],
        dimensions: ['orders.customer_id'],
        filters: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { useDotNotation: true },
      });

      // The SQL should have a GROUP BY clause
      expect(sql.toUpperCase()).toContain('GROUP BY');
      console.info('SQL with dot notation (group by):', sql);

      // Execute the query to verify it works
      const output = await duckdbExec(sql);
      expect(output).toBeDefined();
      expect(Array.isArray(output)).toBe(true);
    });
  });

  describe('cubeQueryToSQL with useDotNotation: false (default)', () => {
    it('should generate SQL with underscore notation aliases', async () => {
      const query = {
        measures: [],
        dimensions: ['orders.customer_id'],
        filters: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { useDotNotation: false },
      });

      // The SQL should contain underscore notation alias (unquoted)
      expect(sql).toContain('orders__customer_id');
      // Should NOT contain quoted dot notation
      expect(sql).not.toContain('"orders.customer_id"');
      console.info('SQL with underscore notation:', sql);

      // Execute the query to verify it works
      const output = await duckdbExec(sql);
      expect(output).toBeDefined();
      expect(Array.isArray(output)).toBe(true);

      // Verify the output has the underscore notation column name
      if (output.length > 0) {
        expect(Object.keys(output[0])).toContain('orders__customer_id');
      }
    });

    it('should work with underscore notation config', async () => {
      const query = {
        measures: [],
        dimensions: ['orders.customer_id'],
        filters: [],
      };

      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { useDotNotation: false },
      });

      // The SQL should contain underscore notation alias (unquoted)
      expect(sql).toContain('orders__customer_id');
      console.info('SQL with underscore config:', sql);

      // Execute the query to verify it works
      const output = await duckdbExec(sql);
      expect(output).toBeDefined();
      expect(Array.isArray(output)).toBe(true);
    });
  });

  describe('comparison between dot notation and underscore notation', () => {
    it('should produce equivalent data with different column naming', async () => {
      const query = {
        measures: ['orders.total_order_amount'],
        dimensions: ['orders.customer_id'],
        filters: [
          {
            member: 'orders.customer_id',
            operator: 'set',
          },
        ],
        order: {
          'orders.customer_id': 'asc',
        },
      };

      const sqlDot = await cubeQueryToSQL({
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { useDotNotation: true },
      });

      const sqlUnderscore = await cubeQueryToSQL({
        query,
        tableSchemas: [TABLE_SCHEMA],
        options: { useDotNotation: false },
      });

      console.info('Dot notation SQL:', sqlDot);
      console.info('Underscore notation SQL:', sqlUnderscore);

      // Execute both queries
      const outputDot = await duckdbExec(sqlDot);
      const outputUnderscore = await duckdbExec(sqlUnderscore);

      // Both should have the same number of rows
      expect(outputDot.length).toBe(outputUnderscore.length);

      // Data should be equivalent (same values, different column names)
      for (let i = 0; i < outputDot.length; i++) {
        const dotRow = outputDot[i] as Record<string, unknown>;
        const underscoreRow = outputUnderscore[i] as Record<string, unknown>;

        expect(dotRow['orders.customer_id']).toBe(
          underscoreRow['orders__customer_id']
        );
        expect(dotRow['orders.total_order_amount']).toBe(
          underscoreRow['orders__total_order_amount']
        );
      }
    });
  });
});

