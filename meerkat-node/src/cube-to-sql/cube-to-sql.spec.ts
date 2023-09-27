import { duckdbExec } from '../duckdb-exec';
import { cubeQueryToSQL } from './cube-to-sql';
import {
  CREATE_TEST_TABLE,
  INPUT_DATA_QUERY,
  TABLE_SCHEMA,
  TEST_DATA,
} from './test-data';
describe('cube-to-sql', () => {
  beforeAll(async () => {
    //Create test table
    await duckdbExec(CREATE_TEST_TABLE);
    //Insert test data
    await duckdbExec(INPUT_DATA_QUERY);
    //Get SQL from cube query
  });

  for (const data of TEST_DATA) {
    it(`Testing ${data.testName}`, async () => {
      const sql = await cubeQueryToSQL(data.cubeInput, TABLE_SCHEMA);
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
        };
      });
      const expectedOutput = data.expectedOutput.map((row) => {
        if (!row.order_date) {
          return row;
        }
        return {
          ...row,
          order_date: new Date(row.order_date).toISOString(),
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
});
