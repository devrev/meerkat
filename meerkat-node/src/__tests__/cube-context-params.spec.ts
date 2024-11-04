import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';
const SCHEMA = {
  name: 'orders',
  sql: 'select * from ${CONTEXT_PARAMS.TABLE_NAME}',
  measures: [],
  dimensions: [
    {
      name: 'id',
      sql: 'id',
      type: 'number',
    },
    {
      name: 'date',
      sql: 'date',
      type: 'string',
    },
    {
      name: 'status',
      sql: 'status',
      type: 'string',
    },
    {
      name: 'amount',
      sql: 'amount',
      type: 'number',
    },
  ],
};
describe('context-param-tests', () => {
  beforeAll(async () => {
    await duckdbExec(`CREATE TABLE orders(
        id INTEGER PRIMARY KEY,
        date DATE,
        status VARCHAR,
        amount DECIMAL
    );`);
    await duckdbExec(`INSERT INTO orders(id, date, status, amount)
      VALUES (1, DATE '2022-01-21', 'completed', 99.99),
             (2, DATE '2022-02-10', 'completed', 200.50),
             (3, DATE '2022-01-25', 'completed', 150.00),
             (4, DATE '2022-03-01', 'cancelled', 40.00),
             (5, DATE '2022-01-28', 'completed', 80.75),
             (6, DATE '2022-02-15', 'pending', 120.00),
             (7, DATE '2022-04-01', 'completed', 210.00);`);
  });

  it('Should apply context params to base SQL', async () => {
    const query = {
      measures: ['*'],
      filters: [
        {
          member: 'orders.status',
          operator: 'equals',
          values: ['pending'],
        },
      ],
      dimensions: [],
    };
    const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA], contextParams: {
      TABLE_NAME: 'orders',
    }});
    console.info('SQL: ', sql);
    const output: any = await duckdbExec(sql);
    expect(output).toHaveLength(1);
  });
});
