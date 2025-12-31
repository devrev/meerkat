import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';
const SCHEMA = {
  name: 'orders',
  sql: "select * from orders WHERE ${FILTER_PARAMS.orders.status.filter('status')}",
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
describe('filter-param-tests', () => {
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
             (7, DATE '2022-02-15', 'pending', 120.00),
             (8, DATE '2022-04-01', 'initiated', null);`);
  });

  describe('useDotNotation: false (default)', () => {
    it('Should apply filter params to base SQL', async () => {
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
      const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA], aliasConfig: { useDotNotation: false } });
      console.info('SQL: ', sql);
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(2);
      expect(output[0].id).toBe(6);
    });

    it('Should apply multiple filter params to base SQL', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            or: [
              {
                member: 'orders.status',
                operator: 'equals',
                values: ['pending'],
              },
              {
                member: 'orders.status',
                operator: 'equals',
                values: ['cancelled'],
              },
              {
                member: 'orders.amount',
                operator: 'gt',
                values: ['40'],
              },
            ],
          },
        ],
        order: { id: 'asc' },
        dimensions: [],
      };
      const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA], aliasConfig: { useDotNotation: false } });
      console.info('SQL: ', sql);
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(3);
      expect(output[0].id).toBe(4);
    });

    it('Should apply true filter if no filters are present', async () => {
      const query = {
        measures: ['*'],
        filters: [],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA], aliasConfig: { useDotNotation: false } });
      console.info('SQL: ', sql);
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(8);
    });

    it('Should apply true filter if filters are present but are not matching', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            and: [
              {
                member: 'orders.amount',
                operator: 'gt',
                values: ['40'],
              },
              {
                or: [
                  {
                    member: 'orders.amount',
                    operator: 'lt',
                    values: ['200'],
                  },
                ],
              },
            ],
          },
        ],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA], aliasConfig: { useDotNotation: false } });
      console.info('SQL: ', sql);
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(5);
    });

    it('Should apply notSet and set filters', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            and: [
              {
                member: 'orders.amount',
                operator: 'notSet',
              },
              {
                member: 'orders.status',
                operator: 'set',
              },
            ],
          },
        ],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA], aliasConfig: { useDotNotation: false } });
      console.info('SQL: ', sql);
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(1);
    });
  });

  describe('useDotNotation: true', () => {
    it('Should apply filter params to base SQL', async () => {
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
      const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA], aliasConfig: { useDotNotation: true } });
      console.info('SQL (dot notation): ', sql);
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(2);
      expect(output[0].id).toBe(6);
    });

    it('Should apply multiple filter params to base SQL', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            or: [
              {
                member: 'orders.status',
                operator: 'equals',
                values: ['pending'],
              },
              {
                member: 'orders.status',
                operator: 'equals',
                values: ['cancelled'],
              },
              {
                member: 'orders.amount',
                operator: 'gt',
                values: ['40'],
              },
            ],
          },
        ],
        order: { id: 'asc' },
        dimensions: [],
      };
      const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA], aliasConfig: { useDotNotation: true } });
      console.info('SQL (dot notation): ', sql);
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(3);
      expect(output[0].id).toBe(4);
    });

    it('Should apply true filter if no filters are present', async () => {
      const query = {
        measures: ['*'],
        filters: [],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA], aliasConfig: { useDotNotation: true } });
      console.info('SQL (dot notation): ', sql);
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(8);
    });

    it('Should apply notSet and set filters', async () => {
      const query = {
        measures: ['*'],
        filters: [
          {
            and: [
              {
                member: 'orders.amount',
                operator: 'notSet',
              },
              {
                member: 'orders.status',
                operator: 'set',
              },
            ],
          },
        ],
        dimensions: [],
      };

      const sql = await cubeQueryToSQL({ query, tableSchemas: [SCHEMA], aliasConfig: { useDotNotation: true } });
      console.info('SQL (dot notation): ', sql);
      const output: any = await duckdbExec(sql);
      expect(output).toHaveLength(1);
    });
  });
});
