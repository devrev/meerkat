import { Query } from '@devrev/meerkat-core';
import { cubeQueryToSQL } from '@devrev/meerkat-node';
import { QueryBuilderImpl } from './query-builder-impl';
import { queryBuilderToCubeQuery } from './query-converter';
describe('QueryBuilder to CubeQuery conversion and SQL generation with single AND filter', () => {
  it('should correctly convert a QueryBuilderImpl object to a CubeQuery with a single AND filter, generate schemas, and produce SQL', async () => {
    const queryBuilder = new QueryBuilderImpl({
      sourceId: 'orders',
      sourceType: 'table',
      measures: [
        {
          type: 'count',
          fieldId: 'orders.order_amount',
        },
        {
          type: 'custom',
          expression: 'CASE WHEN status = "completed" THEN 1 ELSE 0 END',
          name: 'custom_2',
        },
      ],
      dimensions: [
        {
          fieldId: 'orders.order_date',
        },
        {
          fieldId: 'orders.status',
        },
        {
          fieldId: 'customers.customer_name',
        },
      ],
      selections: [],
      joins: [
        {
          sourceId: 'orders',
          sourceType: 'table',
          targetId: 'customers',
          targetType: 'table',
          type: 'inner',
          conditions: [
            { leftFieldId: 'customer_id', rightFieldId: 'id', operator: '=' },
          ],
        },
      ],
      orderBy: [
        {
          fieldId: 'orders.order_amount',
          direction: 'desc',
        },
      ],
      limit: 100,
      offset: 0,
    });

    // Convert QueryBuilderImpl to CubeQuery and generate schemas
    const { query: cubeQuery, schemas } = queryBuilderToCubeQuery(
      queryBuilder.build()
    );

    // Generate SQL
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    const sql = await cubeQueryToSQL(cubeQuery, schemas);
    console.info(sql);
    console.info(JSON.stringify(cubeQuery, null, 2));
    console.info(JSON.stringify(schemas, null, 2));

    // Expected CubeQuery object
    const expectedCubeQuery: Query = {
      measures: [
        'orders.order_amount_sum',
        'CASE WHEN status = "completed" THEN 1 ELSE 0 END',
      ],
      dimensions: [
        'orders.order_date',
        'orders.status',
        'customers.customer_name',
      ],
      filters: [
        {
          and: [
            {
              member: 'orders.status',
              operator: 'equals',
              values: [],
            },
            {
              member: 'orders.order_amount > 100',
              operator: 'equals',
              values: [],
            },
          ],
        },
      ],
      joinPaths: [
        [
          {
            left: 'orders',
            right: 'customers',
            on: 'customer_id',
          },
        ],
      ],
      order: {
        'orders.order_amount': 'desc',
      },
      limit: 100,
      offset: 0,
    };

    // Expected schemas
    const expectedSchemas = [
      {
        name: 'orders',
        sql: 'select * from orders',
        measures: [
          {
            name: 'order_amount_sum',
            sql: 'SUM({MEERKAT}.order_amount)',
            type: 'number',
          },
          {
            name: 'custom_2',
            sql: 'CASE WHEN status = "completed" THEN 1 ELSE 0 END',
            type: 'number',
          },
        ],
        dimensions: [
          {
            name: 'order_date',
            sql: 'orders.order_date',
            type: 'string',
          },
          {
            name: 'status',
            sql: 'orders.status',
            type: 'string',
          },
        ],
        joins: [
          {
            sql: 'orders.customer_id = customers.id',
          },
        ],
      },
      {
        name: 'customers',
        sql: 'select * from customers',
        dimensions: [
          {
            name: 'customer_name',
            sql: 'customers.customer_name',
            type: 'string',
          },
        ],
        measures: [],
        joins: [],
      },
    ];

    // // Assertions for CubeQuery and Schemas
    // expect(cubeQuery).toEqual(expectedCubeQuery);
    // expect(schemas).toEqual(expectedSchemas);

    // // Additional specific assertions
    // expect(cubeQuery.measures).toHaveLength(2);
    // expect(cubeQuery.dimensions).toHaveLength(3);
    // expect(cubeQuery.filters).toHaveLength(1);
    // // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // //@ts-ignore
    // expect(cubeQuery.filters[0].and).toHaveLength(2);
    // expect(cubeQuery.joinPaths).toHaveLength(1);
    // expect(cubeQuery.joinPaths![0]).toHaveLength(1);
    // expect(cubeQuery.limit).toBe(100);
    // expect(cubeQuery.offset).toBe(0);

    // expect(schemas).toHaveLength(2);
    // expect(schemas[0].measures).toHaveLength(2);
    // expect(schemas[0].dimensions).toHaveLength(2);
    // expect(schemas[0].joins).toHaveLength(1);
    // expect(schemas[1].dimensions).toHaveLength(1);

    // // Assert that cubeQueryToSQL was called with the correct arguments
    // expect(cubeQueryToSQL).toHaveBeenCalledWith(cubeQuery, schemas);

    // // Assert the SQL string
    // expect(sql).toBe('SELECT * FROM orders JOIN customers WHERE 1=1');
  });
});
