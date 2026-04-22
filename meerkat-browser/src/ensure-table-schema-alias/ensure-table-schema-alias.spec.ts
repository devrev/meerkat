import type { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import type { TableSchema } from '@devrev/meerkat-core';

jest.mock('@devrev/meerkat-core', () => ({
  ensureColumnAliasBatch: jest.fn(),
  ensureTableSchemaAliasSql: jest.fn(),
}));

import * as meerkatCore from '@devrev/meerkat-core';
import {
  ensureTableSchemaAlias,
  ensureTableSchemasAlias,
} from './ensure-table-schema-alias';

describe('ensureTableSchemasAlias', () => {
  const tableSchemas: TableSchema[] = [
    {
      name: 'orders',
      sql: 'SELECT * FROM orders',
      measures: [
        {
          name: 'gross_amount',
          sql: 'SUM(order_amount)',
          type: 'number',
        },
      ],
      dimensions: [],
    },
  ];

  beforeEach(() => {
    jest.clearAllMocks();
    const ensureColumnAliasBatch =
      meerkatCore.ensureColumnAliasBatch as jest.Mock;
    const ensureTableSchemaAliasSql =
      meerkatCore.ensureTableSchemaAliasSql as jest.Mock;

    ensureColumnAliasBatch.mockResolvedValue([
      {
        sql: 'SUM(orders.order_amount)',
        tableName: 'orders',
        didChange: true,
      },
    ]);
    ensureTableSchemaAliasSql.mockImplementation(
      async ({ tableSchemas, ensureExpressionAlias }) => {
        const batchedSql = await ensureExpressionAlias({
          items: [
            {
              sql: 'SUM(order_amount)',
              context: {
                tableName: 'orders',
                memberName: 'gross_amount',
                memberType: 'measure',
              },
            },
          ],
        });

        return [
          {
            ...tableSchemas[0],
            measures: [
              {
                ...tableSchemas[0].measures[0],
                sql: batchedSql[0],
              },
            ],
          },
        ];
      }
    );
  });

  it('ensures alias for schemas using the provided browser connection', async () => {
    const connection = {
      query: jest.fn(async () => ({
        toArray: () => [
          {
            toJSON: () => ({ json_serialize_sql: '{}' }),
          },
        ],
      })),
    } as unknown as AsyncDuckDBConnection;

    const result = await ensureTableSchemasAlias({
      connection,
      tableSchemas,
    });

    const ensureColumnAliasBatch =
      meerkatCore.ensureColumnAliasBatch as jest.Mock;

    expect(ensureColumnAliasBatch).toHaveBeenCalledWith({
      items: [
        expect.objectContaining({
          sql: 'SUM(order_amount)',
          tableName: 'orders',
        }),
      ],
      executeQuery: expect.any(Function),
    });
    expect(result[0].measures[0].sql).toBe('SUM(orders.order_amount)');
  });

  it('returns a reusable factory bound to the connection', async () => {
    const connection = {
      query: jest.fn(),
    } as unknown as AsyncDuckDBConnection;
    const factory = ensureTableSchemaAlias(connection);

    await factory(tableSchemas);

    const ensureTableSchemaAliasSql =
      meerkatCore.ensureTableSchemaAliasSql as jest.Mock;
    expect(ensureTableSchemaAliasSql).toHaveBeenCalledTimes(1);
  });
});
