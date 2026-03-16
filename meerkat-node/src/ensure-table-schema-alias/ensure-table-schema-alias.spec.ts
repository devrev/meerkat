import type { TableSchema } from '@devrev/meerkat-core';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const {
  ensureColumnAliasBatch,
  ensureTableSchemaAliasSql,
  duckdbExec,
} = vi.hoisted(() => ({
  ensureColumnAliasBatch: vi.fn(),
  ensureTableSchemaAliasSql: vi.fn(),
  duckdbExec: vi.fn(),
}));

vi.mock('@devrev/meerkat-core', () => ({
  ensureColumnAliasBatch,
  ensureTableSchemaAliasSql,
}));

vi.mock('../duckdb-exec', () => ({
  duckdbExec,
}));

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
      dimensions: [
        {
          name: 'customer_id',
          sql: 'customer_id',
          type: 'string',
        },
      ],
    },
  ];

  beforeEach(() => {
    vi.clearAllMocks();
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

  it('ensures alias for schemas using duckdbExec plumbing', async () => {
    const result = await ensureTableSchemasAlias(tableSchemas);

    expect(ensureTableSchemaAliasSql).toHaveBeenCalledWith({
      tableSchemas,
      ensureExpressionAlias: expect.any(Function),
    });
    expect(ensureColumnAliasBatch).toHaveBeenCalledWith({
      items: [
        {
          sql: 'SUM(order_amount)',
          tableName: 'orders',
        },
      ],
      executeQuery: expect.any(Function),
    });
    expect(result[0].measures[0].sql).toBe('SUM(orders.order_amount)');
  });

  it('returns a reusable factory', async () => {
    const factory = ensureTableSchemaAlias();
    const result = await factory(tableSchemas);

    expect(ensureTableSchemaAliasSql).toHaveBeenCalledTimes(1);
    expect(result).toHaveLength(1);
  });
});
