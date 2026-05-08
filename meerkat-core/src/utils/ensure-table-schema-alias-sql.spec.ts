import { createEnsureTableSchemaAliasSqlFixture } from './__fixtures__/ensure-sql-expression-column-alias.fixtures';
import {
  EnsureAliasExpressionContext,
  ensureTableSchemaAliasSql,
} from './ensure-table-schema-alias-sql';

describe('ensureTableSchemaAliasSql', () => {
  it('rewrites measure and dimension SQL while preserving schema-level SQL and joins', async () => {
    const tableSchemas = createEnsureTableSchemaAliasSqlFixture();
    const aliasedSqlByMember = new Map([
      ['orders.gross_amount', 'SUM(orders.order_amount)'],
      [
        'orders.net_amount',
        'SUM(orders.order_amount - orders.discount_amount)',
      ],
      ['orders.customer_id', 'orders.customer_id'],
      ['orders.order_month', "DATE_TRUNC('month', orders.created_at)"],
      ['customers.gross_amount', 'SUM(customers.order_amount)'],
      ['customers.id', 'customers.id'],
    ]);
    const ensureExpressionAlias = jest.fn(async ({ items }) =>
      items.map(({ context }: { context: EnsureAliasExpressionContext }) => {
        const aliasedSql = aliasedSqlByMember.get(
          `${context.tableName}.${context.memberName}`
        );

        if (!aliasedSql) {
          throw new Error('missing aliased SQL fixture');
        }

        return aliasedSql;
      })
    );

    const result = await ensureTableSchemaAliasSql({
      tableSchemas,
      ensureExpressionAlias,
    });

    expect(result).toEqual([
      {
        ...tableSchemas[0],
        measures: [
          {
            ...tableSchemas[0].measures[0],
            sql: 'SUM(orders.order_amount)',
          },
          {
            ...tableSchemas[0].measures[1],
            sql: 'SUM(orders.order_amount - orders.discount_amount)',
          },
        ],
        dimensions: [
          {
            ...tableSchemas[0].dimensions[0],
            sql: 'orders.customer_id',
          },
          {
            ...tableSchemas[0].dimensions[1],
            sql: "DATE_TRUNC('month', orders.created_at)",
          },
        ],
      },
      {
        ...tableSchemas[1],
        measures: [
          {
            ...tableSchemas[1].measures[0],
            sql: 'SUM(customers.order_amount)',
          },
        ],
        dimensions: [
          {
            ...tableSchemas[1].dimensions[0],
            sql: 'customers.id',
          },
        ],
      },
    ]);

    expect(result[0].sql).toBe('SELECT * FROM orders');
    expect(result[0].joins).toEqual([
      {
        sql: 'orders.customer_id = customers.id',
      },
    ]);

    expect(ensureExpressionAlias).toHaveBeenCalledTimes(2);
    expect(ensureExpressionAlias).toHaveBeenCalledWith({
      items: expect.arrayContaining([
        expect.objectContaining({
          sql: 'SUM(order_amount)',
          context: expect.objectContaining({
            tableName: 'orders',
            memberName: 'gross_amount',
            memberType: 'measure',
          }),
        }),
      ]),
    });
  });

  it('does not mutate the original input schemas', async () => {
    const tableSchemas = createEnsureTableSchemaAliasSqlFixture();
    const originalSnapshot = JSON.parse(JSON.stringify(tableSchemas));

    await ensureTableSchemaAliasSql({
      tableSchemas,
      ensureExpressionAlias: async ({ items }) =>
        items.map(({ sql }: { sql: string }) => `mockAliased:${sql}`),
    });

    expect(tableSchemas).toEqual(originalSnapshot);
  });

  it('wraps aliasing failures with table context', async () => {
    const tableSchemas = createEnsureTableSchemaAliasSqlFixture();

    await expect(
      ensureTableSchemaAliasSql({
        tableSchemas,
        ensureExpressionAlias: async ({ items }) =>
          items.map(
            ({ context }: { context: EnsureAliasExpressionContext }) => {
              if (context.memberName === 'order_month') {
                throw new Error('unsupported expression');
              }

              return 'ok';
            }
          ),
      })
    ).rejects.toThrow(
      'Failed to ensure alias for table orders: unsupported expression'
    );
  });

  it('resolves meerkat placeholders to aliases before ensuring alias', async () => {
    const tableSchemas = createEnsureTableSchemaAliasSqlFixture();
    const placeholderSchemas = [
      {
        ...tableSchemas[0],
        measures: [
          ...tableSchemas[0].measures,
          {
            name: 'placeholder_measure',
            sql: 'SUM({MEERKAT}.order_amount)',
            type: 'number' as const,
          },
        ],
        dimensions: [
          ...tableSchemas[0].dimensions,
          {
            name: 'placeholder_dimension',
            sql: '{MEERKAT}.customer_id',
            type: 'string' as const,
          },
        ],
      },
    ];
    const ensureExpressionAlias = jest.fn(async ({ items }) =>
      items.map(({ sql }: { sql: string }) => `mockAliased:${sql}`)
    );

    const result = await ensureTableSchemaAliasSql({
      tableSchemas: placeholderSchemas,
      ensureExpressionAlias,
    });

    expect(result[0].measures[0].sql).toBe('mockAliased:SUM(order_amount)');
    expect(result[0].dimensions[0].sql).toBe('mockAliased:customer_id');
    expect(result[0].measures[2].sql).toBe(
      'mockAliased:SUM({MEERKAT}.order_amount)'
    );
    expect(result[0].dimensions[2].sql).toBe(
      'mockAliased:{MEERKAT}.customer_id'
    );
    expect(ensureExpressionAlias).toHaveBeenCalledTimes(1);
    expect(ensureExpressionAlias).toHaveBeenCalledWith({
      items: expect.arrayContaining([
        {
          sql: 'SUM(order_amount)',
          context: expect.objectContaining({
            tableName: 'orders',
            memberName: 'gross_amount',
          }),
        },
        {
          sql: 'SUM({MEERKAT}.order_amount)',
          context: expect.objectContaining({
            tableName: 'orders',
            memberName: 'placeholder_measure',
          }),
        },
        {
          sql: '{MEERKAT}.customer_id',
          context: expect.objectContaining({
            tableName: 'orders',
            memberName: 'placeholder_dimension',
          }),
        },
      ]),
    });
  });

  it('uses batched aliasing while preserving member order', async () => {
    const tableSchemas = createEnsureTableSchemaAliasSqlFixture();
    const ensureExpressionAlias = jest.fn(async ({ items }) =>
      items.map(
        ({ context }: { context: EnsureAliasExpressionContext }) =>
          `batched(${context.tableName}.${context.memberName})`
      )
    );

    const result = await ensureTableSchemaAliasSql({
      tableSchemas,
      ensureExpressionAlias,
    });

    expect(ensureExpressionAlias).toHaveBeenCalledTimes(2);
    expect(ensureExpressionAlias).toHaveBeenNthCalledWith(1, {
      items: [
        expect.objectContaining({
          sql: 'SUM(order_amount)',
          context: expect.objectContaining({
            tableName: 'orders',
            memberName: 'gross_amount',
            memberType: 'measure',
          }),
        }),
        expect.objectContaining({
          sql: 'SUM(order_amount - discount_amount)',
          context: expect.objectContaining({
            tableName: 'orders',
            memberName: 'net_amount',
            memberType: 'measure',
          }),
        }),
        expect.objectContaining({
          sql: 'customer_id',
          context: expect.objectContaining({
            tableName: 'orders',
            memberName: 'customer_id',
            memberType: 'dimension',
          }),
        }),
        expect.objectContaining({
          sql: "DATE_TRUNC('month', created_at)",
          context: expect.objectContaining({
            tableName: 'orders',
            memberName: 'order_month',
            memberType: 'dimension',
          }),
        }),
      ],
    });
    expect(result[0].measures.map((measure) => measure.sql)).toEqual([
      'batched(orders.gross_amount)',
      'batched(orders.net_amount)',
    ]);
    expect(result[0].dimensions.map((dimension) => dimension.sql)).toEqual([
      'batched(orders.customer_id)',
      'batched(orders.order_month)',
    ]);
    expect(result[1].measures[0].sql).toBe('batched(customers.gross_amount)');
    expect(result[1].dimensions[0].sql).toBe('batched(customers.id)');
  });

  it('fails immediately when a batched table aliasing fails', async () => {
    const tableSchemas = createEnsureTableSchemaAliasSqlFixture();
    const ensureExpressionAlias = jest.fn(async ({ items }) => {
      throw new Error(`chunk parse failed (${items.length})`);
    });

    await expect(
      ensureTableSchemaAliasSql({
        tableSchemas,
        ensureExpressionAlias,
      })
    ).rejects.toThrow(
      'Failed to ensure alias for table orders: chunk parse failed (4)'
    );

    expect(ensureExpressionAlias).toHaveBeenCalled();
    expect(ensureExpressionAlias).toHaveBeenCalledWith({
      items: [
        expect.objectContaining({
          sql: 'SUM(order_amount)',
          context: expect.objectContaining({
            tableName: 'orders',
            memberName: 'gross_amount',
            memberType: 'measure',
          }),
        }),
        expect.objectContaining({
          sql: 'SUM(order_amount - discount_amount)',
          context: expect.objectContaining({
            tableName: 'orders',
            memberName: 'net_amount',
            memberType: 'measure',
          }),
        }),
        expect.objectContaining({
          sql: 'customer_id',
          context: expect.objectContaining({
            tableName: 'orders',
            memberName: 'customer_id',
            memberType: 'dimension',
          }),
        }),
        expect.objectContaining({
          sql: "DATE_TRUNC('month', created_at)",
          context: expect.objectContaining({
            tableName: 'orders',
            memberName: 'order_month',
            memberType: 'dimension',
          }),
        }),
      ],
    });
  });

  it('threads knownTableNames through the expression aliaser', async () => {
    const tableSchemas = createEnsureTableSchemaAliasSqlFixture();
    const ensureExpressionAlias = jest.fn(async ({ items }) =>
      items.map(({ sql }: { sql: string }) => sql)
    );

    await ensureTableSchemaAliasSql({
      tableSchemas,
      ensureExpressionAlias,
    });

    const [firstCallArgs] = ensureExpressionAlias.mock.calls;
    const [{ items: ordersItems }] = firstCallArgs;
    expect(ordersItems[0].context.knownTableNames).toEqual(
      new Set(['orders', 'customers'])
    );
  });

  it('passes the same knownTableNames to every table in the batch', async () => {
    const tableSchemas = createEnsureTableSchemaAliasSqlFixture();
    const ensureExpressionAlias = jest.fn(async ({ items }) =>
      items.map(({ sql }: { sql: string }) => sql)
    );

    await ensureTableSchemaAliasSql({
      tableSchemas,
      ensureExpressionAlias,
    });

    const expected = new Set(['orders', 'customers']);
    for (const [call] of ensureExpressionAlias.mock.calls) {
      for (const item of call.items) {
        expect(item.context.knownTableNames).toEqual(expected);
      }
    }
  });

  it('uses a knownTableNames set derived from all provided schemas', async () => {
    const schemas = [
      ...createEnsureTableSchemaAliasSqlFixture(),
      {
        name: 'audit',
        sql: 'SELECT * FROM audit',
        measures: [
          { name: 'total', sql: 'COUNT(id)', type: 'number' as const },
        ],
        dimensions: [],
      },
    ];
    const ensureExpressionAlias = jest.fn(async ({ items }) =>
      items.map(({ sql }: { sql: string }) => sql)
    );

    await ensureTableSchemaAliasSql({
      tableSchemas: schemas,
      ensureExpressionAlias,
    });

    const expected = new Set(['orders', 'customers', 'audit']);
    const lastCall =
      ensureExpressionAlias.mock.calls[
        ensureExpressionAlias.mock.calls.length - 1
      ][0];
    expect(lastCall.items[0].context.knownTableNames).toEqual(expected);
  });

  it('does not mutate measures/dimensions when ensureExpressionAlias is a no-op', async () => {
    const tableSchemas = createEnsureTableSchemaAliasSqlFixture();
    const before = JSON.parse(JSON.stringify(tableSchemas));

    const result = await ensureTableSchemaAliasSql({
      tableSchemas,
      ensureExpressionAlias: async ({ items }) =>
        items.map(({ sql }: { sql: string }) => sql),
    });

    expect(tableSchemas).toEqual(before);
    expect(result[0].measures[0].sql).toBe(
      tableSchemas[0].measures[0].sql
    );
    expect(result[0]).not.toBe(tableSchemas[0]);
    expect(result[0].measures).not.toBe(tableSchemas[0].measures);
  });

  it('handles schema with no measures or dimensions without calling aliaser', async () => {
    const schemas = [
      {
        name: 'empty',
        sql: 'SELECT 1',
        measures: [],
        dimensions: [],
      },
    ];
    const ensureExpressionAlias = jest.fn(async ({ items }) =>
      items.map(({ sql }: { sql: string }) => sql)
    );

    const result = await ensureTableSchemaAliasSql({
      tableSchemas: schemas,
      ensureExpressionAlias,
    });

    expect(ensureExpressionAlias).not.toHaveBeenCalled();
    expect(result).toEqual(schemas);
  });
});
