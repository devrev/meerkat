import {
  ensureColumnAliasBatch,
  ensureTableSchemaAliasSql,
  getColumnNamesFromAst,
  parseExpressions,
} from '@devrev/meerkat-core';
import { duckdbExec } from '../duckdb-exec';

const executeQuery = (query: string) =>
  duckdbExec<Record<string, string>[]>(query);

const getAliasedColumnNames = async (sql: string) => {
  const [parsedExpression] = await parseExpressions([sql], executeQuery);
  if (!parsedExpression) {
    throw new Error('Missing parsed expression');
  }
  return getColumnNamesFromAst(parsedExpression).sort();
};

describe('single-item batch aliasing integration', () => {
  it.each([
    {
      description: 'nested scalar functions',
      tableName: 'orders',
      inputSql: 'ROUND(COALESCE(amount, 0), 2)',
      expectedColumns: ['orders.amount'],
    },
    {
      description: 'aggregate with nested scalar function',
      tableName: 'orders',
      inputSql: "AVG(DATE_DIFF('minute', created_date, first_response_time))",
      expectedColumns: ['orders.created_date', 'orders.first_response_time'],
    },
    {
      description: 'boolean conjunction expression',
      tableName: 'orders',
      inputSql: 'CASE WHEN a > 0 AND b < 10 THEN c ELSE 0 END',
      expectedColumns: ['orders.a', 'orders.b', 'orders.c'],
    },
    {
      description: 'null check expression',
      tableName: 'orders',
      inputSql: 'CASE WHEN deleted_at IS NULL THEN id END',
      expectedColumns: ['orders.deleted_at', 'orders.id'],
    },
    {
      description: 'coalesce expression',
      tableName: 'orders',
      inputSql: 'COALESCE(discount_amount, 0)',
      expectedColumns: ['orders.discount_amount'],
    },
    {
      description: 'nested arithmetic with multiple column references',
      tableName: 'orders',
      inputSql:
        'SUM(COALESCE(unit_price, 0) * COALESCE(quantity, 0) - COALESCE(discount_amount, 0))',
      expectedColumns: [
        'orders.discount_amount',
        'orders.quantity',
        'orders.unit_price',
      ],
    },
    {
      description: 'case expression with multiple branch references',
      tableName: 'orders',
      inputSql:
        "CASE WHEN status = 'open' AND deleted_at IS NULL THEN net_amount ELSE gross_amount END",
      expectedColumns: [
        'orders.deleted_at',
        'orders.gross_amount',
        'orders.net_amount',
        'orders.status',
      ],
    },
    {
      description: 'between expression with multiple date references',
      tableName: 'orders',
      inputSql:
        'CASE WHEN created_at BETWEEN booked_at AND fulfilled_at THEN revenue ELSE 0 END',
      expectedColumns: [
        'orders.booked_at',
        'orders.created_at',
        'orders.fulfilled_at',
        'orders.revenue',
      ],
    },
    {
      description: 'coalesce chain with repeated arithmetic references',
      tableName: 'orders',
      inputSql:
        'COALESCE(subtotal, 0) + COALESCE(tax_amount, 0) - COALESCE(discount_amount, 0)',
      expectedColumns: [
        'orders.discount_amount',
        'orders.subtotal',
        'orders.tax_amount',
      ],
    },
    {
      description: 'json extract operator',
      tableName: 'tickets',
      inputSql: "stage_json->>'name'",
      expectedColumns: ['tickets.stage_json'],
    },
    {
      description: 'array function',
      tableName: 'tickets',
      inputSql: 'ARRAY_LENGTH(owner_ids)',
      expectedColumns: ['tickets.owner_ids'],
    },
    {
      description: 'reference to a different table remains untouched',
      tableName: 'orders',
      inputSql: 'customers.id',
      expectedColumns: ['customers.id'],
    },
  ])(
    'ensures alias for $description',
    async ({ tableName, inputSql, expectedColumns }) => {
      const [aliasedItem] = await ensureColumnAliasBatch({
        items: [{ sql: inputSql, tableName }],
        executeQuery,
      });

      if (!aliasedItem) {
        throw new Error('Missing aliasing result');
      }

      await expect(getAliasedColumnNames(aliasedItem.sql)).resolves.toEqual(
        expectedColumns
      );
    }
  );
});

describe('ensureTableSchemaAliasSql integration', () => {
  it('rewrites schema members using the DuckDB-backed utils', async () => {
    const tableSchemas: TableSchema[] = [
      {
        name: 'orders',
        sql: 'SELECT * FROM orders',
        measures: [
          {
            name: 'total_amount',
            sql: 'SUM(order_amount)',
            type: 'number',
          },
          {
            name: 'conditional_amount',
            sql: "SUM(CASE WHEN status = 'open' THEN order_amount END)",
            type: 'number',
          },
          {
            name: 'blended_amount',
            sql: 'SUM(COALESCE(net_amount, gross_amount) - discount_amount)',
            type: 'number',
          },
        ],
        dimensions: [
          {
            name: 'order_month',
            sql: "DATE_TRUNC('month', order_date)",
            type: 'time',
          },
          {
            name: 'customer_id',
            sql: 'customer_id',
            type: 'string',
          },
          {
            name: 'order_health',
            sql: 'CASE WHEN refunded_at IS NULL AND settled_at IS NOT NULL THEN status ELSE fallback_status END',
            type: 'string',
          },
        ],
      },
    ];

    const aliasedSchemas = await ensureTableSchemaAliasSql({
      tableSchemas,
      ensureExpressionAlias: async ({ items }) => {
        const aliasedItems = await ensureColumnAliasBatch({
          items: items.map((item) => ({
            sql: item.sql,
            tableName: item.context.tableName,
          })),
          executeQuery,
        });

        return aliasedItems.map((item) => item.sql);
      },
    });

    expect(
      await getAliasedColumnNames(aliasedSchemas[0].measures[0].sql)
    ).toEqual(['orders.order_amount']);
    expect(
      await getAliasedColumnNames(aliasedSchemas[0].measures[1].sql)
    ).toEqual(['orders.order_amount', 'orders.status']);
    expect(
      await getAliasedColumnNames(aliasedSchemas[0].measures[2].sql)
    ).toEqual([
      'orders.discount_amount',
      'orders.gross_amount',
      'orders.net_amount',
    ]);
    expect(
      await getAliasedColumnNames(aliasedSchemas[0].dimensions[0].sql)
    ).toEqual(['orders.order_date']);
    expect(
      await getAliasedColumnNames(aliasedSchemas[0].dimensions[1].sql)
    ).toEqual(['orders.customer_id']);
    expect(
      await getAliasedColumnNames(aliasedSchemas[0].dimensions[2].sql)
    ).toEqual([
      'orders.fallback_status',
      'orders.refunded_at',
      'orders.settled_at',
      'orders.status',
    ]);
  });

  it('ensures alias for schema members through the single batched schema path', async () => {
    const tableSchemas: TableSchema[] = [
      {
        name: 'orders',
        sql: 'SELECT * FROM orders',
        measures: [
          {
            name: 'blended_amount',
            sql: 'SUM(COALESCE(net_amount, gross_amount) - discount_amount)',
            type: 'number',
          },
          {
            name: 'between_amount',
            sql: 'SUM(CASE WHEN created_at BETWEEN booked_at AND fulfilled_at THEN revenue ELSE 0 END)',
            type: 'number',
          },
        ],
        dimensions: [
          {
            name: 'order_health',
            sql: 'CASE WHEN refunded_at IS NULL AND settled_at IS NOT NULL THEN status ELSE fallback_status END',
            type: 'string',
          },
          {
            name: 'order_month',
            sql: "DATE_TRUNC('month', created_at)",
            type: 'time',
          },
        ],
      },
    ];

    const aliasedSchemas = await ensureTableSchemaAliasSql({
      tableSchemas,
      ensureExpressionAlias: async ({ items }) => {
        const aliasedItems = await ensureColumnAliasBatch({
          items: items.map((item) => ({
            sql: item.sql,
            tableName: item.context.tableName,
          })),
          executeQuery,
        });

        return aliasedItems.map((item) => item.sql);
      },
    });

    expect(
      await getAliasedColumnNames(aliasedSchemas[0].measures[0].sql)
    ).toEqual([
      'orders.discount_amount',
      'orders.gross_amount',
      'orders.net_amount',
    ]);
    expect(
      await getAliasedColumnNames(aliasedSchemas[0].measures[1].sql)
    ).toEqual([
      'orders.booked_at',
      'orders.created_at',
      'orders.fulfilled_at',
      'orders.revenue',
    ]);
    expect(
      await getAliasedColumnNames(aliasedSchemas[0].dimensions[0].sql)
    ).toEqual([
      'orders.fallback_status',
      'orders.refunded_at',
      'orders.settled_at',
      'orders.status',
    ]);
    expect(
      await getAliasedColumnNames(aliasedSchemas[0].dimensions[1].sql)
    ).toEqual(['orders.created_at']);
  });
});
