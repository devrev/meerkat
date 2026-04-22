import { TableSchema } from '../../types/cube-types';

export interface EnsureColumnAliasScenario {
  description: string;
  tableName: string;
  inputSql: string;
  expectedSql: string;
  shouldChange: boolean;
  notes?: string;
  knownTableNames?: string[];
}

export interface DeferredEnsureColumnAliasScenario
  extends EnsureColumnAliasScenario {
  deferredReason: string;
}

export const ENSURE_COLUMN_ALIAS_SCENARIOS: EnsureColumnAliasScenario[] = [
  {
    description: 'bare dimension column',
    tableName: 'orders',
    inputSql: 'customer_id',
    expectedSql: 'orders.customer_id',
    shouldChange: true,
  },
  {
    description: 'bare aggregate argument',
    tableName: 'orders',
    inputSql: 'SUM(order_amount)',
    expectedSql: 'SUM(orders.order_amount)',
    shouldChange: true,
  },
  {
    description: 'bare aggregate argument for customers schema',
    tableName: 'customers',
    inputSql: 'SUM(order_amount)',
    expectedSql: 'SUM(customers.order_amount)',
    shouldChange: true,
  },
  {
    description: 'multiple bare columns in arithmetic expression',
    tableName: 'orders',
    inputSql: 'price * quantity',
    expectedSql: 'orders.price * orders.quantity',
    shouldChange: true,
  },
  {
    description: 'mixed qualified and unqualified references',
    tableName: 'orders',
    inputSql: 'orders.price * quantity',
    expectedSql: 'orders.price * orders.quantity',
    shouldChange: true,
  },
  {
    description: 'already qualified column reference',
    tableName: 'orders',
    inputSql: 'orders.order_amount',
    expectedSql: 'orders.order_amount',
    shouldChange: false,
  },
  {
    description: 'already qualified column reference for customers schema',
    tableName: 'customers',
    inputSql: 'customers.order_amount',
    expectedSql: 'customers.order_amount',
    shouldChange: false,
  },
  {
    description: 'cast expression',
    tableName: 'orders',
    inputSql: 'CAST(amount AS DOUBLE)',
    expectedSql: 'CAST(orders.amount AS DOUBLE)',
    shouldChange: true,
  },
  {
    description: 'nested scalar functions',
    tableName: 'orders',
    inputSql: 'ROUND(COALESCE(amount, 0), 2)',
    expectedSql: 'ROUND(COALESCE(orders.amount, 0), 2)',
    shouldChange: true,
  },
  {
    description: 'aggregate with nested scalar function',
    tableName: 'orders',
    inputSql: "AVG(DATE_DIFF('minute', created_date, first_response_time))",
    expectedSql:
      "AVG(DATE_DIFF('minute', orders.created_date, orders.first_response_time))",
    shouldChange: true,
  },
  {
    description: 'count distinct',
    tableName: 'orders',
    inputSql: 'COUNT(DISTINCT id)',
    expectedSql: 'COUNT(DISTINCT orders.id)',
    shouldChange: true,
  },
  {
    description: 'case expression with comparison and result column',
    tableName: 'orders',
    inputSql: "CASE WHEN status = 'open' THEN id END",
    expectedSql: "CASE WHEN orders.status = 'open' THEN orders.id END",
    shouldChange: true,
  },
  {
    description: 'boolean conjunction expression',
    tableName: 'orders',
    inputSql: 'CASE WHEN a > 0 AND b < 10 THEN c ELSE 0 END',
    expectedSql:
      'CASE WHEN orders.a > 0 AND orders.b < 10 THEN orders.c ELSE 0 END',
    shouldChange: true,
  },
  {
    description: 'null check expression',
    tableName: 'orders',
    inputSql: 'CASE WHEN deleted_at IS NULL THEN id END',
    expectedSql:
      'CASE WHEN orders.deleted_at IS NULL THEN orders.id END',
    shouldChange: true,
  },
  {
    description: 'coalesce expression',
    tableName: 'orders',
    inputSql: 'COALESCE(discount_amount, 0)',
    expectedSql: 'COALESCE(orders.discount_amount, 0)',
    shouldChange: true,
  },
  {
    description: 'json extract operator',
    tableName: 'tickets',
    inputSql: "stage_json->>'name'",
    expectedSql: "tickets.stage_json->>'name'",
    shouldChange: true,
  },
  {
    description: 'array function',
    tableName: 'tickets',
    inputSql: 'ARRAY_LENGTH(owner_ids)',
    expectedSql: 'ARRAY_LENGTH(tickets.owner_ids)',
    shouldChange: true,
  },
  {
    description: 'string literal with column-like content',
    tableName: 'orders',
    inputSql: "'customer_id'",
    expectedSql: "'customer_id'",
    shouldChange: false,
    notes: 'Column-like text inside string literals should not be rewritten.',
  },
  {
    description: 'quoted identifier',
    tableName: 'orders',
    inputSql: '"Order ID"',
    expectedSql: '"Order ID"',
    shouldChange: false,
  },
  {
    description: 'reference to a different table remains untouched',
    tableName: 'orders',
    inputSql: 'customers.id',
    expectedSql: 'customers.id',
    shouldChange: false,
  },
  {
    description: 'struct field access on local column is qualified',
    tableName: 'issue',
    knownTableNames: ['issue', 'devusers'],
    inputSql: 'stage.stage_id',
    expectedSql: 'issue.stage.stage_id',
    shouldChange: true,
  },
  {
    description:
      'struct field access inside aggregate on local column is qualified',
    tableName: 'issue',
    knownTableNames: ['issue', 'devusers'],
    inputSql: 'COUNT(stage.stage_id)',
    expectedSql: 'COUNT(issue.stage.stage_id)',
    shouldChange: true,
  },
  {
    description:
      'multi-part ref where leading identifier is another table stays untouched',
    tableName: 'orders',
    knownTableNames: ['orders', 'customers'],
    inputSql: 'customers.id',
    expectedSql: 'customers.id',
    shouldChange: false,
  },
  {
    description: 'already-qualified struct access is not double-qualified',
    tableName: 'issue',
    knownTableNames: ['issue'],
    inputSql: 'issue.stage.stage_id',
    expectedSql: 'issue.stage.stage_id',
    shouldChange: false,
  },
];

export const DEFERRED_ENSURE_COLUMN_ALIAS_SCENARIOS: DeferredEnsureColumnAliasScenario[] =
  [
    {
      description: 'window aggregate with order expression',
      tableName: 'orders',
      inputSql:
        'AVG(COUNT(order_id)) OVER (ORDER BY record_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)',
      expectedSql:
        'AVG(COUNT(orders.order_id)) OVER (ORDER BY orders.record_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)',
      shouldChange: true,
      deferredReason:
        'Needs dedicated coverage for window order and frame AST traversal.',
    },
    {
      description: 'aggregate filter clause',
      tableName: 'orders',
      inputSql: "SUM(amount) FILTER (WHERE direction = 'Income')",
      expectedSql:
        "SUM(orders.amount) FILTER (WHERE orders.direction = 'Income')",
      shouldChange: true,
      deferredReason:
        'DuckDB filter-clause AST shape should be verified before enabling.',
    },
    {
      description: 'subquery expression',
      tableName: 'orders',
      inputSql:
        "(SELECT CASE WHEN COUNT(DISTINCT id) > 0 THEN MAX(created_at) END)",
      expectedSql:
        "(SELECT CASE WHEN COUNT(DISTINCT orders.id) > 0 THEN MAX(orders.created_at) END)",
      shouldChange: true,
      deferredReason:
        'Nested SELECT rewriting should be confirmed against actual parser output.',
    },
    {
      description: 'three-part qualified identifier',
      tableName: 'orders',
      inputSql: 'analytics.orders.amount',
      expectedSql: 'analytics.orders.amount',
      shouldChange: false,
      deferredReason:
        'Need to confirm whether DuckDB returns three-part names in member expressions.',
    },
  ];

export const createEnsureTableSchemaAliasSqlFixture = (): TableSchema[] => [
  {
    name: 'orders',
    sql: 'SELECT * FROM orders',
    measures: [
      {
        name: 'gross_amount',
        sql: 'SUM(order_amount)',
        type: 'number',
        alias: 'Gross Amount',
      },
      {
        name: 'net_amount',
        sql: 'SUM(order_amount - discount_amount)',
        type: 'number',
      },
    ],
    dimensions: [
      {
        name: 'customer_id',
        sql: 'customer_id',
        type: 'string',
        alias: 'Customer ID',
      },
      {
        name: 'order_month',
        sql: "DATE_TRUNC('month', created_at)",
        type: 'time',
        modifier: {
          shouldUnnestGroupBy: false,
        },
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
    sql: 'SELECT * FROM customers',
    measures: [
      {
        name: 'gross_amount',
        sql: 'SUM(order_amount)',
        type: 'number',
      },
    ],
    dimensions: [
      {
        name: 'id',
        sql: 'id',
        type: 'string',
      },
    ],
  },
];
