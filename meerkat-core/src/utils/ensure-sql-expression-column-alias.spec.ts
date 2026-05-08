import {
  ExpressionClass,
  ExpressionType,
  ParsedExpression,
  ResultModifierType,
} from '../types/duckdb-serialization-types';
import {
  isCaseExpression,
  isCastExpression,
  isColumnRefExpression,
  isComparisonExpression,
  isConstantExpression,
  isFunctionExpression,
  isLambdaExpression,
  isOperatorExpression,
} from '../types/utils';
import {
  DEFERRED_ENSURE_COLUMN_ALIAS_SCENARIOS,
  ENSURE_COLUMN_ALIAS_SCENARIOS,
} from './__fixtures__/ensure-sql-expression-column-alias.fixtures';
import type { GetQueryOutput } from './duckdb-ast-parse-serialize';

jest.mock('./duckdb-ast-parse-serialize', () => {
  const actual = jest.requireActual('./duckdb-ast-parse-serialize');
  return {
    ...actual,
    parseExpressions: jest.fn(),
    serializeExpressions: jest.fn(),
  };
});

import * as parseSerializeMod from './duckdb-ast-parse-serialize';
import { ensureColumnAliasBatch } from './ensure-sql-expression-column-alias';

const mockParseExpressions = parseSerializeMod.parseExpressions as jest.Mock;
const mockSerializeExpressions =
  parseSerializeMod.serializeExpressions as jest.Mock;

const createColumnRef = (
  columnNames: string | string[],
  alias = ''
): ParsedExpression => ({
  class: ExpressionClass.COLUMN_REF,
  type: ExpressionType.COLUMN_REF,
  alias,
  column_names: Array.isArray(columnNames) ? columnNames : [columnNames],
});

const createStringConstant = (value: string): ParsedExpression => ({
  class: ExpressionClass.CONSTANT,
  type: ExpressionType.VALUE_CONSTANT,
  alias: '',
  value: {
    type: {
      id: 'VARCHAR',
      type_info: null,
    },
    is_null: false,
    value,
  },
});

const createFunction = ({
  functionName,
  children,
  distinct = false,
  isOperator = false,
  filter = null,
}: {
  functionName: string;
  children: ParsedExpression[];
  distinct?: boolean;
  isOperator?: boolean;
  filter?: ParsedExpression | null;
}): ParsedExpression => ({
  class: ExpressionClass.FUNCTION,
  type: ExpressionType.FUNCTION,
  alias: '',
  function_name: functionName,
  schema: '',
  children,
  filter,
  order_bys: {
    type: ResultModifierType.ORDER_MODIFIER,
    orders: [],
  },
  distinct,
  is_operator: isOperator,
  export_state: false,
  catalog: '',
});

const createCast = (
  child: ParsedExpression,
  type = 'DOUBLE'
): ParsedExpression =>
  ({
    class: ExpressionClass.CAST,
    type: ExpressionType.OPERATOR_CAST,
    alias: '',
    child,
    cast_type: {
      id: type,
      type_info: null,
    },
    try_cast: false,
  } as unknown as ParsedExpression);

const createComparison = ({
  type,
  left,
  right,
}: {
  type: ExpressionType;
  left: ParsedExpression;
  right: ParsedExpression;
}): ParsedExpression => ({
  class: ExpressionClass.COMPARISON,
  type,
  alias: '',
  left,
  right,
});

const createCase = ({
  whenExpr,
  thenExpr,
  elseExpr,
}: {
  whenExpr: ParsedExpression;
  thenExpr: ParsedExpression;
  elseExpr: ParsedExpression;
}): ParsedExpression => ({
  class: ExpressionClass.CASE,
  type: ExpressionType.CASE_EXPR,
  alias: '',
  case_checks: [
    {
      when_expr: whenExpr,
      then_expr: thenExpr,
    },
  ],
  else_expr: elseExpr,
});

const createLambda = ({
  lhs,
  expr,
}: {
  lhs: ParsedExpression;
  expr: ParsedExpression;
}): ParsedExpression =>
  ({
    class: ExpressionClass.LAMBDA,
    type: ExpressionType.LAMBDA,
    alias: '',
    lhs,
    expr,
  } as ParsedExpression);

const serializeExpression = (node: ParsedExpression): string => {
  if (isColumnRefExpression(node)) {
    const columnName = node.column_names.join('.');
    return /\s/.test(columnName) ? `"${columnName}"` : columnName;
  }

  if (isConstantExpression(node)) {
    const value = node.value;

    if (typeof value === 'object' && value !== null && 'is_null' in value) {
      if (value.is_null) {
        return 'NULL';
      }

      if (
        typeof value.type === 'object' &&
        value.type !== null &&
        value.type.id === 'VARCHAR'
      ) {
        return `'${String(value.value)}'`;
      }

      return String(value.value);
    }

    return String(value);
  }

  if (isCastExpression(node)) {
    return `CAST(${serializeExpression(node.child)} AS ${String(
      node.cast_type.id
    )})`;
  }

  if (isComparisonExpression(node)) {
    const operatorMap: Partial<Record<ExpressionType, string>> = {
      [ExpressionType.COMPARE_EQUAL]: '=',
      [ExpressionType.COMPARE_GREATERTHAN]: '>',
      [ExpressionType.COMPARE_LESSTHAN]: '<',
    };
    const operator = operatorMap[node.type] || node.type;
    return `${serializeExpression(node.left)} ${operator} ${serializeExpression(
      node.right
    )}`;
  }

  if (isCaseExpression(node)) {
    const caseChecks = node.case_checks
      .map(
        (check) =>
          `WHEN ${serializeExpression(
            check.when_expr
          )} THEN ${serializeExpression(check.then_expr)}`
      )
      .join(' ');
    const elseExpression = node.else_expr as ParsedExpression;
    const shouldOmitElse =
      isConstantExpression(elseExpression) &&
      typeof elseExpression.value === 'object' &&
      elseExpression.value !== null &&
      'is_null' in elseExpression.value &&
      elseExpression.value.is_null;

    if (shouldOmitElse) {
      return `CASE ${caseChecks} END`;
    }

    return `CASE ${caseChecks} ELSE ${serializeExpression(elseExpression)} END`;
  }

  if (isOperatorExpression(node)) {
    if (node.type === ExpressionType.OPERATOR_COALESCE) {
      return `COALESCE(${node.children
        .map((child) => serializeExpression(child))
        .join(', ')})`;
    }

    if (node.type === ExpressionType.OPERATOR_IS_NULL) {
      return `${serializeExpression(node.children[0])} IS NULL`;
    }

    return `${node.type}(${node.children
      .map((child) => serializeExpression(child))
      .join(', ')})`;
  }

  if (isFunctionExpression(node)) {
    if (node.is_operator && node.children.length === 2) {
      return `${serializeExpression(node.children[0])} ${
        node.function_name
      } ${serializeExpression(node.children[1])}`;
    }

    const args = node.children.map((child) => serializeExpression(child));
    const serializedArgs =
      node.distinct && args.length > 0
        ? [`DISTINCT ${args[0]}`, ...args.slice(1)]
        : args;
    const base = `${node.function_name.toUpperCase()}(${serializedArgs.join(
      ', '
    )})`;

    if (!node.filter) {
      return base;
    }

    return `${base} FILTER (WHERE ${serializeExpression(node.filter)})`;
  }

  if (isLambdaExpression(node)) {
    return `${serializeExpression(node.lhs)} -> ${serializeExpression(
      node.expr as ParsedExpression
    )}`;
  }

  throw new Error(
    `Unsupported expression class in test serializer: ${node.class}`
  );
};

const expressionAstBySql: Record<string, ParsedExpression> = {
  customer_id: createColumnRef('customer_id'),
  'SUM(order_amount)': createFunction({
    functionName: 'SUM',
    children: [createColumnRef('order_amount')],
  }),
  'price * quantity': createFunction({
    functionName: '*',
    children: [createColumnRef('price'), createColumnRef('quantity')],
    isOperator: true,
  }),
  'orders.price * quantity': createFunction({
    functionName: '*',
    children: [
      createColumnRef(['orders', 'price']),
      createColumnRef('quantity'),
    ],
    isOperator: true,
  }),
  'orders.order_amount': createColumnRef(['orders', 'order_amount']),
  'customers.order_amount': createColumnRef(['customers', 'order_amount']),
  'customers.id': createColumnRef(['customers', 'id']),
  'CAST(amount AS DOUBLE)': createCast(createColumnRef('amount')),
  'COUNT(DISTINCT id)': createFunction({
    functionName: 'COUNT',
    children: [createColumnRef('id')],
    distinct: true,
  }),
  "CASE WHEN status = 'open' THEN id END": createCase({
    whenExpr: createComparison({
      type: ExpressionType.COMPARE_EQUAL,
      left: createColumnRef('status'),
      right: createStringConstant('open'),
    }),
    thenExpr: createColumnRef('id'),
    elseExpr: {
      class: ExpressionClass.CONSTANT,
      type: ExpressionType.VALUE_CONSTANT,
      alias: '',
      value: {
        type: {
          id: 'NULL',
          type_info: null,
        },
        is_null: true,
      },
    },
  }),
  "'customer_id'": createStringConstant('customer_id'),
  '"Order ID"': createColumnRef('Order ID'),
  'stage.stage_id': createColumnRef(['stage', 'stage_id']),
  'issue.stage.stage_id': createColumnRef(['issue', 'stage', 'stage_id']),
  'COUNT(stage.stage_id)': createFunction({
    functionName: 'COUNT',
    children: [createColumnRef(['stage', 'stage_id'])],
  }),
  'foo.bar': createColumnRef(['foo', 'bar']),
  missing_column: createColumnRef('missing_column'),
  'stage.stage_id + amount': createFunction({
    functionName: '+',
    children: [
      createColumnRef(['stage', 'stage_id']),
      createColumnRef('amount'),
    ],
    isOperator: true,
  }),
  'stage.stage_id = devusers.id': createComparison({
    type: ExpressionType.COMPARE_EQUAL,
    left: createColumnRef(['stage', 'stage_id']),
    right: createColumnRef(['devusers', 'id']),
  }),
  'CASE WHEN stage.stage_id = 1 THEN owner.id END': createCase({
    whenExpr: createComparison({
      type: ExpressionType.COMPARE_EQUAL,
      left: createColumnRef(['stage', 'stage_id']),
      right: {
        class: ExpressionClass.CONSTANT,
        type: ExpressionType.VALUE_CONSTANT,
        alias: '',
        value: {
          type: { id: 'INTEGER', type_info: null },
          is_null: false,
          value: 1,
        },
      } as ParsedExpression,
    }),
    thenExpr: createColumnRef(['owner', 'id']),
    elseExpr: {
      class: ExpressionClass.CONSTANT,
      type: ExpressionType.VALUE_CONSTANT,
      alias: '',
      value: {
        type: { id: 'NULL', type_info: null },
        is_null: true,
      },
    } as ParsedExpression,
  }),
  'SUM(stage.stage_id)': createFunction({
    functionName: 'SUM',
    children: [createColumnRef(['stage', 'stage_id'])],
  }),
  'list_transform(stage.items, x -> x)': createFunction({
    functionName: 'list_transform',
    children: [
      createColumnRef(['stage', 'items']),
      createLambda({
        lhs: createColumnRef('x'),
        expr: createColumnRef('x'),
      }),
    ],
  }),
  "list_transform(priority_tags, x -> CASE WHEN x = 1 THEN 'P1' ELSE 'Unknown' END)":
    createFunction({
      functionName: 'list_transform',
      children: [
        createColumnRef('priority_tags'),
        createLambda({
          lhs: createColumnRef('x'),
          expr: createCase({
            whenExpr: createComparison({
              type: ExpressionType.COMPARE_EQUAL,
              left: createColumnRef('x'),
              right: {
                class: ExpressionClass.CONSTANT,
                type: ExpressionType.VALUE_CONSTANT,
                alias: '',
                value: {
                  type: {
                    id: 'INTEGER',
                    type_info: null,
                  },
                  is_null: false,
                  value: 1,
                },
              } as ParsedExpression,
            }),
            thenExpr: createStringConstant('P1'),
            elseExpr: createStringConstant('Unknown'),
          }),
        }),
      ],
    }),
};

const parseExpressionFixtures = async (sqls: string[]) => {
  return sqls.map((sql) => {
    const expression = expressionAstBySql[sql];
    if (!expression) {
      throw new Error(`Missing AST fixture for ${sql}`);
    }
    return JSON.parse(JSON.stringify(expression)) as ParsedExpression;
  });
};

const serializeExpressionFixtures = async (expressions: ParsedExpression[]) => {
  return expressions.map((ast) => serializeExpression(ast));
};

const dummyGetQueryOutput: GetQueryOutput = async () => [];

beforeEach(() => {
  mockParseExpressions.mockImplementation(async (sqls: string[]) =>
    parseExpressionFixtures(sqls)
  );
  mockSerializeExpressions.mockImplementation(
    async (expressions: ParsedExpression[]) =>
      serializeExpressionFixtures(expressions)
  );
});

afterEach(() => {
  jest.clearAllMocks();
});

const ensureSingleColumnAlias = async (sql: string, tableName: string) => {
  const [result] = await ensureColumnAliasBatch({
    items: [{ sql, tableName }],
    executeQuery: dummyGetQueryOutput,
  });

  if (!result) {
    throw new Error('Missing alias result');
  }

  return result.sql;
};

describe('ensureColumnAlias phase 1 coverage', () => {
  it('documents a broad supported scenario matrix', () => {
    expect(ENSURE_COLUMN_ALIAS_SCENARIOS.length).toBeGreaterThanOrEqual(20);
    expect(
      ENSURE_COLUMN_ALIAS_SCENARIOS.some((scenario) => scenario.shouldChange)
    ).toBe(true);
    expect(
      ENSURE_COLUMN_ALIAS_SCENARIOS.some((scenario) => !scenario.shouldChange)
    ).toBe(true);
  });

  it('tracks deferred edge cases explicitly', () => {
    expect(DEFERRED_ENSURE_COLUMN_ALIAS_SCENARIOS).toHaveLength(4);
    expect(
      DEFERRED_ENSURE_COLUMN_ALIAS_SCENARIOS.every(
        (scenario) => scenario.deferredReason.length > 0
      )
    ).toBe(true);
  });
});

describe('single-item batch aliasing implemented scenarios', () => {
  const implementedScenarioDescriptions = new Set([
    'bare dimension column',
    'bare aggregate argument',
    'bare aggregate argument for customers schema',
    'multiple bare columns in arithmetic expression',
    'mixed qualified and unqualified references',
    'already qualified column reference',
    'already qualified column reference for customers schema',
    'cast expression',
    'count distinct',
    'case expression with comparison and result column',
    'string literal with column-like content',
    'quoted identifier',
    'reference to a different table remains untouched',
  ]);

  const implementedScenarios = ENSURE_COLUMN_ALIAS_SCENARIOS.filter(
    (scenario) => implementedScenarioDescriptions.has(scenario.description)
  );

  for (const scenario of implementedScenarios) {
    it(`ensures alias for ${scenario.description}`, async () => {
      const result = await ensureSingleColumnAlias(
        scenario.inputSql,
        scenario.tableName
      );

      expect(result).toBe(scenario.expectedSql);
    });
  }
});

describe('single-item batch aliasing guardrails', () => {
  it('preserves the original SQL when no aliasing is needed', async () => {
    mockParseExpressions.mockResolvedValueOnce([
      {
        class: ExpressionClass.FUNCTION,
        type: ExpressionType.FUNCTION,
        alias: '',
        function_name: 'count_star',
        schema: '',
        children: [],
        filter: null,
        order_bys: {
          type: ResultModifierType.ORDER_MODIFIER,
          orders: [],
        },
        distinct: false,
        is_operator: false,
        export_state: false,
        catalog: '',
      } as ParsedExpression,
    ]);

    const result = await ensureSingleColumnAlias('COUNT(*)', 'orders');

    expect(result).toBe('COUNT(*)');
    expect(mockSerializeExpressions).not.toHaveBeenCalled();
  });

  it('does not alias lambda-bound identifiers', async () => {
    const result = await ensureSingleColumnAlias(
      "list_transform(priority_tags, x -> CASE WHEN x = 1 THEN 'P1' ELSE 'Unknown' END)",
      'issues'
    );

    expect(result).toBe(
      "LIST_TRANSFORM(issues.priority_tags, x -> CASE WHEN x = 1 THEN 'P1' ELSE 'Unknown' END)"
    );
  });
});

describe('ensureColumnAliasBatch', () => {
  it('matches single-expression aliasing outputs for representative expressions', async () => {
    const scenarios = ENSURE_COLUMN_ALIAS_SCENARIOS.slice(0, 6);

    const batchResults = await ensureColumnAliasBatch({
      items: scenarios.map((scenario) => ({
        sql: scenario.inputSql,
        tableName: scenario.tableName,
        context: scenario.description,
      })),
      executeQuery: dummyGetQueryOutput,
    });

    const singleResults = await Promise.all(
      scenarios.map((scenario) =>
        ensureSingleColumnAlias(scenario.inputSql, scenario.tableName)
      )
    );

    expect(batchResults.map((result) => result.sql)).toEqual(singleResults);
    expect(batchResults.map((result) => result.context)).toEqual(
      scenarios.map((scenario) => scenario.description)
    );
    expect(mockParseExpressions).toHaveBeenCalled();
  });

  it('uses the batched parse path for unchanged and changed expressions', async () => {
    const results = await ensureColumnAliasBatch({
      items: [
        {
          sql: 'customer_id',
          tableName: 'orders',
        },
        {
          sql: 'customers.id',
          tableName: 'orders',
        },
      ],
      executeQuery: dummyGetQueryOutput,
    });

    expect(results.map((result) => result.sql)).toEqual([
      'orders.customer_id',
      'customers.id',
    ]);
    expect(mockParseExpressions).toHaveBeenCalledTimes(1);
  });
});

describe('column refs with quotes and dots are not re-aliased', () => {
  it('does not alias a quoted identifier (column_names with spaces)', async () => {
    const result = await ensureSingleColumnAlias('"Order ID"', 'orders');
    expect(result).toBe('"Order ID"');
  });

  it('does not alias an already dot-qualified column ref from the same table', async () => {
    const result = await ensureSingleColumnAlias(
      'orders.order_amount',
      'orders'
    );
    expect(result).toBe('orders.order_amount');
  });

  it('does not alias an already dot-qualified column ref from a different table', async () => {
    const result = await ensureSingleColumnAlias('customers.id', 'orders');
    expect(result).toBe('customers.id');
  });
});

describe('schema-aware struct field aliasing', () => {
  const runWithContext = async ({
    sql,
    tableName,
    knownTableNames,
  }: {
    sql: string;
    tableName: string;
    knownTableNames?: string[];
  }) => {
    const [result] = await ensureColumnAliasBatch({
      items: [
        {
          sql,
          tableName,
          knownTableNames: knownTableNames
            ? new Set(knownTableNames)
            : undefined,
        },
      ],
      executeQuery: dummyGetQueryOutput,
    });
    if (!result) {
      throw new Error('Missing alias result');
    }
    return result.sql;
  };

  it('qualifies struct field access on a local column', async () => {
    const result = await runWithContext({
      sql: 'stage.stage_id',
      tableName: 'issue',
      knownTableNames: ['issue', 'devusers'],
    });
    expect(result).toBe('issue.stage.stage_id');
  });

  it('qualifies struct access inside an aggregate', async () => {
    const result = await runWithContext({
      sql: 'COUNT(stage.stage_id)',
      tableName: 'issue',
      knownTableNames: ['issue', 'devusers'],
    });
    expect(result).toBe('COUNT(issue.stage.stage_id)');
  });

  it('leaves cross-table references to a known table alias untouched', async () => {
    const result = await runWithContext({
      sql: 'customers.id',
      tableName: 'orders',
      knownTableNames: ['orders', 'customers'],
    });
    expect(result).toBe('customers.id');
  });

  it('does not double-qualify already-qualified struct access', async () => {
    const result = await runWithContext({
      sql: 'issue.stage.stage_id',
      tableName: 'issue',
      knownTableNames: ['issue'],
    });
    expect(result).toBe('issue.stage.stage_id');
  });

  it('falls back to legacy behavior when knownTableNames is omitted', async () => {
    const result = await runWithContext({
      sql: 'customer_id',
      tableName: 'orders',
    });
    expect(result).toBe('orders.customer_id');
  });
});

describe('schema-aware struct aliasing — extended coverage', () => {
  const run = async ({
    sql,
    tableName,
    knownTableNames,
  }: {
    sql: string;
    tableName: string;
    knownTableNames?: string[];
  }) => {
    const [result] = await ensureColumnAliasBatch({
      items: [
        {
          sql,
          tableName,
          knownTableNames: knownTableNames
            ? new Set(knownTableNames)
            : undefined,
        },
      ],
      executeQuery: dummyGetQueryOutput,
    });
    return result?.sql;
  };

  it('qualifies struct access mixed with bare columns in arithmetic', async () => {
    const result = await run({
      sql: 'stage.stage_id + amount',
      tableName: 'issue',
      knownTableNames: ['issue', 'devusers'],
    });
    expect(result).toBe('issue.stage.stage_id + issue.amount');
  });

  it('qualifies local struct but preserves cross-table ref in comparison', async () => {
    const result = await run({
      sql: 'stage.stage_id = devusers.id',
      tableName: 'issue',
      knownTableNames: ['issue', 'devusers'],
    });
    expect(result).toBe('issue.stage.stage_id = devusers.id');
  });

  it('qualifies struct access inside CASE branches', async () => {
    const result = await run({
      sql: 'CASE WHEN stage.stage_id = 1 THEN owner.id END',
      tableName: 'issue',
      knownTableNames: ['issue', 'devusers'],
    });
    expect(result).toBe(
      'CASE WHEN issue.stage.stage_id = 1 THEN issue.owner.id END'
    );
  });

  it('qualifies struct access inside SUM aggregate', async () => {
    const result = await run({
      sql: 'SUM(stage.stage_id)',
      tableName: 'issue',
      knownTableNames: ['issue', 'devusers'],
    });
    expect(result).toBe('SUM(issue.stage.stage_id)');
  });

  it('qualifies struct access inside lambda function argument but not lambda-bound identifier', async () => {
    const result = await run({
      sql: 'list_transform(stage.items, x -> x)',
      tableName: 'issue',
      knownTableNames: ['issue', 'devusers'],
    });
    expect(result).toBe('LIST_TRANSFORM(issue.stage.items, x -> x)');
  });

  it('treats ambiguous multi-part ref as cross-table when knownTableNames contains root', async () => {
    const result = await run({
      sql: 'customers.id',
      tableName: 'orders',
      knownTableNames: ['orders', 'customers'],
    });
    expect(result).toBe('customers.id');
  });

  it('qualifies multi-part ref with unknown root as struct when schema batch is present', async () => {
    const result = await run({
      sql: 'foo.bar',
      tableName: 'issue',
      knownTableNames: ['issue'],
    });
    expect(result).toBe('issue.foo.bar');
  });

  it('stays conservative on multi-part ref when knownTableNames is omitted', async () => {
    const result = await run({
      sql: 'foo.bar',
      tableName: 'issue',
    });
    expect(result).toBe('foo.bar');
  });

  it('does not double-qualify when root matches table even without knownTableNames', async () => {
    const result = await run({
      sql: 'orders.order_amount',
      tableName: 'orders',
    });
    expect(result).toBe('orders.order_amount');
  });
});

describe('ensureColumnAliasBatch — batched mixed tables', () => {
  it('applies per-item knownTableNames correctly within a single batch', async () => {
    const results = await ensureColumnAliasBatch({
      items: [
        {
          sql: 'stage.stage_id',
          tableName: 'issue',
          knownTableNames: new Set(['issue', 'devusers']),
        },
        {
          sql: 'customers.id',
          tableName: 'orders',
          knownTableNames: new Set(['orders', 'customers']),
        },
        {
          sql: 'customer_id',
          tableName: 'orders',
        },
      ],
      executeQuery: dummyGetQueryOutput,
    });

    expect(results.map((r) => r.sql)).toEqual([
      'issue.stage.stage_id',
      'customers.id',
      'orders.customer_id',
    ]);
    expect(results.map((r) => r.didChange)).toEqual([true, false, true]);
  });

  it('preserves context per-item through batched aliasing', async () => {
    const results = await ensureColumnAliasBatch({
      items: [
        {
          sql: 'customer_id',
          tableName: 'orders',
          context: { memberName: 'a' },
        },
        {
          sql: 'stage.stage_id',
          tableName: 'issue',
          knownTableNames: new Set(['issue']),
          context: { memberName: 'b' },
        },
      ],
      executeQuery: dummyGetQueryOutput,
    });

    expect(results.map((r) => r.context)).toEqual([
      { memberName: 'a' },
      { memberName: 'b' },
    ]);
  });
});

describe.skip('single-item batch aliasing pending scenarios', () => {
  for (const scenario of ENSURE_COLUMN_ALIAS_SCENARIOS) {
    if (expressionAstBySql[scenario.inputSql]) {
      continue;
    }

    it(`ensures alias for ${scenario.description}`, async () => {
      const result = await ensureSingleColumnAlias(
        scenario.inputSql,
        scenario.tableName
      );

      expect(result).toBe(scenario.expectedSql);
    });
  }

  for (const scenario of DEFERRED_ENSURE_COLUMN_ALIAS_SCENARIOS) {
    it(`handles deferred scenario: ${scenario.description}`, async () => {
      const result = await ensureSingleColumnAlias(
        scenario.inputSql,
        scenario.tableName
      );

      expect(result).toBe(scenario.expectedSql);
    });
  }
});
