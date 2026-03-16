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
