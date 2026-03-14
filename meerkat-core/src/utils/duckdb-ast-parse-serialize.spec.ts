import { ParsedSerialization } from '../ast-validator';
import {
  AggregateHandling,
  ExpressionClass,
  ExpressionType,
  ParsedExpression,
  QueryNodeType,
  ResultModifierType,
  TableReferenceType,
} from '../types/duckdb-serialization-types';
import {
  parseExpressions,
  serializeExpressions,
} from './duckdb-ast-parse-serialize';

describe('parseExpressions', () => {
  it('wraps expressions in SELECT when parsing and returns the inner expression AST', async () => {
    const expression = {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      column_names: ['customer_id'],
    };
    const serialization: ParsedSerialization = {
      statements: [
        {
          node: {
            type: QueryNodeType.SELECT_NODE,
            modifiers: [],
            cte_map: { map: [] },
            select_list: [expression],
            from_table: {
              type: TableReferenceType.EMPTY,
              alias: '',
              sample: null,
            },
            group_expressions: [],
            group_sets: [],
            aggregate_handling: AggregateHandling.STANDARD_HANDLING,
            having: null,
            sample: null,
            qualify: null,
          },
        },
      ],
    };
    const executeQuery = jest.fn(async (query: string) => {
      expect(query).toContain("json_serialize_sql('SELECT customer_id')");
      return [{ json_serialize_sql: JSON.stringify(serialization) }];
    });

    const parsed = await parseExpressions(['customer_id'], executeQuery);

    expect(parsed).toHaveLength(1);
    expect(parsed[0]).toEqual(expression);
    expect(executeQuery).toHaveBeenCalledTimes(1);
  });

  it('parses multiple expressions from a single synthetic SELECT in order', async () => {
    const expressions = [
      {
        class: ExpressionClass.COLUMN_REF,
        type: ExpressionType.COLUMN_REF,
        alias: '',
        column_names: ['customer_id'],
      },
      {
        class: ExpressionClass.COLUMN_REF,
        type: ExpressionType.COLUMN_REF,
        alias: '',
        column_names: ['order_amount'],
      },
    ];
    const serialization: ParsedSerialization = {
      statements: [
        {
          node: {
            type: QueryNodeType.SELECT_NODE,
            modifiers: [],
            cte_map: { map: [] },
            select_list: expressions,
            from_table: {
              type: TableReferenceType.EMPTY,
              alias: '',
              sample: null,
            },
            group_expressions: [],
            group_sets: [],
            aggregate_handling: AggregateHandling.STANDARD_HANDLING,
            having: null,
            sample: null,
            qualify: null,
          },
        },
      ],
    };
    const executeQuery = jest.fn(async (query: string) => {
      expect(query).toContain(
        "json_serialize_sql('SELECT customer_id, order_amount')"
      );
      return [{ json_serialize_sql: JSON.stringify(serialization) }];
    });

    const parsed = await parseExpressions(
      ['customer_id', 'order_amount'],
      executeQuery
    );

    expect(parsed).toEqual(expressions);
    expect(executeQuery).toHaveBeenCalledTimes(1);
  });

  it('strips query_location metadata during parsing', async () => {
    const expressionWithQueryLocation = {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      column_names: ['customer_id'],
      query_location: 12,
    };
    const serialization: ParsedSerialization = {
      statements: [
        {
          node: {
            type: QueryNodeType.SELECT_NODE,
            modifiers: [],
            cte_map: { map: [] },
            select_list: [expressionWithQueryLocation],
            from_table: {
              type: TableReferenceType.EMPTY,
              alias: '',
              sample: null,
            },
            group_expressions: [],
            group_sets: [],
            aggregate_handling: AggregateHandling.STANDARD_HANDLING,
            having: null,
            sample: null,
            qualify: null,
          },
        },
      ],
    };
    const executeQuery = jest.fn(async () => {
      return [{ json_serialize_sql: JSON.stringify(serialization) }];
    });

    const parsed = await parseExpressions(['customer_id'], executeQuery);

    expect(parsed[0]).not.toHaveProperty('query_location');
  });
});

describe('serializeExpressions', () => {
  it('serializes multiple expressions from a single synthetic SELECT in order', async () => {
    const executeQuery = jest.fn(async (query: string) => {
      expect(query).toContain('json_deserialize_sql');
      expect(query).toContain('__meerkat_batch_expr_0__');
      expect(query).toContain('__meerkat_batch_expr_1__');

      return [
        {
          json_deserialize_sql:
            'SELECT orders.customer_id AS __meerkat_batch_expr_0__, SUM(orders.order_amount) AS __meerkat_batch_expr_1__;',
        },
      ];
    });

    const serializedExpressions = await serializeExpressions(
      [
        {
          class: ExpressionClass.COLUMN_REF,
          type: ExpressionType.COLUMN_REF,
          alias: '',
          column_names: ['orders', 'customer_id'],
        } as ParsedExpression,
        {
          class: ExpressionClass.FUNCTION,
          type: ExpressionType.FUNCTION,
          alias: '',
          function_name: 'sum',
          schema: '',
          children: [
            {
              class: ExpressionClass.COLUMN_REF,
              type: ExpressionType.COLUMN_REF,
              alias: '',
              column_names: ['orders', 'order_amount'],
            },
          ],
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
      ],
      executeQuery
    );

    expect(serializedExpressions).toEqual([
      'orders.customer_id',
      'SUM(orders.order_amount)',
    ]);
    expect(executeQuery).toHaveBeenCalledTimes(1);
  });

  it('throws when the deserializer response is not a SELECT wrapper', async () => {
    const executeQuery = async () => [
      { json_deserialize_sql: 'orders.customer_id' },
    ];

    await expect(
      serializeExpressions(
        [
          {
            class: ExpressionClass.COLUMN_REF,
            type: ExpressionType.COLUMN_REF,
            alias: '',
            column_names: ['orders', 'customer_id'],
          } as ParsedExpression,
        ],
        executeQuery
      )
    ).rejects.toThrow('Expected SELECT wrapper, received: orders.customer_id');
  });
});
