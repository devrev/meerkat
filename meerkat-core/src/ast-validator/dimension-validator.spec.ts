import {
  ExpressionType,
  ParsedExpression,
  QueryNodeType,
  ResultModifierType,
  TableReferenceType,
} from '../types/duckdb-serialization-types';
import { ExpressionClass } from '../types/duckdb-serialization-types/serialization/Expression';
import { AggregateHandling } from '../types/duckdb-serialization-types/serialization/QueryNode';
import {
  validateDimension,
  validateExpressionNode,
} from './dimension-validator';
import { ParsedSerialization } from './types';

const EMPTY_VALID_FUNCTIONS = new Set<string>();
const VALID_FUNCTIONS = new Set(['contains', 'round', 'power']);

const COLUMN_REF_NODE: ParsedExpression = {
  class: ExpressionClass.COLUMN_REF,
  type: ExpressionType.COLUMN_REF,
  alias: 'alias',
  query_location: 0,
  column_names: ['column_name'],
};

const INVALID_NODE: ParsedExpression = {
  class: ExpressionClass.INVALID,
  type: ExpressionType.INVALID,
  alias: '',
  query_location: 0,
};

const PARSED_SERIALIZATION: ParsedSerialization = {
  error: false,
  statements: [
    {
      node: {
        type: QueryNodeType.SELECT_NODE,
        modifiers: [],
        cte_map: {
          map: [],
        },
        select_list: [COLUMN_REF_NODE],
        from_table: {
          type: TableReferenceType.BASE_TABLE,
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

describe('validateDimension', () => {
  it('should throw error if the statement if there is no statement', () => {
    expect(() =>
      validateDimension(
        {
          error: false,
          statements: [],
        },
        []
      )
    ).toThrow('No statement found');
  });

  it('should throw error if no statement is found', () => {
    expect(() =>
      validateDimension(
        {
          error: false,
          statements: [
            {
              node: {
                type: QueryNodeType.CTE_NODE,
                modifiers: [],
                cte_map: {
                  map: [],
                },
              },
            },
          ],
        },
        []
      )
    ).toThrow('Statement must be a SELECT node');
  });

  it('should throw error if select list is not exactly one expression', () => {
    expect(() =>
      validateDimension(
        {
          error: false,
          statements: [
            {
              node: {
                type: QueryNodeType.SELECT_NODE,
                modifiers: [],
                cte_map: {
                  map: [],
                },
                select_list: [],
              },
            },
          ],
        },
        []
      )
    ).toThrow('SELECT must contain exactly one expression');
  });

  it('should return true if the statement is valid', () => {
    expect(validateDimension(PARSED_SERIALIZATION, [])).toBe(true);
  });

  it('should throw error if the expression is invalid', () => {
    expect(() =>
      validateDimension(
        {
          ...PARSED_SERIALIZATION,
          statements: [
            {
              ...PARSED_SERIALIZATION.statements[0],
              node: {
                ...PARSED_SERIALIZATION.statements[0].node,
                select_list: [INVALID_NODE],
              },
            },
          ],
        },
        ['contains']
      )
    ).toThrow('Invalid expression type: INVALID');
  });
});

describe('validateExpressionNode for dimension expressions', () => {
  it('should return true for node type COLUMN_REF', () => {
    const COLUMN_REF_NODE: ParsedExpression = {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      query_location: 0,
      column_names: ['column_name'],
    };

    expect(validateExpressionNode(COLUMN_REF_NODE, EMPTY_VALID_FUNCTIONS)).toBe(
      true
    );
  });

  it('should return true for node type COLUMN_REF with alias', () => {
    expect(validateExpressionNode(COLUMN_REF_NODE, EMPTY_VALID_FUNCTIONS)).toBe(
      true
    );
  });

  it('should return true for node type VALUE_CONSTANT', () => {
    const VALUE_CONSTANT_NODE: ParsedExpression = {
      class: ExpressionClass.CONSTANT,
      type: ExpressionType.VALUE_CONSTANT,
      alias: '',
      query_location: 0,
      value: '1',
    };

    expect(
      validateExpressionNode(VALUE_CONSTANT_NODE, EMPTY_VALID_FUNCTIONS)
    ).toBe(true);
  });

  it('should return true for node type OPERATOR_CAST', () => {
    const OPERATOR_CAST_NODE: ParsedExpression = {
      class: ExpressionClass.CAST,
      type: ExpressionType.OPERATOR_CAST,
      alias: '',
      query_location: 7,
      child: {
        class: ExpressionClass.COLUMN_REF,
        type: ExpressionType.COLUMN_REF,
        alias: '',
        query_location: 12,
        column_names: ['column_name'],
      },
      cast_type: {
        id: 1,
      },
      try_cast: false,
    };

    expect(
      validateExpressionNode(OPERATOR_CAST_NODE, EMPTY_VALID_FUNCTIONS)
    ).toBe(true);
  });

  it('should return true for node type OPERATOR_COALESCE', () => {
    const OPERATOR_COALESCE_NODE: ParsedExpression = {
      class: ExpressionClass.OPERATOR,
      type: ExpressionType.OPERATOR_COALESCE,
      alias: '',
      query_location: 18446744073709552000,
      children: [
        {
          class: ExpressionClass.COLUMN_REF,
          type: ExpressionType.COLUMN_REF,
          alias: '',
          query_location: 16,
          column_names: ['column_name'],
        },
        {
          class: ExpressionClass.CONSTANT,
          type: ExpressionType.VALUE_CONSTANT,
          alias: '',
          query_location: 38,
          value: {
            type: {
              id: 'INTEGER',
              type_info: null,
            },
            is_null: false,
            value: 0,
          },
        },
      ],
    };

    expect(
      validateExpressionNode(OPERATOR_COALESCE_NODE, EMPTY_VALID_FUNCTIONS)
    ).toBe(true);
  });

  it('should return true for node type FUNCTION with ROUND function and if it contains in validFunctions', () => {
    const CASE_EXPR_NODE: ParsedExpression = {
      class: ExpressionClass.FUNCTION,
      type: ExpressionType.FUNCTION,
      alias: '',
      query_location: 7,
      function_name: 'round',
      schema: '',
      children: [
        {
          class: ExpressionClass.COLUMN_REF,
          type: ExpressionType.COLUMN_REF,
          alias: '',
          query_location: 13,
          column_names: ['column_name'],
        },
        {
          class: ExpressionClass.CONSTANT,
          type: ExpressionType.VALUE_CONSTANT,
          alias: '',
          query_location: 41,
          value: {
            type: {
              id: 'INTEGER',
              type_info: null,
            },
            is_null: false,
            value: 1,
          },
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
    };

    expect(validateExpressionNode(CASE_EXPR_NODE, VALID_FUNCTIONS)).toBe(true);
  });

  it('should throw error for node type FUNCTION with ROUND function and if it not contains in validFunctions', () => {
    const CASE_EXPR_NODE: ParsedExpression = {
      class: ExpressionClass.FUNCTION,
      type: ExpressionType.FUNCTION,
      alias: '',
      query_location: 7,
      function_name: 'round',
      schema: '',
      children: [
        {
          class: ExpressionClass.COLUMN_REF,
          type: ExpressionType.COLUMN_REF,
          alias: '',
          query_location: 13,
          column_names: ['column_name'],
        },
        {
          class: ExpressionClass.CONSTANT,
          type: ExpressionType.VALUE_CONSTANT,
          alias: '',
          query_location: 41,
          value: {
            type: {
              id: 'INTEGER',
              type_info: null,
            },
            is_null: false,
            value: 1,
          },
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
    };

    expect(() =>
      validateExpressionNode(CASE_EXPR_NODE, new Set(['contains']))
    ).toThrowError('Invalid function: round');
  });

  it('should return true for node type CASE', () => {
    const CASE_EXPR_NODE: ParsedExpression = {
      class: ExpressionClass.CASE,
      type: ExpressionType.CASE_EXPR,
      alias: '',
      query_location: 7,
      case_checks: [
        {
          when_expr: {
            class: ExpressionClass.COMPARISON,
            type: ExpressionType.COMPARE_GREATERTHAN,
            alias: '',
            query_location: 35,
            left: {
              class: ExpressionClass.COLUMN_REF,
              type: ExpressionType.COLUMN_REF,
              alias: '',
              query_location: 17,
              column_names: ['actual_close_date'],
            },
            right: {
              class: ExpressionClass.COLUMN_REF,
              type: ExpressionType.COLUMN_REF,
              alias: '',
              query_location: 37,
              column_names: ['created_date'],
            },
          },
          then_expr: {
            class: ExpressionClass.COLUMN_REF,
            type: ExpressionType.COLUMN_REF,
            alias: '',
            query_location: 55,
            column_names: ['actual_close_date'],
          },
        },
      ],
      else_expr: {
        class: ExpressionClass.CONSTANT,
        type: ExpressionType.VALUE_CONSTANT,
        alias: '',
        query_location: 18446744073709552000,
        value: {
          type: {
            id: 'NULL',
            type_info: null,
          },
          is_null: true,
        },
      },
    };

    expect(validateExpressionNode(CASE_EXPR_NODE, EMPTY_VALID_FUNCTIONS)).toBe(
      true
    );
  });

  it('should throw error for node type INVALID', () => {
    expect(() =>
      validateExpressionNode(INVALID_NODE, EMPTY_VALID_FUNCTIONS)
    ).toThrowError('Invalid expression type');
  });
});
