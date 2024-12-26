import {
  ExpressionType,
  ParsedExpression,
  ResultModifierType,
} from '../../types/duckdb-serialization-types';
import { ExpressionClass } from '../../types/duckdb-serialization-types/serialization/Expression';

export const EMPTY_VALID_FUNCTIONS = new Set<string>();
export const VALID_FUNCTIONS = new Set(['contains', 'round', 'power']);

const VALID_SCALAR_FUNCTIONS = new Set(['+', '-', '*', '/', '||']);

export const COLUMN_REF_NODE: ParsedExpression = {
  class: ExpressionClass.COLUMN_REF,
  type: ExpressionType.COLUMN_REF,
  alias: 'alias',
  query_location: 0,
  column_names: ['column_name'],
};

export const INVALID_NODE: ParsedExpression = {
  class: ExpressionClass.INVALID,
  type: ExpressionType.INVALID,
  alias: '',
  query_location: 0,
};

export const DIMENSION_TEST_CASES: {
  description: string;
  node: ParsedExpression;
  validFunctions: Set<string>;
  expected: boolean;
}[] = [
  {
    description: 'node type COLUMN_REF',
    node: {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      query_location: 0,
      column_names: ['column_name'],
    },
    validFunctions: EMPTY_VALID_FUNCTIONS,
    expected: true,
  },
  {
    description: 'node type COLUMN_REF with alias',
    node: COLUMN_REF_NODE,
    validFunctions: EMPTY_VALID_FUNCTIONS,
    expected: true,
  },
  {
    description: 'node type VALUE_CONSTANT',
    node: {
      class: ExpressionClass.CONSTANT,
      type: ExpressionType.VALUE_CONSTANT,
      alias: '',
      query_location: 0,
      value: '1',
    },
    validFunctions: EMPTY_VALID_FUNCTIONS,
    expected: true,
  },
  {
    description: 'node type OPERATOR_CAST',
    node: {
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
    },
    validFunctions: EMPTY_VALID_FUNCTIONS,
    expected: true,
  },
  {
    description: 'node type OPERATOR_COALESCE',
    node: {
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
    },
    validFunctions: EMPTY_VALID_FUNCTIONS,
    expected: true,
  },
  {
    description:
      'node type FUNCTION with ROUND function and if it contains in validFunctions',
    node: {
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
    },
    validFunctions: VALID_FUNCTIONS,
    expected: true,
  },
  {
    description: 'node type CASE',
    node: {
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
    },
    validFunctions: EMPTY_VALID_FUNCTIONS,
    expected: true,
  },
];

export const MEASURE_TEST_CASES: {
  description: string;
  node: ParsedExpression;
  validFunctions: Set<string>;
  expected: boolean;
  validScalarFunctions: Set<string>;
}[] = [
  {
    description: 'node type FUNCTION with count_star',
    node: {
      class: ExpressionClass.FUNCTION,
      type: ExpressionType.FUNCTION,
      alias: '',
      query_location: 7,
      function_name: 'count_star',
      schema: '',
      children: [],
    },
    validFunctions: EMPTY_VALID_FUNCTIONS,
    expected: true,
  },
  {
    description: 'node type FUNCTION with SUM',
    node: {
      class: ExpressionClass.FUNCTION,
      type: ExpressionType.FUNCTION,
      alias: '',
      query_location: 7,
      function_name: 'sum',
      schema: '',
      children: [
        {
          class: ExpressionClass.COLUMN_REF,
          type: ExpressionType.COLUMN_REF,
          query_location: 11,
          column_names: ['column1'],
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
    },
    validFunctions: new Set(['sum']),
    expected: true,
  },
  {
    description: 'node type FUNCTION with MAX and operator',
    node: {
      class: ExpressionClass.FUNCTION,
      type: ExpressionType.FUNCTION,
      alias: '',
      query_location: 38,
      function_name: '/',
      schema: '',
      children: [
        {
          class: ExpressionClass.FUNCTION,
          type: ExpressionType.FUNCTION,
          alias: '',
          query_location: 7,
          function_name: 'max',
          schema: '',
          children: [
            {
              class: ExpressionClass.COLUMN_REF,
              type: ExpressionType.COLUMN_REF,
              alias: '',
              query_location: 11,
              column_names: ['p50_upstream_service_time'],
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
        },
        {
          class: ExpressionClass.CONSTANT,
          type: ExpressionType.VALUE_CONSTANT,
          alias: '',
          query_location: 40,
          value: {
            type: {
              id: 'INTEGER',
              type_info: null,
            },
            is_null: false,
            value: 1000,
          },
        },
      ],
      filter: null,
      order_bys: {
        type: ResultModifierType.ORDER_MODIFIER,
        orders: [],
      },
      distinct: false,
      is_operator: true,
      export_state: false,
      catalog: '',
    },
    validFunctions: new Set(['max']),
    validScalarFunctions: new Set(['/']),
    expected: true,
  },
];
