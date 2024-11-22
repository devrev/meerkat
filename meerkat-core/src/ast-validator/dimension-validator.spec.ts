import {
  ExpressionType,
  ParsedExpression,
  ResultModifierType,
} from '../types/duckdb-serialization-types';
import { ExpressionClass } from '../types/duckdb-serialization-types/serialization/Expression';
import { validator } from './dimension-validator';

describe('dimension validator', () => {
  it('should return true for node type COLUMN_REF', () => {
    const COLUMN_REF_NODE: ParsedExpression = {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      query_location: 0,
      column_names: ['column_name'],
    };

    expect(validator(COLUMN_REF_NODE, [])).toBe(true);
  });

  it('should return true for node type VALUE_CONSTANT', () => {
    const VALUE_CONSTANT_NODE: ParsedExpression = {
      class: ExpressionClass.CONSTANT,
      type: ExpressionType.VALUE_CONSTANT,
      alias: '',
      query_location: 0,
      value: '1',
    };

    expect(validator(VALUE_CONSTANT_NODE, [])).toBe(true);
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

    expect(validator(OPERATOR_CAST_NODE, [])).toBe(true);
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

    expect(validator(OPERATOR_COALESCE_NODE, [])).toBe(true);
  });

  it('should return true for node type FUNCTION with ROUND function', () => {
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

    expect(validator(CASE_EXPR_NODE, [])).toBe(true);
  });
});
