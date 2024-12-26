import {
  ExpressionClass,
  ExpressionType,
  ParsedExpression,
  ResultModifierType,
} from '../../types/duckdb-serialization-types';
import { validateExpressionNode } from '../dimension-validator';
import {
  DIMENSION_TEST_CASES,
  EMPTY_VALID_FUNCTIONS,
  INVALID_NODE,
} from './test-data';

describe('validateExpressionNode for dimension expressions', () => {
  for (const data of DIMENSION_TEST_CASES) {
    it(`should return true for dimension expression: ${data.description}`, () => {
      expect(validateExpressionNode(data.node, data.validFunctions)).toBe(
        data.expected
      );
    });
  }

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

  it('should throw error for node type INVALID', () => {
    expect(() =>
      validateExpressionNode(INVALID_NODE, EMPTY_VALID_FUNCTIONS)
    ).toThrowError('Invalid expression type');
  });
});
