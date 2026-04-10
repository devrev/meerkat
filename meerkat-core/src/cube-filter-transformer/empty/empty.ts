import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { createColumnRef } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';

const emptyCondition = (
  columnName: string,
  options: { isAlias: boolean }
) => {
  const columnRef = createColumnRef(columnName, { isAlias: options.isAlias });

  return {
    class: ExpressionClass.CONJUNCTION,
    type: ExpressionType.CONJUNCTION_OR,
    alias: '',
    children: [
      {
        class: ExpressionClass.OPERATOR,
        type: ExpressionType.OPERATOR_IS_NULL,
        alias: '',
        children: [columnRef],
      },
      {
        class: ExpressionClass.COMPARISON,
        type: ExpressionType.COMPARE_EQUAL,
        alias: '',
        left: {
          class: ExpressionClass.FUNCTION,
          type: ExpressionType.FUNCTION,
          alias: '',
          function_name: 'len',
          schema: '',
          children: [columnRef],
          filter: null,
          order_bys: { type: 'ORDER_MODIFIER', orders: [] },
          distinct: false,
          is_operator: false,
          export_state: false,
          catalog: '',
        },
        right: {
          class: ExpressionClass.CONSTANT,
          type: ExpressionType.VALUE_CONSTANT,
          alias: '',
          value: {
            type: { id: 'INTEGER', type_info: null },
            is_null: false,
            value: 0,
          },
        },
      },
    ],
  };
};

const nullCondition = (
  columnName: string,
  options: { isAlias: boolean }
) => ({
  class: ExpressionClass.OPERATOR,
  type: ExpressionType.OPERATOR_IS_NULL,
  alias: '',
  children: [createColumnRef(columnName, { isAlias: options.isAlias })],
});

/**
 * For array columns: (col IS NULL OR len(col) = 0)
 * For primitive columns: (col IS NULL)
 */
export const emptyTransform: CubeToParseExpressionTransform = (
  query,
  options
) => {
  const { member, memberInfo } = query;

  if (memberInfo) {
    switch (memberInfo.type) {
      case 'string_array':
      case 'number_array':
        return emptyCondition(member, options);
      default:
        return nullCondition(member, options);
    }
  }

  return emptyCondition(member, options);
};
