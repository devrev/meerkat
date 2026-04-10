import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { createColumnRef } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';

const arrayNotEmptyCondition = (
  columnName: string,
  options: { isAlias: boolean }
) => {
  const columnRef = createColumnRef(columnName, { isAlias: options.isAlias });

  return {
    class: ExpressionClass.CONJUNCTION,
    type: ExpressionType.CONJUNCTION_AND,
    alias: '',
    children: [
      {
        class: ExpressionClass.OPERATOR,
        type: ExpressionType.OPERATOR_IS_NOT_NULL,
        alias: '',
        children: [columnRef],
      },
      {
        class: ExpressionClass.COMPARISON,
        type: ExpressionType.COMPARE_GREATERTHAN,
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

const notNullCondition = (
  columnName: string,
  options: { isAlias: boolean }
) => ({
  class: ExpressionClass.OPERATOR,
  type: ExpressionType.OPERATOR_IS_NOT_NULL,
  alias: '',
  children: [createColumnRef(columnName, { isAlias: options.isAlias })],
});

/**
 * For array columns: (col IS NOT NULL AND len(col) > 0)
 * For non-array columns: (col IS NOT NULL)
 */
export const arrayNotEmptyTransform: CubeToParseExpressionTransform = (
  query,
  options
) => {
  const { member, memberInfo } = query;

  if (memberInfo) {
    switch (memberInfo.type) {
      case 'string_array':
      case 'number_array':
        return arrayNotEmptyCondition(member, options);
      default:
        return notNullCondition(member, options);
    }
  }

  return arrayNotEmptyCondition(member, options);
};
