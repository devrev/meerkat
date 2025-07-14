import { getAlias } from '../member-formatters';
import {
  ExpressionClass,
  ExpressionType,
} from '../types/duckdb-serialization-types/serialization/Expression';
import { OrderType } from '../types/duckdb-serialization-types/serialization/Nodes';
import { ResultModifierType } from '../types/duckdb-serialization-types/serialization/ResultModifier';

export const cubeOrderByToAST = (
  order: { [key: string]: 'asc' | 'desc' },
  aliases?: Record<string, string>
) => {
  const orderArr = [];
  for (const key in order) {
    const value = order[key];
    const astOrderBy =
      value === 'asc' ? OrderType.ASCENDING : OrderType.DESCENDING;
    const orderByAST = {
      type: astOrderBy,
      null_order: OrderType.ORDER_DEFAULT,
      expression: {
        class: ExpressionClass.COLUMN_REF,
        type: ExpressionType.COLUMN_REF,
        alias: '',
        /**
         * We need to convert the key in the __ format as they are being projected in this format
         */
        column_names: [getAlias(key, aliases)],
      },
    };
    orderArr.push(orderByAST);
  }
  return {
    type: ResultModifierType.ORDER_MODIFIER,
    orders: orderArr,
  };
};
