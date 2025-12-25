import { memberKeyToSafeKey } from '../member-formatters/member-key-to-safe-key';
import {
  ExpressionClass,
  ExpressionType,
} from '../types/duckdb-serialization-types/serialization/Expression';
import { OrderType } from '../types/duckdb-serialization-types/serialization/Nodes';
import { ResultModifierType } from '../types/duckdb-serialization-types/serialization/ResultModifier';

export const cubeOrderByToAST = (order: { [key: string]: 'asc' | 'desc' }) => {
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
         * Use safe keys internally instead of aliases
         */
        column_names: [memberKeyToSafeKey(key)],
      },
    };
    orderArr.push(orderByAST);
  }
  return {
    type: ResultModifierType.ORDER_MODIFIER,
    orders: orderArr,
  };
};
