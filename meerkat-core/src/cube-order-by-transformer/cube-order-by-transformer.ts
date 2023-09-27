import {
  ExpressionClass,
  ExpressionType,
  OrderType,
  ResultModifierType,
} from '@devrev/duckdb-serialization-types';

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
        column_names: key.split('.'),
      },
    };
    orderArr.push(orderByAST);
  }
  return {
    type: ResultModifierType.ORDER_MODIFIER,
    orders: orderArr,
  };
};
