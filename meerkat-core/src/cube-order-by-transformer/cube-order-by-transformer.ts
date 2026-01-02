import {
  QueryOptions,
  getAliasForAST,
} from '../member-formatters/get-alias';
import { TableSchema } from '../types/cube-types';
import {
  ExpressionClass,
  ExpressionType,
} from '../types/duckdb-serialization-types/serialization/Expression';
import { OrderType } from '../types/duckdb-serialization-types/serialization/Nodes';
import { ResultModifierType } from '../types/duckdb-serialization-types/serialization/ResultModifier';

export const cubeOrderByToAST = (
  order: { [key: string]: 'asc' | 'desc' },
  tableSchema: TableSchema,
  config: QueryOptions
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
         * Column name reference - uses alias format matching projections
         */
        column_names: [getAliasForAST(key, tableSchema, config)],
      },
    };
    orderArr.push(orderByAST);
  }
  return {
    type: ResultModifierType.ORDER_MODIFIER,
    orders: orderArr,
  };
};
