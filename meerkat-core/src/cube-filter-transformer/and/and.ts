import {
  ConjunctionExpression,
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/index';

export const andDuckdbCondition = (): ConjunctionExpression => {
  return {
    class: ExpressionClass.CONJUNCTION,
    type: ExpressionType.CONJUNCTION_AND,
    alias: '',
    children: [],
  };
};
