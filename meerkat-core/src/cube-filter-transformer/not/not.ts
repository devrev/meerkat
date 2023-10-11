import {
  ConjunctionExpression,
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/index';

export const notDuckdbCondition = (): ConjunctionExpression => {
  return {
    class: ExpressionClass.OPERATOR,
    type: ExpressionType.OPERATOR_NOT,
    alias: '',
    children: [],
  };
};
