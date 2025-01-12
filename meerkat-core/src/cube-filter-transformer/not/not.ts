import {
  ExpressionClass,
  ExpressionType,
  OperatorExpression,
} from '../../types/duckdb-serialization-types/index';

export const notDuckdbCondition = (): OperatorExpression => {
  return {
    class: ExpressionClass.OPERATOR,
    type: ExpressionType.OPERATOR_NOT,
    alias: '',
    children: [],
  };
};
