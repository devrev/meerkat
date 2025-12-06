import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { ConjunctionExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';

export const orDuckdbCondition = (): ConjunctionExpression => {
  return {
    class: ExpressionClass.CONJUNCTION,
    type: ExpressionType.CONJUNCTION_OR,
    alias: '',
    children: [],
  };
};
