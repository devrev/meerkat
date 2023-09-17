import {
  ConjunctionExpression,
  ParsedExpression,
} from '@devrev/duckdb-serialization-types';
import {
  BoundConjunctionExpression,
  ExpressionClass,
  ExpressionType,
} from 'duckdb-serialization-types/src/serialization/Expression';

export const orDuckdbCondition = (): ConjunctionExpression => {
  return {
    class: ExpressionClass.CONJUNCTION,
    type: ExpressionType.CONJUNCTION_OR,
    alias: '',
    children: [],
  };
};
