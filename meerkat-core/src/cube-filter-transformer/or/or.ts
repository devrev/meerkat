import { ConjunctionExpression } from '@devrev/duckdb-serialization-types';
import {
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
