import { memberKeyToSafeKey } from '../member-formatters';
import { Member } from '../types/cube-types/query';
import {
  ExpressionClass,
  ExpressionType,
} from '../types/duckdb-serialization-types/serialization/Expression';

export const cubeDimensionToGroupByAST = (dimensions: Member[]) => {
  const groupByAST = dimensions.map((dimension) => {
    const dimensionAST = {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      column_names: [memberKeyToSafeKey(dimension)],
    };

    return dimensionAST;
  });

  return groupByAST;
};
