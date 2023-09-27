import { Member } from '@devrev/cube-types';
import {
  ExpressionClass,
  ExpressionType,
} from '@devrev/duckdb-serialization-types';

export const cubeDimensionToGroupByAST = (dimensions: Member[]) => {
  const groupByAST = dimensions.map((dimension) => {
    const dimensionAST = {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      column_names: dimension.split('.'),
    };

    return dimensionAST;
  });

  return groupByAST;
};
