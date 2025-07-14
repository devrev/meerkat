import { getAlias } from '../member-formatters';
import { Member } from '../types/cube-types/query';
import {
  ExpressionClass,
  ExpressionType,
} from '../types/duckdb-serialization-types/serialization/Expression';

export const cubeDimensionToGroupByAST = (
  dimensions: Member[],
  aliases?: Record<string, string>
) => {
  const groupByAST = dimensions.map((dimension) => {
    const dimensionAST = {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      column_names: [getAlias(dimension, aliases)],
    };

    return dimensionAST;
  });

  return groupByAST;
};
