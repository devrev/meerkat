import { getAliasForAST } from '../member-formatters/get-alias';
import { TableSchema } from '../types/cube-types';
import { Member } from '../types/cube-types/query';
import {
  ExpressionClass,
  ExpressionType,
} from '../types/duckdb-serialization-types/serialization/Expression';

export const cubeDimensionToGroupByAST = (
  dimensions: Member[],
  tableSchema: TableSchema
) => {
  const groupByAST = dimensions.map((dimension) => {
    const dimensionAST = {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      column_names: [getAliasForAST(dimension, tableSchema)],
    };

    return dimensionAST;
  });

  return groupByAST;
};
