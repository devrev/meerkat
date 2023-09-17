import {
  ExpressionClass,
  ExpressionType,
} from '@devrev/duckdb-serialization-types';

export const baseDuckdbCondition = (
  columnName: string,
  type: ExpressionType,
  value: string
) => {
  return {
    class: ExpressionClass.COMPARISON,
    type: type,
    alias: '',
    left: {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      column_names: [columnName],
    },
    right: {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      column_names: [value],
    },
  };
};
