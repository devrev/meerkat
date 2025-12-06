import {
  QueryFilterWithSQL,
  QueryFilterWithValues,
} from '../types/cube-types/query';
import { Dimension, Measure } from '../types/cube-types/table';

export type QueryOperatorsWithInfoSQL = QueryFilterWithSQL & {
  memberInfo: Measure | Dimension;
};

/**
 * Query filter with member info added
 */
export type QueryOperatorsWithInfoValues = QueryFilterWithValues & {
  memberInfo: Measure | Dimension;
};

export type QueryOperatorsWithInfo =
  | QueryOperatorsWithInfoSQL
  | QueryOperatorsWithInfoValues;

export type LogicalAndFilterWithInfo = {
  and: (
    | QueryOperatorsWithInfo
    | { or: (QueryOperatorsWithInfo | LogicalAndFilterWithInfo)[] }
  )[];
};

export type LogicalOrFilterWithInfo = {
  or: (QueryOperatorsWithInfo | LogicalAndFilterWithInfo)[];
};

export type QueryFilterWithInfo =
  | (
      | QueryOperatorsWithInfo
      | LogicalAndFilterWithInfo
      | LogicalOrFilterWithInfo
    )
  | (
      | QueryOperatorsWithInfo
      | LogicalAndFilterWithInfo
      | LogicalOrFilterWithInfo
    )[];

export type QueryFiltersWithInfoSingular =
  | QueryOperatorsWithInfo
  | LogicalAndFilterWithInfo
  | LogicalOrFilterWithInfo;

export type QueryFiltersWithInfo = QueryFiltersWithInfoSingular[];

/**
 * Type guard to check if filter uses SQL expression
 */
export const isQueryOperatorsWithSQLInfo = (
  filter: QueryOperatorsWithInfo
): filter is QueryOperatorsWithInfoSQL => {
  return 'sqlExpression' in filter && typeof filter.sqlExpression === 'string';
};
