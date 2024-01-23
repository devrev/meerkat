import { QueryFilter } from '../types/cube-types/query';
import { Dimension, Measure } from '../types/cube-types/table';

export type QueryOperatorsWithInfo = QueryFilter & {
  memberInfo: Measure | Dimension;
};

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

export type QueryFiltersWithInfoSingular = QueryOperatorsWithInfo
| LogicalAndFilterWithInfo
| LogicalOrFilterWithInfo;

export type QueryFiltersWithInfo = QueryFiltersWithInfoSingular[];
