import { Dimension, Measure } from '../types/cube-types/table';
import { QueryFilter } from '../types/cube-types/query';

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

export type QueryFiltersWithInfo = (
  | QueryOperatorsWithInfo
  | LogicalAndFilterWithInfo
  | LogicalOrFilterWithInfo
)[];
