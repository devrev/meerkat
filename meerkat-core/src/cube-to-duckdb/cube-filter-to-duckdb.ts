import { Dimension, Measure, Query, QueryFilter } from '@devrev/cube-types';
import { getTransformerFunction } from '../cube-filter-transformer/factory';

export const cubeFilterToDuckdb = (cube: Pick<Query, 'filters'>) => {
  const cubeFilter = cube.filters?.[0] as any;

  if (!cubeFilter) {
    return null;
  }

  const transformer = getTransformerFunction(cubeFilter);

  if (!transformer) {
    return null;
  }

  return transformer(cubeFilter);
};

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

export const cubeFilterToDuckdbAST = (cubeFilter: QueryFiltersWithInfo) => {
  if (!cubeFilter) {
    return undefined;
  }

  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  //@ts-ignore
  const transformer = getTransformerFunction(cubeFilter);

  if (!transformer) {
    return undefined;
  }

  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  //@ts-ignore
  return transformer(cubeFilter);
};
