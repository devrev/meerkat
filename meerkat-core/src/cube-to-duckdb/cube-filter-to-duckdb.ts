import { Query } from '@devrev/cube-types';
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
