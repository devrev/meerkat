import { QueryFilter } from '@devrev/cube-types';
import { containsTransform } from './contains/contains';
import { equalsTransform } from './equals/equals';
import { notContainsTransform } from './not-contains/not-contains';
import { notEqualsTransform } from './not-equals/not-equals';

export const getTransformerFunction = (query: QueryFilter) => {
  switch (query.operator) {
    case 'equals':
      return equalsTransform;
    case 'notEquals':
      return notEqualsTransform;
    case 'contains':
      return containsTransform;
    case 'notContains':
      return notContainsTransform;

    default:
      console.error('This operator is not supported yet', query.operator);
      throw new Error(`Unknown operator ${query.operator}`);
  }
};
