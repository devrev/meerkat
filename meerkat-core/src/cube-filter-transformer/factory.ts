import {
  ParsedExpression,
  SelectNode,
  SelectStatement,
} from '@devrev/duckdb-serialization-types';
import {
  QueryFilterWithInfo,
  QueryFiltersWithInfo,
  QueryOperatorsWithInfo,
} from '../cube-to-duckdb/cube-filter-to-duckdb';
import {
  hasChildren,
  isFilterArray,
  isLogicalAnd,
  isLogicalAndOR,
  isLogicalOr,
  isQueryFilter,
} from '../utils/type-guards';
import { andDuckdbCondition } from './and/and';
import { containsTransform } from './contains/contains';
import { equalsTransform } from './equals/equals';
import { gtTransform } from './gt/gt';
import { gteTransform } from './gte/gte';
import { inDataRangeTransform } from './in-date-range/in-date-range';
import { ltTransform } from './lt/lt';
import { lteTransform } from './lte/lte';
import { notInDataRangeTransform } from './not-In-date-range/not-In-date-range';
import { notContainsTransform } from './not-contains/not-contains';
import { notEqualsTransform } from './not-equals/not-equals';
import { orDuckdbCondition } from './or/or';

export type CubeToParseExpressionTransform = (
  query: QueryOperatorsWithInfo
) => ParsedExpression;

// Comparison operators
const cubeFilterOperatorsToDuckdb = (cubeFilter: QueryOperatorsWithInfo) => {
  switch (cubeFilter.operator) {
    case 'equals':
      return equalsTransform(cubeFilter);
    case 'notEquals':
      return notEqualsTransform(cubeFilter);
    case 'contains':
      return containsTransform(cubeFilter);
    case 'notContains':
      return notContainsTransform(cubeFilter);
    case 'gt':
      return gtTransform(cubeFilter);
    case 'gte':
      return gteTransform(cubeFilter);
    case 'lt':
      return ltTransform(cubeFilter);
    case 'lte':
      return lteTransform(cubeFilter);
    case 'inDateRange':
      return inDataRangeTransform(cubeFilter);
    case 'notInDateRange':
      return notInDataRangeTransform(cubeFilter);
  }
};

const cubeFilterLogicalAndOrToDuckdb = (
  cubeFilter: QueryFilterWithInfo,
  whereObj: ParsedExpression | null
): ParsedExpression | null => {
  /**
   * This condition is true when you are at the leaf most level of the filter
   */
  if (!isFilterArray(cubeFilter) && isQueryFilter(cubeFilter)) {
    const data = cubeFilterOperatorsToDuckdb(cubeFilter);
    if (!data) {
      throw new Error('Could not transform the filter');
    }
    return data;
  }

  if (!isFilterArray(cubeFilter) && isLogicalAnd(cubeFilter)) {
    // And or Or we need to recurse
    const andDuckdbExpression = andDuckdbCondition();
    const data = cubeFilterLogicalAndOrToDuckdb(
      cubeFilter.and,
      andDuckdbExpression
    );
    return data;
  }

  if (!isFilterArray(cubeFilter) && isLogicalOr(cubeFilter)) {
    // And or Or we need to recurse
    const orDuckdbExpression = orDuckdbCondition();
    const data = cubeFilterLogicalAndOrToDuckdb(
      cubeFilter.or,
      orDuckdbExpression
    );
    return data;
  }

  if (isFilterArray(cubeFilter)) {
    for (const filter of cubeFilter) {
      const data = cubeFilterLogicalAndOrToDuckdb(filter, whereObj);
      if (data) {
        if (hasChildren(whereObj)) {
          whereObj.children.push(data);
        } else {
          whereObj = data;
        }
      }
    }
    return whereObj;
  }
  return whereObj;
};

export const cubeFilterToDuckdbAST = (
  cubeFilter: QueryFiltersWithInfo,
  ast: SelectStatement
) => {
  let whereObj: ParsedExpression | null | undefined =
    (ast.node as SelectNode).where_clause || null;

  if (cubeFilter.length > 1) {
    console.error('We do not support multiple filters yet');
    throw new Error('We do not support multiple filters yet');
  }

  const filter = cubeFilter[0];

  if (isQueryFilter(filter)) {
    whereObj = cubeFilterOperatorsToDuckdb(filter);
  }

  if (isLogicalAndOR(filter)) {
    // And or Or we need to recurse
    whereObj = cubeFilterLogicalAndOrToDuckdb(filter, whereObj || null);
  }

  return whereObj;
};
