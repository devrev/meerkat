import {
  QueryFilterWithInfo,
  QueryFiltersWithInfo,
  QueryOperatorsWithInfo,
} from '../cube-to-duckdb/cube-filter-to-duckdb';
import { ParsedExpression } from '../types/duckdb-serialization-types/serialization/ParsedExpression';
import { SelectNode } from '../types/duckdb-serialization-types/serialization/QueryNode';
import { SelectStatement } from '../types/duckdb-serialization-types/serialization/Statement';
import {
  hasChildren,
  isFilterArray,
  isLogicalAnd,
  isLogicalAndOR,
  isLogicalOr,
  isQueryFilter,
} from '../utils/type-guards';
import { andDuckdbCondition } from './and/and';
import { CreateColumnRefOptions } from './base-condition-builder/base-condition-builder';
import { containsTransform } from './contains/contains';
import { equalsTransform } from './equals/equals';
import { gtTransform } from './gt/gt';
import { gteTransform } from './gte/gte';
import { inDataRangeTransform } from './in-date-range/in-date-range';
import { inTransform } from './in/in';
import { ltTransform } from './lt/lt';
import { lteTransform } from './lte/lte';
import { notInDataRangeTransform } from './not-In-date-range/not-In-date-range';
import { notContainsTransform } from './not-contains/not-contains';
import { notEqualsTransform } from './not-equals/not-equals';
import { notInTransform } from './not-in/not-in';
import { notSetTransform } from './not-set/not-set';
import { orDuckdbCondition } from './or/or';
import { setTransform } from './set/set';

export type CubeToParseExpressionTransform = (
  query: QueryOperatorsWithInfo,
  options: CreateColumnRefOptions
) => ParsedExpression;

// Comparison operators
const cubeFilterOperatorsToDuckdb = (
  cubeFilter: QueryOperatorsWithInfo,
  options: CreateColumnRefOptions
) => {
  switch (cubeFilter.operator) {
    case 'equals':
      return equalsTransform(cubeFilter, options);
    case 'notEquals':
      return notEqualsTransform(cubeFilter, options);
    case 'in':
      return inTransform(cubeFilter, options);
    case 'notIn':
      return notInTransform(cubeFilter, options);
    case 'contains':
      return containsTransform(cubeFilter, options);
    case 'notContains':
      return notContainsTransform(cubeFilter, options);
    case 'gt':
      return gtTransform(cubeFilter, options);
    case 'gte':
      return gteTransform(cubeFilter, options);
    case 'lt':
      return ltTransform(cubeFilter, options);
    case 'lte':
      return lteTransform(cubeFilter, options);
    case 'inDateRange':
      return inDataRangeTransform(cubeFilter, options);
    case 'notInDateRange':
      return notInDataRangeTransform(cubeFilter, options);
    case 'notSet': {
      return notSetTransform(cubeFilter, options);
    }
    case 'set': {
      return setTransform(cubeFilter, options);
    }
    default:
      throw new Error('Could not transform the filter');
  }
};

const cubeFilterLogicalAndOrToDuckdb = (
  cubeFilter: QueryFilterWithInfo,
  whereObj: ParsedExpression | null,
  options: CreateColumnRefOptions
): ParsedExpression | null => {
  /**
   * This condition is true when you are at the leaf most level of the filter
   */
  if (!isFilterArray(cubeFilter) && isQueryFilter(cubeFilter)) {
    const data = cubeFilterOperatorsToDuckdb(cubeFilter, options);
    if (!data) {
      throw new Error('Could not transform the filter');
    }
    return data;
  }

  if (!isFilterArray(cubeFilter) && isLogicalAnd(cubeFilter)) {
    if (cubeFilter.and.length === 0) {
      return null;
    }
    // And or Or we need to recurse
    const andDuckdbExpression = andDuckdbCondition();
    const data = cubeFilterLogicalAndOrToDuckdb(
      cubeFilter.and,
      andDuckdbExpression,
      options
    );
    return data;
  }

  if (!isFilterArray(cubeFilter) && isLogicalOr(cubeFilter)) {
    if (cubeFilter.or.length === 0) {
      return null;
    }
    // And or Or we need to recurse
    const orDuckdbExpression = orDuckdbCondition();
    const data = cubeFilterLogicalAndOrToDuckdb(
      cubeFilter.or,
      orDuckdbExpression,
      options
    );
    return data;
  }

  if (isFilterArray(cubeFilter)) {
    for (const filter of cubeFilter) {
      const data = cubeFilterLogicalAndOrToDuckdb(filter, whereObj, options);
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
  ast: SelectStatement,
  options: CreateColumnRefOptions
) => {
  let whereObj: ParsedExpression | null | undefined =
    (ast.node as SelectNode).where_clause || null;

  if (cubeFilter.length > 1) {
    console.error('We do not support multiple filters yet');
    throw new Error('We do not support multiple filters yet');
  }

  const filter = cubeFilter[0];

  if (isQueryFilter(filter)) {
    whereObj = cubeFilterOperatorsToDuckdb(filter, options);
  }

  if (isLogicalAndOR(filter)) {
    // And or Or we need to recurse
    whereObj = cubeFilterLogicalAndOrToDuckdb(
      filter,
      whereObj || null,
      options
    );
  }

  return whereObj;
};
