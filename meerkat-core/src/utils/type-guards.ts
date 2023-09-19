import {
  LogicalAndFilter,
  LogicalOrFilter,
  QueryFilter,
} from '@devrev/cube-types';
import {
  ConjunctionExpression,
  ParsedExpression,
} from '@devrev/duckdb-serialization-types';

export function isLogicalAndOR(
  expression: QueryFilter | LogicalAndFilter | LogicalOrFilter
): expression is LogicalAndFilter | LogicalOrFilter {
  if (!expression) {
    return false;
  }
  if (
    Object.prototype.hasOwnProperty.call(expression, 'and') ||
    Object.prototype.hasOwnProperty.call(expression, 'or')
  ) {
    return true;
  }
  return false;
}

export function isLogicalAnd(
  expression: QueryFilter | LogicalAndFilter | LogicalOrFilter
): expression is LogicalAndFilter {
  if (!expression) {
    return false;
  }
  if (Object.prototype.hasOwnProperty.call(expression, 'and')) {
    return true;
  }
  return false;
}

export function isLogicalOr(
  expression: QueryFilter | LogicalAndFilter | LogicalOrFilter
): expression is LogicalOrFilter {
  if (!expression) {
    return false;
  }
  if (Object.prototype.hasOwnProperty.call(expression, 'or')) {
    return true;
  }
  return false;
}

export function isQueryFilter(
  expression: QueryFilter | LogicalAndFilter | LogicalOrFilter
): expression is QueryFilter {
  if (!expression) {
    return false;
  }
  if (Object.prototype.hasOwnProperty.call(expression, 'member')) {
    return true;
  }
  return false;
}

export function isFilterArray(
  expression:
    | (QueryFilter | LogicalAndFilter | LogicalOrFilter)
    | (QueryFilter | LogicalAndFilter | LogicalOrFilter)[]
): expression is (QueryFilter | LogicalAndFilter | LogicalOrFilter)[] {
  if (!expression) {
    return false;
  }
  if (Array.isArray(expression)) {
    return true;
  }
  return false;
}

export function hasChildren(
  whereObj: ParsedExpression | null
): whereObj is ConjunctionExpression {
  if (!whereObj) {
    return false;
  }
  if (Object.prototype.hasOwnProperty.call(whereObj, 'children')) {
    return true;
  }
  return false;
}
