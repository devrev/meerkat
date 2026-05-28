import {
  ComparisonExpression,
  ConjunctionExpression,
  ExpressionClass,
  ExpressionType,
  ParsedExpression,
} from '../../../types/duckdb-serialization-types';
import { QueryFilterWithValues } from '../../../types/cube-types/query';
import { Measure } from '../../../types/cube-types/table';
import { getConstantValue } from '../../../utils/ast-constants';
import { matchMeasureFromExpr } from '../ast-utils';

const COMPARISON_OPERATOR_MAP: Record<string, QueryFilterWithValues['operator']> = {
  [ExpressionType.COMPARE_EQUAL]: 'equals',
  [ExpressionType.COMPARE_NOTEQUAL]: 'notEquals',
  [ExpressionType.COMPARE_GREATERTHAN]: 'gt',
  [ExpressionType.COMPARE_GREATERTHANOREQUALTO]: 'gte',
  [ExpressionType.COMPARE_LESSTHAN]: 'lt',
  [ExpressionType.COMPARE_LESSTHANOREQUALTO]: 'lte',
};

// When the literal is on the LEFT (e.g. "5 > col"), flip the operator for the column.
const FLIPPED_COMPARISON_MAP: Record<string, QueryFilterWithValues['operator']> = {
  [ExpressionType.COMPARE_EQUAL]: 'equals',
  [ExpressionType.COMPARE_NOTEQUAL]: 'notEquals',
  [ExpressionType.COMPARE_GREATERTHAN]: 'lt',
  [ExpressionType.COMPARE_GREATERTHANOREQUALTO]: 'lte',
  [ExpressionType.COMPARE_LESSTHAN]: 'gt',
  [ExpressionType.COMPARE_LESSTHANOREQUALTO]: 'gte',
};

export function extractHavingFromAst(
  havingExpr: ParsedExpression,
  tableName: string,
  measures: readonly Measure[]
): QueryFilterWithValues[] {
  const filters = tryExtractAllHaving(havingExpr, tableName, measures);
  if (filters === null) return [];
  return filters;
}

// All-or-nothing: if ANY HAVING condition can't be matched to a measure, return null.
// This prevents partial extraction that could double-filter or lose conditions.
function tryExtractAllHaving(
  havingExpr: ParsedExpression,
  tableName: string,
  measures: readonly Measure[]
): QueryFilterWithValues[] | null {
  if (havingExpr.class === ExpressionClass.COMPARISON) {
    const filter = extractHavingComparison(havingExpr as ComparisonExpression, tableName, measures);
    return filter ? [filter] : null;
  }
  if (
    havingExpr.class === ExpressionClass.CONJUNCTION &&
    havingExpr.type === ExpressionType.CONJUNCTION_AND
  ) {
    const conj = havingExpr as ConjunctionExpression;
    const filters: QueryFilterWithValues[] = [];
    for (const child of conj.children) {
      const childFilters = tryExtractAllHaving(child, tableName, measures);
      if (childFilters === null) return null;
      filters.push(...childFilters);
    }
    return filters;
  }
  return null;
}

function extractHavingComparison(
  comp: ComparisonExpression,
  tableName: string,
  measures: readonly Measure[]
): QueryFilterWithValues | null {
  const measureLeft = matchMeasureFromExpr(comp.left, measures);
  if (measureLeft) {
    const value = getConstantValue(comp.right);
    const op = COMPARISON_OPERATOR_MAP[comp.type];
    if (op && value !== null) {
      return {
        member: `${tableName}.${measureLeft.name}`,
        operator: op,
        values: [String(value)],
      };
    }
  }
  const measureRight = matchMeasureFromExpr(comp.right, measures);
  if (measureRight) {
    const value = getConstantValue(comp.left);
    const op = FLIPPED_COMPARISON_MAP[comp.type];
    if (op && value !== null) {
      return {
        member: `${tableName}.${measureRight.name}`,
        operator: op,
        values: [String(value)],
      };
    }
  }
  return null;
}
