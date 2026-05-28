import {
  LogicalAndFilter,
  LogicalOrFilter,
  QueryFilterWithValues,
} from '../../../types/cube-types/query';
import { Dimension } from '../../../types/cube-types/table';
import {
  BetweenExpression,
  ComparisonExpression,
  ConjunctionExpression,
  ExpressionClass,
  ExpressionType,
  FunctionExpression,
  OperatorExpression,
  ParsedExpression,
} from '../../../types/duckdb-serialization-types';
import { extractComparisonFilter } from './extract-comparison';
import { extractBetweenFilter } from './extract-between';
import { extractOperatorFilter, extractFunctionFilter } from './extract-operators';

// Attempts to extract a single WHERE condition into Meerkat filter(s).
// Dispatches by AST expression class to the appropriate leaf extractor.
// Returns null if the expression can't be represented as a Meerkat filter.
export function tryExtractFilters(
  expr: ParsedExpression,
  tableName: string,
  memberTypes: Record<string, Dimension['type']>
): (QueryFilterWithValues | LogicalOrFilter)[] | null {
  if (expr.class === ExpressionClass.COMPARISON) {
    const f = extractComparisonFilter(expr as ComparisonExpression, tableName, memberTypes);
    return f ? [f] : null;
  }
  if (expr.class === ExpressionClass.BETWEEN) {
    return extractBetweenFilter(expr as BetweenExpression, tableName, memberTypes);
  }
  if (expr.class === ExpressionClass.OPERATOR) {
    const f = extractOperatorFilter(expr as OperatorExpression, tableName);
    return f ? [f] : null;
  }
  if (expr.class === ExpressionClass.FUNCTION) {
    const f = extractFunctionFilter(expr as FunctionExpression, tableName);
    return f ? [f] : null;
  }
  if (
    expr.class === ExpressionClass.CONJUNCTION &&
    expr.type === ExpressionType.CONJUNCTION_OR
  ) {
    const orFilter = extractOrFilter(expr as ConjunctionExpression, tableName, memberTypes);
    return orFilter ? [orFilter] : null;
  }
  return null;
}

// Extracts an OR conjunction — all branches must be extractable or entire OR goes to residual.
export function extractOrFilter(
  conj: ConjunctionExpression,
  tableName: string,
  memberTypes: Record<string, Dimension['type']>
): LogicalOrFilter | null {
  const orChildren: (QueryFilterWithValues | LogicalAndFilter)[] = [];
  for (const child of conj.children) {
    const branch = extractOrBranch(child, tableName, memberTypes);
    if (!branch) return null;
    orChildren.push(branch);
  }
  return { or: orChildren };
}

// Extracts a single branch of an OR: either a simple filter or an AND group.
function extractOrBranch(
  expr: ParsedExpression,
  tableName: string,
  memberTypes: Record<string, Dimension['type']>
): QueryFilterWithValues | LogicalAndFilter | null {
  if (
    expr.class === ExpressionClass.CONJUNCTION &&
    expr.type === ExpressionType.CONJUNCTION_AND
  ) {
    const andConj = expr as ConjunctionExpression;
    const andMembers: (QueryFilterWithValues | LogicalOrFilter)[] = [];
    for (const grandchild of andConj.children) {
      const extracted = tryExtractFilters(grandchild, tableName, memberTypes);
      if (!extracted) return null;
      andMembers.push(...extracted);
    }
    return { and: andMembers } as LogicalAndFilter;
  }

  const extracted = tryExtractFilters(expr, tableName, memberTypes);
  if (!extracted) return null;

  if (extracted.length === 1 && 'member' in extracted[0]) {
    return extracted[0] as QueryFilterWithValues;
  }
  return { and: extracted } as LogicalAndFilter;
}
