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

export interface FilterExtractionResult {
  filters: (QueryFilterWithValues | LogicalOrFilter)[];
  residual: ParsedExpression | undefined;
  warnings: string[];
  memberTypes: Record<string, Dimension['type']>;
}

export function extractFiltersFromAst(
  whereExpr: ParsedExpression,
  tableName: string
): FilterExtractionResult {
  const filters: (QueryFilterWithValues | LogicalOrFilter)[] = [];
  const residualParts: ParsedExpression[] = [];
  const warnings: string[] = [];
  const memberTypes: Record<string, Dimension['type']> = {};

  if (whereExpr.class === ExpressionClass.CONJUNCTION) {
    const conj = whereExpr as ConjunctionExpression;
    if (whereExpr.type === ExpressionType.CONJUNCTION_AND) {
      for (const child of conj.children) {
        const extracted = tryExtractFilters(child, tableName, memberTypes);
        if (extracted) {
          filters.push(...extracted);
        } else {
          residualParts.push(child);
          warnings.push('Non-extractable WHERE condition retained in base SQL');
        }
      }
    } else if (whereExpr.type === ExpressionType.CONJUNCTION_OR) {
      const orFilter = extractOrFilter(conj, tableName, memberTypes);
      if (orFilter) {
        filters.push(orFilter);
      } else {
        residualParts.push(whereExpr);
        warnings.push(
          'OR condition partially non-extractable, retained in base SQL'
        );
      }
    }
  } else {
    const extracted = tryExtractFilters(whereExpr, tableName, memberTypes);
    if (extracted) {
      filters.push(...extracted);
    } else {
      residualParts.push(whereExpr);
      warnings.push('Non-extractable WHERE condition retained in base SQL');
    }
  }

  const residual = buildResidualConjunction(residualParts);
  return { filters, residual, warnings, memberTypes };
}

function buildResidualConjunction(
  parts: ParsedExpression[]
): ParsedExpression | undefined {
  if (parts.length === 0) return undefined;
  if (parts.length === 1) return parts[0];
  return {
    class: ExpressionClass.CONJUNCTION,
    type: ExpressionType.CONJUNCTION_AND,
    alias: '',
    children: parts,
  } as ConjunctionExpression;
}

function extractOrFilter(
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

function tryExtractFilters(
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
