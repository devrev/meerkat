import {
  LogicalAndFilter,
  LogicalOrFilter,
  QueryFilterWithValues,
} from '../types/cube-types/query';
import { Dimension, Measure } from '../types/cube-types/table';
import {
  BetweenExpression,
  ComparisonExpression,
  ConjunctionExpression,
  ExpressionClass,
  ExpressionType,
  FunctionExpression,
  OperatorExpression,
  ParsedExpression,
} from '../types/duckdb-serialization-types';
import { getConstantValue, getConstantTypeId, getQualifiedColumnRef, matchMeasureFromExpr } from './helpers';

const COMPARISON_OPERATOR_MAP: Record<string, QueryFilterWithValues['operator']> = {
  [ExpressionType.COMPARE_EQUAL]: 'equals',
  [ExpressionType.COMPARE_NOTEQUAL]: 'notEquals',
  [ExpressionType.COMPARE_GREATERTHAN]: 'gt',
  [ExpressionType.COMPARE_GREATERTHANOREQUALTO]: 'gte',
  [ExpressionType.COMPARE_LESSTHAN]: 'lt',
  [ExpressionType.COMPARE_LESSTHANOREQUALTO]: 'lte',
};

const FLIPPED_COMPARISON_MAP: Record<string, QueryFilterWithValues['operator']> = {
  [ExpressionType.COMPARE_EQUAL]: 'equals',
  [ExpressionType.COMPARE_NOTEQUAL]: 'notEquals',
  [ExpressionType.COMPARE_GREATERTHAN]: 'lt',
  [ExpressionType.COMPARE_GREATERTHANOREQUALTO]: 'lte',
  [ExpressionType.COMPARE_LESSTHAN]: 'gt',
  [ExpressionType.COMPARE_LESSTHANOREQUALTO]: 'gte',
};

export interface FilterExtractionResult {
  filters: (QueryFilterWithValues | LogicalOrFilter)[];
  residual: ParsedExpression | undefined;
  warnings: string[];
}

export function extractFiltersFromAst(
  whereExpr: ParsedExpression,
  tableName: string
): FilterExtractionResult {
  const filters: (QueryFilterWithValues | LogicalOrFilter)[] = [];
  const residualParts: ParsedExpression[] = [];
  const warnings: string[] = [];

  if (whereExpr.class === ExpressionClass.CONJUNCTION) {
    const conj = whereExpr as ConjunctionExpression;
    if (whereExpr.type === ExpressionType.CONJUNCTION_AND) {
      for (const child of conj.children) {
        const extracted = tryExtractFilters(child, tableName);
        if (extracted) {
          filters.push(...extracted);
        } else {
          residualParts.push(child);
          warnings.push('Non-extractable WHERE condition retained in base SQL');
        }
      }
    } else if (whereExpr.type === ExpressionType.CONJUNCTION_OR) {
      const orFilter = extractOrFilter(conj, tableName);
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
    const extracted = tryExtractFilters(whereExpr, tableName);
    if (extracted) {
      filters.push(...extracted);
    } else {
      residualParts.push(whereExpr);
      warnings.push('Non-extractable WHERE condition retained in base SQL');
    }
  }

  const residual = buildResidualConjunction(residualParts);
  return { filters, residual, warnings };
}

export function extractHavingFromAst(
  havingExpr: ParsedExpression,
  tableName: string,
  measures: readonly Measure[]
): QueryFilterWithValues[] {
  const filters = tryExtractAllHaving(havingExpr, tableName, measures);
  if (filters === null) return [];
  return filters;
}

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

export function ensureFilterColumnInSchema(
  filter: QueryFilterWithValues,
  dimensions: readonly Dimension[],
  tableName: string
): Dimension[] | null {
  const prefix = `${tableName}.`;
  if (!filter.member.startsWith(prefix)) return null;
  const colName = filter.member.slice(prefix.length);
  const exists = dimensions.some(
    (d) => d.name === colName || d.sql === colName
  );
  if (!exists) {
    return [{ name: colName, sql: colName, type: inferFilterColumnType(filter) }];
  }
  return null;
}

const DATE_OPERATORS = new Set(['inDateRange', 'notInDateRange', 'beforeDate', 'afterDate', 'onTheDate']);
const NUMERIC_PATTERN = /^-?\d+(\.\d+)?$/;

function inferFilterColumnType(filter: QueryFilterWithValues): Dimension['type'] {
  if (DATE_OPERATORS.has(filter.operator)) {
    return 'time';
  }
  if (filter.values && filter.values.length > 0 && filter.values.every((v) => NUMERIC_PATTERN.test(v))) {
    return 'number';
  }
  return 'string';
}

export function ensureOrFilterColumnsInSchema(
  orFilter: LogicalOrFilter,
  dimensions: readonly Dimension[],
  tableName: string
): Dimension[] | null {
  const newDims: Dimension[] = [];
  collectFilterDimensions(orFilter, dimensions, tableName, newDims);
  return newDims.length > 0 ? newDims : null;
}

function collectFilterDimensions(
  filter: QueryFilterWithValues | LogicalOrFilter | LogicalAndFilter,
  dimensions: readonly Dimension[],
  tableName: string,
  newDims: Dimension[]
): void {
  if ('member' in filter) {
    const added = ensureFilterColumnInSchema(
      filter as QueryFilterWithValues,
      [...dimensions, ...newDims],
      tableName
    );
    if (added) newDims.push(...added);
  } else if ('or' in filter) {
    for (const child of (filter as LogicalOrFilter).or) {
      collectFilterDimensions(child, dimensions, tableName, newDims);
    }
  } else if ('and' in filter) {
    for (const child of (filter as LogicalAndFilter).and) {
      collectFilterDimensions(
        child as QueryFilterWithValues | LogicalOrFilter | LogicalAndFilter,
        dimensions,
        tableName,
        newDims
      );
    }
  }
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
  tableName: string
): LogicalOrFilter | null {
  const orChildren: (QueryFilterWithValues | LogicalAndFilter)[] = [];
  for (const child of conj.children) {
    if (
      child.class === ExpressionClass.CONJUNCTION &&
      child.type === ExpressionType.CONJUNCTION_AND
    ) {
      const andConj = child as ConjunctionExpression;
      const andMembers: (QueryFilterWithValues | LogicalOrFilter)[] = [];
      let allExtracted = true;
      for (const grandchild of andConj.children) {
        const extracted = tryExtractFilters(grandchild, tableName);
        if (extracted) {
          andMembers.push(...extracted);
        } else {
          allExtracted = false;
          break;
        }
      }
      if (!allExtracted) return null;
      orChildren.push({ and: andMembers } as LogicalAndFilter);
    } else {
      const extracted = tryExtractFilters(child, tableName);
      if (!extracted) return null;
      if (extracted.length === 1) {
        orChildren.push(extracted[0] as QueryFilterWithValues | LogicalAndFilter);
      } else {
        orChildren.push({ and: extracted } as LogicalAndFilter);
      }
    }
  }
  return { or: orChildren };
}

function tryExtractFilters(
  expr: ParsedExpression,
  tableName: string
): (QueryFilterWithValues | LogicalOrFilter)[] | null {
  if (expr.class === ExpressionClass.COMPARISON) {
    const f = extractComparisonFilter(expr as ComparisonExpression, tableName);
    return f ? [f] : null;
  }
  if (expr.class === ExpressionClass.BETWEEN) {
    return extractBetweenFilter(expr as BetweenExpression, tableName);
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
    const orFilter = extractOrFilter(expr as ConjunctionExpression, tableName);
    return orFilter ? [orFilter] : null;
  }
  return null;
}

function resolveMemberName(
  expr: ParsedExpression,
  tableName: string
): string | null {
  const ref = getQualifiedColumnRef(expr);
  if (!ref) return null;
  if (ref.table && ref.table !== tableName) return null;
  return `${tableName}.${ref.column}`;
}

function isConstantLike(expr: ParsedExpression): boolean {
  return (
    expr.class === ExpressionClass.CONSTANT ||
    (expr.class === ExpressionClass.CAST && getConstantValue(expr) !== null)
  );
}

function extractComparisonFilter(
  expr: ComparisonExpression,
  tableName: string
): QueryFilterWithValues | null {
  const memberLeft = resolveMemberName(expr.left, tableName);
  const memberRight = resolveMemberName(expr.right, tableName);

  if (memberLeft && isConstantLike(expr.right)) {
    return buildComparisonResult(
      memberLeft,
      expr.right,
      COMPARISON_OPERATOR_MAP[expr.type],
      expr.type
    );
  }

  if (memberRight && isConstantLike(expr.left)) {
    return buildComparisonResult(
      memberRight,
      expr.left,
      FLIPPED_COMPARISON_MAP[expr.type],
      expr.type
    );
  }

  return null;
}

function buildComparisonResult(
  member: string,
  constantExpr: ParsedExpression,
  operator: QueryFilterWithValues['operator'] | undefined,
  compType: string
): QueryFilterWithValues | null {
  const value = getConstantValue(constantExpr);

  if (value === null) {
    if (compType === ExpressionType.COMPARE_EQUAL) {
      return { member, operator: 'notSet', values: [] };
    }
    if (compType === ExpressionType.COMPARE_NOTEQUAL) {
      return { member, operator: 'set', values: [] };
    }
    return null;
  }

  if (!operator) return null;
  return { member, operator, values: [String(value)] };
}

function isDateTypedConstant(expr: ParsedExpression): boolean {
  if (expr.class === ExpressionClass.CAST) {
    const cast = expr as ParsedExpression & { cast_type?: { id?: string }; child?: ParsedExpression };
    const castId = (cast.cast_type?.id || '').toUpperCase();
    if (castId.includes('DATE') || castId.includes('TIMESTAMP')) return true;
    if (cast.child) return isDateTypedConstant(cast.child);
    return false;
  }
  const typeId = getConstantTypeId(expr);
  if (!typeId) return false;
  const upper = typeId.toUpperCase();
  return upper.includes('DATE') || upper.includes('TIMESTAMP') || upper === 'INTERVAL';
}

function isBetweenDateRange(expr: BetweenExpression): boolean {
  return isDateTypedConstant(expr.lower) || isDateTypedConstant(expr.upper);
}

function extractBetweenFilter(
  expr: BetweenExpression,
  tableName: string
): QueryFilterWithValues[] | null {
  const member = resolveMemberName(expr.input, tableName);
  if (!member) return null;

  const lower = getConstantValue(expr.lower);
  const upper = getConstantValue(expr.upper);
  if (lower === null || upper === null) return null;

  if (isBetweenDateRange(expr)) {
    return [
      { member, operator: 'inDateRange', values: [String(lower), String(upper)] },
    ];
  }

  return [
    { member, operator: 'gte', values: [String(lower)] },
    { member, operator: 'lte', values: [String(upper)] },
  ];
}

const IN_OPERATOR_MAP: Record<string, QueryFilterWithValues['operator']> = {
  [ExpressionType.COMPARE_IN]: 'in',
  [ExpressionType.COMPARE_NOT_IN]: 'notIn',
};

function extractOperatorFilter(
  expr: OperatorExpression,
  tableName: string
): QueryFilterWithValues | null {
  if (
    expr.type === ExpressionType.OPERATOR_IS_NULL &&
    expr.children.length === 1
  ) {
    const member = resolveMemberName(expr.children[0], tableName);
    if (!member) return null;
    return { member, operator: 'notSet', values: [] };
  }
  if (
    expr.type === ExpressionType.OPERATOR_IS_NOT_NULL &&
    expr.children.length === 1
  ) {
    const member = resolveMemberName(expr.children[0], tableName);
    if (!member) return null;
    return { member, operator: 'set', values: [] };
  }

  const inOp = IN_OPERATOR_MAP[expr.type];
  if (inOp && expr.children.length >= 2) {
    const member = resolveMemberName(expr.children[0], tableName);
    if (!member) return null;
    const values: string[] = [];
    for (let i = 1; i < expr.children.length; i++) {
      const val = getConstantValue(expr.children[i]);
      if (val === null) return null;
      values.push(String(val));
    }
    return { member, operator: inOp, values };
  }

  return null;
}

const LIKE_OPERATOR_MAP: Record<string, QueryFilterWithValues['operator']> = {
  '~~': 'contains',
  '~~*': 'contains',
  '!~~': 'notContains',
  '!~~*': 'notContains',
};

function extractFunctionFilter(
  expr: FunctionExpression,
  tableName: string
): QueryFilterWithValues | null {
  const fnName = expr.function_name.toLowerCase();
  const likeOp = LIKE_OPERATOR_MAP[fnName];
  if (!likeOp || expr.children.length < 2) return null;

  const member = resolveMemberName(expr.children[0], tableName);
  if (!member) return null;

  const patternVal = getConstantValue(expr.children[1]);
  if (patternVal === null) return null;

  const pattern = String(patternVal);
  if (!pattern.startsWith('%') || !pattern.endsWith('%') || pattern.length <= 2)
    return null;

  const inner = pattern.slice(1, -1);
  if (inner.includes('%') || inner.includes('_')) return null;

  return { member, operator: likeOp, values: [inner] };
}

