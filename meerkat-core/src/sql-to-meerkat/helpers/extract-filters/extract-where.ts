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
import { getConstantValue, getConstantTypeId, isNullConstant } from '../../../utils/ast-constants';
import { getQualifiedColumnRef } from '../../../utils/ast-column-ref';
import { getNamespacedKey } from '../../../member-formatters/get-namespaced-key';
import { typeFromConstantExpr } from './filter-schema';

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

// ─── Comparison ───────────────────────────────────────────────────────────────

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

function resolveMemberName(
  expr: ParsedExpression,
  tableName: string
): string | null {
  const ref = getQualifiedColumnRef(expr);
  if (!ref) return null;
  if (ref.table && ref.table !== tableName) return null;
  return getNamespacedKey(tableName, ref.column);
}

function isConstantLike(expr: ParsedExpression): boolean {
  if (expr.class === ExpressionClass.CONSTANT) return true;
  if (expr.class === ExpressionClass.CAST) {
    const cast = expr as ParsedExpression & { child?: ParsedExpression };
    return cast.child ? isConstantLike(cast.child) : false;
  }
  return false;
}

function extractComparisonFilter(
  expr: ComparisonExpression,
  tableName: string,
  memberTypes: Record<string, Dimension['type']>
): QueryFilterWithValues | null {
  const memberLeft = resolveMemberName(expr.left, tableName);
  const memberRight = resolveMemberName(expr.right, tableName);

  if (memberLeft && isConstantLike(expr.right)) {
    if (!memberTypes[memberLeft]) {
      memberTypes[memberLeft] = typeFromConstantExpr(expr.right);
    }
    return buildComparisonResult(
      memberLeft,
      expr.right,
      COMPARISON_OPERATOR_MAP[expr.type],
      expr.type
    );
  }

  if (memberRight && isConstantLike(expr.left)) {
    if (!memberTypes[memberRight]) {
      memberTypes[memberRight] = typeFromConstantExpr(expr.left);
    }
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
  if (isNullConstant(constantExpr)) {
    if (compType === ExpressionType.COMPARE_EQUAL) {
      return { member, operator: 'notSet', values: [] };
    }
    if (compType === ExpressionType.COMPARE_NOTEQUAL) {
      return { member, operator: 'set', values: [] };
    }
    return null;
  }

  const value = getConstantValue(constantExpr);
  if (value === null || !operator) return null;
  return { member, operator, values: [String(value)] };
}

// ─── BETWEEN ──────────────────────────────────────────────────────────────────

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
  tableName: string,
  memberTypes: Record<string, Dimension['type']>
): QueryFilterWithValues[] | null {
  const member = resolveMemberName(expr.input, tableName);
  if (!member) return null;

  const lower = getConstantValue(expr.lower);
  const upper = getConstantValue(expr.upper);
  if (lower === null || upper === null) return null;

  if (isBetweenDateRange(expr)) {
    if (!memberTypes[member]) memberTypes[member] = 'time';
    return [
      { member, operator: 'inDateRange', values: [String(lower), String(upper)] },
    ];
  }

  if (!memberTypes[member]) memberTypes[member] = typeFromConstantExpr(expr.lower);
  return [
    { member, operator: 'gte', values: [String(lower)] },
    { member, operator: 'lte', values: [String(upper)] },
  ];
}

// ─── IN / NOT IN / IS NULL ────────────────────────────────────────────────────

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

// ─── LIKE / ILIKE ─────────────────────────────────────────────────────────────

// DuckDB represents LIKE/ILIKE as FUNCTION nodes with these operator names
const LIKE_OPERATOR_MAP: Record<string, QueryFilterWithValues['operator']> = {
  '~~': 'contains',     // LIKE
  '~~*': 'contains',    // ILIKE
  '!~~': 'notContains', // NOT LIKE
  '!~~*': 'notContains', // NOT ILIKE
};

// Extracts LIKE/ILIKE '%value%' patterns as contains/notContains filters.
// Only supports simple substring patterns — rejects wildcards in the inner value.
function extractFunctionFilter(
  expr: FunctionExpression,
  tableName: string
): QueryFilterWithValues | null {
  // DuckDB uses function names like '~~*' for ILIKE — look up in map
  const fnName = expr.function_name.toLowerCase();
  const likeOp = LIKE_OPERATOR_MAP[fnName];
  if (!likeOp || expr.children.length < 2) return null;

  // children[0] = column being matched, children[1] = pattern constant
  const member = resolveMemberName(expr.children[0], tableName);
  if (!member) return null;

  const patternVal = getConstantValue(expr.children[1]);
  if (patternVal === null) return null;

  // Only extract %value% patterns (must have leading AND trailing %, with content between)
  const pattern = String(patternVal);
  if (!pattern.startsWith('%') || !pattern.endsWith('%') || pattern.length <= 2)
    return null;

  // Reject complex patterns with inner wildcards (e.g. '%te_t%', '%a%b%')
  const inner = pattern.slice(1, -1);
  if (inner.includes('%') || inner.includes('_')) return null;

  return { member, operator: likeOp, values: [inner] };
}
