import { QueryFilterWithValues } from '../../../types/cube-types/query';
import {
  ExpressionType,
  FunctionExpression,
  OperatorExpression,
  ParsedExpression,
} from '../../../types/duckdb-serialization-types';
import { getConstantValue } from '../../../utils/ast-constants';
import { resolveMemberName } from './extract-comparison';

// ─── IN / NOT IN / IS NULL ────────────────────────────────────────────────────

const IN_OPERATOR_MAP: Record<string, QueryFilterWithValues['operator']> = {
  [ExpressionType.COMPARE_IN]: 'in',
  [ExpressionType.COMPARE_NOT_IN]: 'notIn',
};

export function extractOperatorFilter(
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
export function extractFunctionFilter(
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
