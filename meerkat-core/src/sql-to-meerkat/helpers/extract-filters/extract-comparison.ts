import { QueryFilterWithValues } from '../../../types/cube-types/query';
import { Dimension } from '../../../types/cube-types/table';
import {
  ComparisonExpression,
  ExpressionClass,
  ExpressionType,
  ParsedExpression,
} from '../../../types/duckdb-serialization-types';
import { getConstantValue, isNullConstant } from '../../../utils/ast-constants';
import { getQualifiedColumnRef } from '../../../utils/ast-column-ref';
import { getNamespacedKey } from '../../../member-formatters/get-namespaced-key';
import { typeFromConstantExpr } from './filter-schema';

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

// Resolves a column ref to a qualified member name (e.g. "tickets.status").
// Rejects cross-table references (e.g. u.team when tableName is "t") — those go to residual.
export function resolveMemberName(
  expr: ParsedExpression,
  tableName: string
): string | null {
  const ref = getQualifiedColumnRef(expr);
  if (!ref) return null;
  if (ref.table && ref.table !== tableName) return null;
  return getNamespacedKey(tableName, ref.column);
}

// Checks if an expression is a constant (possibly wrapped in CAST chains).
export function isConstantLike(expr: ParsedExpression): boolean {
  if (expr.class === ExpressionClass.CONSTANT) return true;
  if (expr.class === ExpressionClass.CAST) {
    const cast = expr as ParsedExpression & { child?: ParsedExpression };
    return cast.child ? isConstantLike(cast.child) : false;
  }
  return false;
}

export function extractComparisonFilter(
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
