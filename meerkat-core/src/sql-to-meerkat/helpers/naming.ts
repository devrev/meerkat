import {
  ColumnRefExpression,
  ExpressionClass,
  FunctionExpression,
  OperatorExpression,
  ParsedExpression,
} from '../../types/duckdb-serialization-types';
import { Dimension } from '../../types/cube-types/table';
import { isStarExpr } from './aggregate-detection';

/**
 * Extracts a short name from an expression for use as a dimension name fallback.
 *
 * - COLUMN_REF: last segment of column_names (e.g. ["t","status"] → "status")
 * - FUNCTION: the function name (e.g. "date_trunc")
 * - Everything else: generic "col"
 *
 * Only used when the expression has no explicit alias.
 */
export function exprToName(expr: ParsedExpression): string {
  if (expr.class === ExpressionClass.COLUMN_REF) {
    const col = expr as ColumnRefExpression;
    return col.column_names[col.column_names.length - 1];
  }
  if (expr.class === ExpressionClass.FUNCTION) {
    const fn = expr as FunctionExpression;
    return fn.function_name;
  }
  return 'col';
}

/**
 * Generates a readable auto-name for unaliased aggregate expressions.
 *
 * Examples:
 * - COUNT(*) → "count"
 * - SUM(amount) → "sum_amount"
 * - SUM(amount) / COUNT(*) → "sum_amount_count"
 * - ROUND(AVG(x), 2) → "avg_x" (unwraps non-aggregate wrappers)
 * - CAST(SUM(x) AS INT) → "sum_x" (unwraps CAST)
 *
 * For arithmetic operators (DuckDB FUNCTION with is_operator=true),
 * recursively generates names for each operand and joins with "_".
 */
export function generateAggregateName(expr: ParsedExpression): string {
  // OPERATOR class (IS NULL, IN) — join child names
  if (expr.class === ExpressionClass.OPERATOR) {
    const op = expr as OperatorExpression;
    const parts = op.children.map((child) => generateAggregateName(child));
    return parts.join('_');
  }
  // CAST — unwrap and name the inner expression
  if (expr.class === ExpressionClass.CAST) {
    const cast = expr as ParsedExpression & { child?: ParsedExpression };
    return cast.child ? generateAggregateName(cast.child) : 'measure';
  }
  // Non-function leaf (CONSTANT, COLUMN_REF, etc.) — generic fallback
  if (expr.class !== ExpressionClass.FUNCTION) return 'measure';

  const fn = expr as FunctionExpression;
  const fnName = fn.function_name.toLowerCase();

  // Arithmetic operators (+, -, *, /) are FUNCTION with is_operator=true in DuckDB
  if ((fn as FunctionExpression & { is_operator?: boolean }).is_operator) {
    const parts = fn.children.map((child) => generateAggregateName(child));
    return parts.join('_');
  }

  // COUNT(*) / count_star() — always just "count"
  if (
    fnName === 'count_star' ||
    fn.children.length === 0 ||
    isStarExpr(fn.children[0])
  ) {
    return 'count';
  }

  // Standard aggregate: "fnName_firstChildName" (e.g. "sum_amount", "avg_priority")
  const childName = exprToName(fn.children[0]);
  return `${fnName}_${childName}`;
}

/**
 * Sanitizes a name to valid SQL identifier characters and ensures uniqueness.
 *
 * Steps:
 * 1. Replace non-alphanumeric/underscore chars with "_"
 * 2. Strip leading/trailing underscores
 * 3. If empty after cleaning, use "col"
 * 4. If name collides with existing set, append "_2", "_3", etc.
 *
 * Mutates the `existing` set by adding the returned name.
 */
export function deduplicateName(name: string, existing: Set<string>): string {
  // Clean: keep only [a-zA-Z0-9_], strip edge underscores
  const clean =
    name.replace(/[^a-zA-Z0-9_]/g, '_').replace(/^_+|_+$/g, '') || 'col';
  if (!existing.has(clean)) {
    existing.add(clean);
    return clean;
  }
  // Collision: find first available suffix
  let i = 2;
  while (existing.has(`${clean}_${i}`)) i++;
  const deduped = `${clean}_${i}`;
  existing.add(deduped);
  return deduped;
}

/**
 * Infers Meerkat dimension type from the AST expression structure.
 *
 * - date_trunc/date_part/strftime → 'time'
 * - CAST to timestamp/date type → 'time'
 * - Everything else → 'string' (safe default)
 *
 * Only used for dimensions in the SELECT list. Measures always get 'number'.
 * Filter-derived dimensions use typeFromConstantExpr (AST constant type) instead.
 */
export function inferTypeFromExpr(expr: ParsedExpression): Dimension['type'] {
  if (expr.class === ExpressionClass.FUNCTION) {
    const fn = expr as FunctionExpression;
    const name = fn.function_name.toLowerCase();
    if (name === 'date_trunc' || name === 'date_part' || name === 'strftime') {
      return 'time';
    }
  }
  if (expr.class === ExpressionClass.CAST) {
    const cast = expr as ParsedExpression & { cast_type?: { id?: string } };
    const typeName = cast.cast_type?.id?.toLowerCase() || '';
    if (typeName.includes('timestamp') || typeName.includes('date')) {
      return 'time';
    }
  }
  return 'string';
}
