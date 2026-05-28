import {
  ColumnRefExpression,
  ExpressionClass,
  FunctionExpression,
  OperatorExpression,
  ParsedExpression,
} from '../types/duckdb-serialization-types';
import { Dimension } from '../types/cube-types/table';
import { isStarExpr } from './aggregate-detection';

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

// Generates a readable auto-name for unaliased aggregate expressions.
// Examples: COUNT(*) → "count", SUM(amount) → "sum_amount",
// SUM(amount)/COUNT(*) → "sum_amount_count" (joins child names with _).
export function generateAggregateName(expr: ParsedExpression): string {
  if (expr.class === ExpressionClass.OPERATOR) {
    const op = expr as OperatorExpression;
    const parts = op.children.map((child) => generateAggregateName(child));
    return parts.join('_');
  }
  if (expr.class === ExpressionClass.CAST) {
    const cast = expr as ParsedExpression & { child?: ParsedExpression };
    return cast.child ? generateAggregateName(cast.child) : 'measure';
  }
  if (expr.class !== ExpressionClass.FUNCTION) return 'measure';
  const fn = expr as FunctionExpression;
  const fnName = fn.function_name.toLowerCase();

  // DuckDB represents arithmetic operators (+,-,*,/) as FUNCTION with is_operator=true
  if ((fn as FunctionExpression & { is_operator?: boolean }).is_operator) {
    const parts = fn.children.map((child) => generateAggregateName(child));
    return parts.join('_');
  }

  if (
    fnName === 'count_star' ||
    fn.children.length === 0 ||
    isStarExpr(fn.children[0])
  ) {
    return 'count';
  }

  const childName = exprToName(fn.children[0]);
  return `${fnName}_${childName}`;
}

// Sanitizes a name to valid SQL identifier chars and appends _N suffix on collision.
export function deduplicateName(name: string, existing: Set<string>): string {
  const clean =
    name.replace(/[^a-zA-Z0-9_]/g, '_').replace(/^_+|_+$/g, '') || 'col';
  if (!existing.has(clean)) {
    existing.add(clean);
    return clean;
  }
  let i = 2;
  while (existing.has(`${clean}_${i}`)) i++;
  const deduped = `${clean}_${i}`;
  existing.add(deduped);
  return deduped;
}

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
