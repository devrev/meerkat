import {
  ConstantExpression,
  ExpressionClass,
  ParsedExpression,
} from '../types/duckdb-serialization-types';

/**
 * Returns the DuckDB type ID string (e.g. "INTEGER", "VARCHAR", "TIMESTAMP")
 * from a CONSTANT expression's value.type.id field. Returns null for non-constants.
 */
export function getConstantTypeId(expr: ParsedExpression): string | null {
  if (expr.class !== ExpressionClass.CONSTANT) return null;
  const constant = expr as ConstantExpression;
  const val = constant.value as
    | { type?: { id?: string } }
    | undefined;
  return val?.type?.id || null;
}

/**
 * Checks if an expression is explicitly SQL NULL, recursing through CAST chains.
 *
 * This is distinct from getConstantValue() returning null, which can also mean
 * "unrepresentable value" (e.g. HUGEINT). isNullConstant only returns true when
 * the AST constant has is_null: true.
 *
 * Examples:
 * - NULL → true
 * - CAST(NULL AS INTEGER) → true (recurses through CAST)
 * - 5 → false
 * - HUGEINT literal → false (not null, just unrepresentable)
 */
export function isNullConstant(expr: ParsedExpression): boolean {
  if (expr.class === ExpressionClass.CAST) {
    const cast = expr as ParsedExpression & { child?: ParsedExpression };
    return cast.child ? isNullConstant(cast.child) : false;
  }
  if (expr.class !== ExpressionClass.CONSTANT) return false;
  const constant = expr as ConstantExpression;
  const val = constant.value as { is_null?: boolean } | undefined;
  return val?.is_null === true;
}

/**
 * Extracts a scalar value from a CONSTANT AST node, recursing through CASTs.
 *
 * Returns null in three distinct cases:
 * 1. The constant is SQL NULL (is_null: true)
 * 2. The value is unrepresentable as string|number (HUGEINT → {upper,lower} object)
 * 3. The expression is not a constant/cast chain at all
 *
 * Special handling for DECIMAL: DuckDB stores decimals as scaled integers.
 * For example, 99.5 is stored as value=995 with scale=1.
 * We divide by 10^scale to recover the actual number.
 */
export function getConstantValue(
  expr: ParsedExpression
): string | number | null {
  if (expr.class === ExpressionClass.CAST) {
    const cast = expr as ParsedExpression & { child?: ParsedExpression };
    return cast.child ? getConstantValue(cast.child) : null;
  }
  if (expr.class !== ExpressionClass.CONSTANT) return null;
  const constant = expr as ConstantExpression;
  const val = constant.value as
    | {
        is_null?: boolean;
        value?: string | number;
        type?: { id?: string; type_info?: { scale?: number } };
      }
    | undefined;
  if (val?.is_null) return null;
  if (val?.value == null) return null;

  const rawValue = val.value;
  // HUGEINT produces {upper, lower} objects — not representable as string/number
  if (typeof rawValue !== 'string' && typeof rawValue !== 'number') return null;

  // DECIMAL: recover actual value by dividing by 10^scale
  if (
    val.type?.id === 'DECIMAL' &&
    typeof rawValue === 'number' &&
    typeof val.type.type_info?.scale === 'number' &&
    val.type.type_info.scale > 0
  ) {
    return rawValue / Math.pow(10, val.type.type_info.scale);
  }

  return rawValue;
}
