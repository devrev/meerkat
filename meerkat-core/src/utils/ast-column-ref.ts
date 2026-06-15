import {
  ColumnRefExpression,
  ExpressionClass,
  ParsedExpression,
} from '../types/duckdb-serialization-types';

export interface QualifiedColumnRef {
  table: string | null;
  column: string;
}

/**
 * Extracts the table qualifier and column name from a COLUMN_REF expression.
 *
 * DuckDB column_names examples:
 * - ["status"]           → { table: null, column: "status" }
 * - ["t", "status"]     → { table: "t", column: "status" }
 * - ["s", "t", "col"]   → { table: "t", column: "col" } (takes last two)
 *
 * Returns null for non-COLUMN_REF expressions.
 */
export function getQualifiedColumnRef(
  expr: ParsedExpression
): QualifiedColumnRef | null {
  if (expr.class !== ExpressionClass.COLUMN_REF) return null;
  const col = expr as ColumnRefExpression;
  if (col.column_names.length >= 2) {
    return {
      table: col.column_names[col.column_names.length - 2],
      column: col.column_names[col.column_names.length - 1],
    };
  }
  return { table: null, column: col.column_names[0] };
}

/**
 * Returns the column name (last segment) from a COLUMN_REF, or null if not a column ref.
 * Simpler version of getQualifiedColumnRef when you don't need the table qualifier.
 */
export function getColumnName(expr: ParsedExpression): string | null {
  if (expr.class !== ExpressionClass.COLUMN_REF) return null;
  const col = expr as ColumnRefExpression;
  return col.column_names[col.column_names.length - 1];
}
