import {
  ColumnRefExpression,
  ConstantExpression,
  ExpressionClass,
  FunctionExpression,
  ParsedExpression,
  QueryNodeType,
  SelectNode,
} from '../../types/duckdb-serialization-types';
import { Measure } from '../../types/cube-types/table';
import { isStarExpr } from './aggregate-detection';

export { stripQueryLocationInPlace } from '../../utils/duckdb-ast-parse-serialize';

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

/**
 * Returns the DuckDB type ID string (e.g. "INTEGER", "VARCHAR", "TIMESTAMP")
 * from a CONSTANT expression's value.type.id field. Returns null for non-constants.
 */
export function getConstantTypeId(
  expr: ParsedExpression
): string | null {
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
  // Recurse through CAST wrappers (e.g. CAST('2024-01-01' AS DATE))
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
  // Case 1: SQL NULL
  if (val?.is_null) return null;
  // Case 2: missing value field
  if (val?.value == null) return null;

  const rawValue = val.value;
  // Case 3: HUGEINT produces {upper, lower} objects — not representable
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

/**
 * Extracts the primary table name from a SelectNode's FROM clause.
 *
 * Resolution rules:
 * - BASE_TABLE: alias (if present) or table_name → "tickets", "t"
 * - SUBQUERY: alias or fallback "subquery"
 * - JOIN: recurse into left side (the primary/driving table)
 * - TABLE_FUNCTION or other: alias or fallback "query"
 * - No FROM clause: "query"
 */
export function extractTableName(selectNode: SelectNode): string {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- from_table shape varies by type
  const fromTable = selectNode.from_table as any;
  if (!fromTable) return 'query';

  return resolveTableRef(fromTable);
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- from_table shape varies by type
function resolveTableRef(ref: any): string {
  if (!ref) return 'query';
  if (ref.type === 'BASE_TABLE') {
    return ref.alias || ref.table_name || 'query';
  }
  if (ref.type === 'SUBQUERY') {
    return ref.alias || 'subquery';
  }
  if (ref.type === 'JOIN') {
    // For JOINs, the leftmost table is the "primary" table
    return resolveTableRef(ref.left);
  }
  // TABLE_FUNCTION, EMPTY, or unknown — use alias if available
  return ref.alias || 'query';
}

/**
 * Checks if the CTE map contains any WITH RECURSIVE definitions.
 * Used to reject recursive CTEs early (not supported by sqlToMeerkat).
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- cte_map is deeply nested untyped JSON
export function hasRecursiveCteInMap(node: any): boolean {
  const cteMap = node.cte_map?.map;
  if (!Array.isArray(cteMap)) return false;
  return cteMap.some(
    (entry: any) =>
      entry.value?.query?.node?.type === QueryNodeType.RECURSIVE_CTE_NODE
  );
}

/**
 * Escapes single quotes for embedding SQL inside DuckDB's json_serialize_sql('...').
 * Standard SQL escaping: ' → '' (doubled single quote).
 */
export function sanitizeForSerialize(sql: string): string {
  return sql.replace(/'/g, "''");
}

/**
 * Matches a HAVING expression to a known measure from the schema.
 *
 * Used to determine if a HAVING condition can be extracted as a measure filter.
 * Matching strategies (tried in order):
 *
 * 1. Alias match: expr.alias === measure.name
 *    (e.g. HAVING expression with explicit alias matching a SELECT alias)
 *
 * 2. Column ref: DuckDB resolves "HAVING cnt > 1" to a COLUMN_REF with
 *    column_names: ["cnt"]. We match this against measure names.
 *
 * 3. CAST unwrap: recurse into the child (HAVING CAST(COUNT(*) AS INT) > 5)
 *
 * 4. Function match: compare serialized SQL patterns like "sum(amount)"
 *    or "count_star()" against measure.sql values.
 *
 * Returns null if no measure matches — caller should use residual path.
 */
export function matchMeasureFromExpr(
  expr: ParsedExpression,
  measures: readonly Measure[]
): Measure | null {
  // Strategy 1: explicit alias on the AST node
  if (expr.alias) {
    const byAlias = measures.find((m) => m.name === expr.alias);
    if (byAlias) return byAlias;
  }
  // Strategy 2: column reference (DuckDB resolves aliases to COLUMN_REF in HAVING)
  if (expr.class === ExpressionClass.COLUMN_REF) {
    const colName = getColumnName(expr);
    if (colName) {
      return measures.find((m) => m.name === colName) || null;
    }
    return null;
  }
  // Strategy 3: unwrap CAST
  if (expr.class === ExpressionClass.CAST) {
    const cast = expr as ParsedExpression & { child?: ParsedExpression };
    if (cast.child) return matchMeasureFromExpr(cast.child, measures);
    return null;
  }
  // Strategy 4: match function expression against measure.sql
  if (expr.class === ExpressionClass.FUNCTION) {
    const fn = expr as FunctionExpression;
    const fnLower = fn.function_name.toLowerCase();
    // For single-child functions: try "fn(col)" pattern
    const childCol =
      fn.children.length === 1 ? getColumnName(fn.children[0]) : null;

    const candidates: string[] = [];
    if (childCol) candidates.push(`${fnLower}(${childCol})`);
    // For parameterless / star functions: try "fn()" and "count_star()"
    if (fn.children.length === 0 || isStarExpr(fn.children[0])) {
      candidates.push(`${fnLower}()`);
      candidates.push('count_star()');
    }

    return (
      measures.find((m) => candidates.includes(m.sql.toLowerCase())) || null
    );
  }
  return null;
}
