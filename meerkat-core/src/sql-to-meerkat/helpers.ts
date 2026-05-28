import {
  ColumnRefExpression,
  ConstantExpression,
  ExpressionClass,
  ExpressionType,
  FunctionExpression,
  OperatorExpression,
  ParsedExpression,
  QueryNodeType,
  SelectNode,
} from '../types/duckdb-serialization-types';
import { Dimension, Measure } from '../types/cube-types/table';
import { GetQueryOutput } from '../utils/duckdb-ast-parse-serialize';

/**
 * Utility functions for DuckDB AST inspection.
 *
 * All functions operate on DuckDB's JSON-serialized AST nodes (from json_serialize_sql).
 * They use expression class/type enums — never string parsing or regex on SQL text.
 */

// Queries DuckDB's function catalog to get all registered aggregate functions.
// This covers built-in, extension, and user-defined aggregates dynamically.
export async function fetchAggregateFunctions(
  getQueryOutput: GetQueryOutput
): Promise<Set<string>> {
  const rows = await getQueryOutput(
    "SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_type = 'aggregate'"
  );
  return new Set(rows.map((r) => r['function_name'].toLowerCase()));
}

// Recursively checks if an expression contains an aggregate function anywhere in its tree.
// Handles: FUNCTION (direct or nested), CAST(agg), OPERATOR(IS NULL/IN wrapping agg).
// Note: DuckDB represents arithmetic (+,-,*,/) as FUNCTION with is_operator=true, not OPERATOR.
export function isAggregateExpr(
  expr: ParsedExpression,
  aggregateFunctions: ReadonlySet<string>
): boolean {
  if (expr.class === ExpressionClass.FUNCTION) {
    const fn = expr as FunctionExpression;
    if (aggregateFunctions.has(fn.function_name.toLowerCase())) return true;
    return fn.children.some((child) => isAggregateExpr(child, aggregateFunctions));
  }
  if (expr.class === ExpressionClass.CAST) {
    const cast = expr as ParsedExpression & { child?: ParsedExpression };
    return cast.child ? isAggregateExpr(cast.child, aggregateFunctions) : false;
  }
  if (expr.class === ExpressionClass.OPERATOR) {
    const op = expr as OperatorExpression;
    return op.children.some((child) => isAggregateExpr(child, aggregateFunctions));
  }
  return false;
}

export function isWindowExpr(expr: ParsedExpression): boolean {
  return expr.class === ExpressionClass.WINDOW;
}

export function isStarExpr(expr: ParsedExpression): boolean {
  return expr.class === ExpressionClass.STAR;
}

function isDirectAggregate(
  expr: ParsedExpression,
  aggregateFunctions: ReadonlySet<string>
): boolean {
  if (expr.class !== ExpressionClass.FUNCTION) return false;
  const fn = expr as FunctionExpression;
  return aggregateFunctions.has(fn.function_name.toLowerCase());
}

// Detects invalid nested aggregates like COUNT(SUM(x)). These are syntactically
// valid but rejected by DuckDB at execution — we skip them from the schema.
export function isNestedAggregateExpr(
  expr: ParsedExpression,
  aggregateFunctions: ReadonlySet<string>
): boolean {
  if (!isDirectAggregate(expr, aggregateFunctions)) return false;
  const fn = expr as FunctionExpression;
  return fn.children.some((child) => isAggregateExpr(child, aggregateFunctions));
}

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

export interface QualifiedColumnRef {
  table: string | null;
  column: string;
}

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

export function getColumnName(expr: ParsedExpression): string | null {
  if (expr.class !== ExpressionClass.COLUMN_REF) return null;
  const col = expr as ColumnRefExpression;
  return col.column_names[col.column_names.length - 1];
}

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

// Checks if a constant is explicitly SQL NULL (is_null flag), recursing through CASTs.
// Distinct from getConstantValue returning null (which also covers unrepresentable values).
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

// Extracts a scalar value from a constant AST node. Returns null if:
// - The value is SQL NULL (is_null: true)
// - The value is unrepresentable (HUGEINT object, undefined)
// - The expression is not a constant/cast chain
// DuckDB stores DECIMALs as scaled integers (e.g. 99.5 → value=995, scale=1).
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
    return resolveTableRef(ref.left);
  }
  return ref.alias || 'query';
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- cte_map is deeply nested untyped JSON
export function hasRecursiveCteInMap(node: any): boolean {
  const cteMap = node.cte_map?.map;
  if (!Array.isArray(cteMap)) return false;
  return cteMap.some(
    (entry: any) =>
      entry.value?.query?.node?.type === QueryNodeType.RECURSIVE_CTE_NODE
  );
}

// astSerializerQuery wraps input in single quotes for DuckDB's json_serialize_sql('...')
export function sanitizeForSerialize(sql: string): string {
  return sql.replace(/'/g, "''");
}

// Removes query_location fields from the AST tree (iterative DFS).
// These are byte offsets into the original SQL text — irrelevant for serialization
// and can cause DuckDB's json_deserialize_sql to produce different output.
export function stripQueryLocationInPlace(root: unknown): void {
  const stack: unknown[] = [root];
  while (stack.length > 0) {
    const current = stack.pop();
    if (!current || typeof current !== 'object') continue;
    if (Array.isArray(current)) {
      current.forEach((item) => stack.push(item));
      continue;
    }
    const record = current as Record<string, unknown>;
    if (Object.prototype.hasOwnProperty.call(record, 'query_location')) {
      delete record['query_location'];
    }
    Object.values(record).forEach((value) => stack.push(value));
  }
}

// Matches a HAVING expression to a known measure. Used to extract HAVING as measure filters.
// Handles: alias reference, column ref (DuckDB resolves "HAVING cnt > 1" to COLUMN_REF),
// CAST wrappers, and direct aggregate functions (matched against measure.sql).
export function matchMeasureFromExpr(
  expr: ParsedExpression,
  measures: readonly Measure[]
): Measure | null {
  if (expr.alias) {
    const byAlias = measures.find((m) => m.name === expr.alias);
    if (byAlias) return byAlias;
  }
  // DuckDB represents "HAVING alias > val" as COLUMN_REF, not the original aggregate
  if (expr.class === ExpressionClass.COLUMN_REF) {
    const colName = getColumnName(expr);
    if (colName) {
      return measures.find((m) => m.name === colName) || null;
    }
    return null;
  }
  if (expr.class === ExpressionClass.CAST) {
    const cast = expr as ParsedExpression & { child?: ParsedExpression };
    if (cast.child) return matchMeasureFromExpr(cast.child, measures);
    return null;
  }
  if (expr.class === ExpressionClass.FUNCTION) {
    const fn = expr as FunctionExpression;
    const fnLower = fn.function_name.toLowerCase();
    const childCol =
      fn.children.length === 1 ? getColumnName(fn.children[0]) : null;

    const candidates: string[] = [];
    if (childCol) candidates.push(`${fnLower}(${childCol})`);
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
