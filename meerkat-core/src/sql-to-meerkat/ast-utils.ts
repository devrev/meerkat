import {
  ColumnRefExpression,
  ConstantExpression,
  ExpressionClass,
  FunctionExpression,
  ParsedExpression,
  QueryNodeType,
  SelectNode,
} from '../types/duckdb-serialization-types';
import { Measure } from '../types/cube-types/table';
import { isStarExpr } from './aggregate-detection';

export { stripQueryLocationInPlace } from '../utils/duckdb-ast-parse-serialize';

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
