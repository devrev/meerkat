import {
  ExpressionClass,
  FunctionExpression,
  ParsedExpression,
  QueryNodeType,
  SelectNode,
  TableRef,
  TableReferenceType,
} from '../../types/duckdb-serialization-types';
import { Measure } from '../../types/cube-types/table';
import { isStarExpr } from './aggregate-detection';
import { getColumnName } from '../../utils/ast-column-ref';

// Re-export shared utils so existing imports from './ast-utils' continue to work
export { getConstantValue, getConstantTypeId, isNullConstant } from '../../utils/ast-constants';
export { getQualifiedColumnRef, getColumnName } from '../../utils/ast-column-ref';
export type { QualifiedColumnRef } from '../../utils/ast-column-ref';
export { stripQueryLocationInPlace } from '../../utils/duckdb-ast-parse-serialize';

/**
 * Extracts the primary table name from a SelectNode's FROM clause.
 *
 * Resolution rules:
 * - BASE_TABLE: alias (if present) or table_name
 * - SUBQUERY: alias or fallback "subquery"
 * - JOIN: recurse into left side (the primary/driving table)
 * - TABLE_FUNCTION or other: alias or fallback "query"
 * - No FROM clause: "query"
 */
export function extractTableName(selectNode: SelectNode): string {
  if (!selectNode.from_table) return 'query';
  return resolveTableRef(selectNode.from_table);
}

function resolveTableRef(ref: TableRef): string {
  if (ref.type === TableReferenceType.BASE_TABLE) {
    return ref.alias || ref.table_name || 'query';
  }
  if (ref.type === TableReferenceType.SUBQUERY) {
    return ref.alias || 'subquery';
  }
  if (ref.type === TableReferenceType.JOIN) {
    return resolveTableRef(ref.left);
  }
  return ref.alias || 'query';
}

/** Checks if the CTE map contains any WITH RECURSIVE definitions. */
export function hasRecursiveCteInMap(node: SelectNode): boolean {
  const cteMap = node.cte_map?.map;
  if (!Array.isArray(cteMap)) return false;
  // Runtime shape: [{key, value: {query: {node: {type}}}}] — type definition is incomplete
  return cteMap.some(
    (entry: unknown) => {
      const e = entry as { value?: { query?: { node?: { type?: string } } } };
      return e.value?.query?.node?.type === QueryNodeType.RECURSIVE_CTE_NODE;
    }
  );
}

/** Escapes single quotes for embedding SQL inside json_serialize_sql('...'). */
export function sanitizeForSerialize(sql: string): string {
  return sql.replace(/'/g, "''");
}

/**
 * Matches a HAVING expression to a known measure from the schema.
 *
 * Strategies (tried in order):
 * 1. Alias match: expr.alias === measure.name
 * 2. Column ref: DuckDB resolves "HAVING cnt > 1" to COLUMN_REF
 * 3. CAST unwrap: recurse into child
 * 4. Function match: compare serialized SQL patterns against measure.sql
 */
export function matchMeasureFromExpr(
  expr: ParsedExpression,
  measures: readonly Measure[]
): Measure | null {
  if (expr.alias) {
    const byAlias = measures.find((m) => m.name === expr.alias);
    if (byAlias) return byAlias;
  }
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
