import {
  ColumnRefExpression,
  ConstantExpression,
  ExpressionClass,
  ExpressionType,
  FunctionExpression,
  ParsedExpression,
  QueryNodeType,
  SelectNode,
} from '../types/duckdb-serialization-types';
import { Dimension, Measure } from '../types/cube-types/table';
import {
  GetQueryOutput,
  serializeExpressions,
} from '../utils/duckdb-ast-parse-serialize';

export const AGGREGATE_FUNCTIONS = new Set([
  'count',
  'count_star',
  'sum',
  'avg',
  'min',
  'max',
  'stddev',
  'stddev_pop',
  'stddev_samp',
  'variance',
  'var_pop',
  'var_samp',
  'median',
  'percentile_cont',
  'percentile_disc',
  'approx_count_distinct',
  'list',
  'array_agg',
  'string_agg',
  'group_concat',
  'listagg',
  'first',
  'last',
  'any_value',
  'bit_and',
  'bit_or',
  'bit_xor',
  'bool_and',
  'bool_or',
]);

export function isAggregateExpr(expr: ParsedExpression): boolean {
  if (expr.class === ExpressionClass.FUNCTION) {
    const fn = expr as FunctionExpression;
    return AGGREGATE_FUNCTIONS.has(fn.function_name.toLowerCase());
  }
  return false;
}

export function isWindowExpr(expr: ParsedExpression): boolean {
  return expr.class === ExpressionClass.WINDOW;
}

export function isStarExpr(expr: ParsedExpression): boolean {
  return expr.class === ExpressionClass.STAR;
}

export function isNestedAggregateExpr(expr: ParsedExpression): boolean {
  if (!isAggregateExpr(expr)) return false;
  const fn = expr as FunctionExpression;
  return fn.children.some(isAggregateExpr);
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

export function generateAggregateName(expr: ParsedExpression): string {
  if (expr.class !== ExpressionClass.FUNCTION) return 'measure';
  const fn = expr as FunctionExpression;
  const fnName = fn.function_name.toLowerCase();

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
  if (expr.class === ExpressionClass.COLUMN_REF) {
    const col = expr as ColumnRefExpression;
    const colName = col.column_names[col.column_names.length - 1].toLowerCase();
    if (
      colName.endsWith('_at') ||
      colName.endsWith('_date') ||
      colName === 'timestamp' ||
      colName === 'date' ||
      colName === 'datetime'
    ) {
      return 'time';
    }
  }
  return 'string';
}

export async function exprToSql(
  expr: ParsedExpression,
  getQueryOutput: GetQueryOutput
): Promise<string> {
  const copy = JSON.parse(JSON.stringify(expr));
  copy.alias = '';
  stripQueryLocationInPlace(copy);
  const results = await serializeExpressions([copy], getQueryOutput);
  return results[0];
}

export function getColumnName(expr: ParsedExpression): string | null {
  if (expr.class !== ExpressionClass.COLUMN_REF) return null;
  const col = expr as ColumnRefExpression;
  return col.column_names[col.column_names.length - 1];
}

export function getConstantValue(
  expr: ParsedExpression
): string | number | null {
  if (expr.class !== ExpressionClass.CONSTANT) return null;
  const constant = expr as ConstantExpression;
  const val = constant.value as
    | { is_null?: boolean; value?: string | number }
    | undefined;
  if (val?.is_null) return null;
  return val?.value ?? null;
}

export function extractTableName(selectNode: SelectNode): string {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- from_table shape varies by type
  const fromTable = selectNode.from_table as any;
  if (!fromTable) return 'query';

  if (fromTable.type === 'BASE_TABLE') {
    return fromTable.alias || fromTable.table_name || 'query';
  }
  if (fromTable.type === 'SUBQUERY') {
    return fromTable.alias || 'subquery';
  }
  if (fromTable.type === 'JOIN') {
    const left = fromTable.left;
    if (left?.type === 'BASE_TABLE') {
      return left.alias || left.table_name || 'query';
    }
    return left?.alias || 'query';
  }
  return fromTable.alias || 'query';
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

export function sanitizeForSerialize(sql: string): string {
  return sql.replace(/'/g, "''");
}

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

export function matchMeasureFromExpr(
  expr: ParsedExpression,
  measures: readonly Measure[]
): Measure | null {
  if (expr.class === ExpressionClass.FUNCTION) {
    const fn = expr as FunctionExpression;
    return (
      measures.find((m) =>
        m.sql.toLowerCase().startsWith(fn.function_name.toLowerCase() + '(')
      ) || null
    );
  }
  if (expr.alias) {
    return measures.find((m) => m.name === expr.alias) || null;
  }
  return null;
}
