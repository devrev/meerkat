import {
  ExpressionClass,
  FunctionExpression,
  OperatorExpression,
  ParsedExpression,
} from '../types/duckdb-serialization-types';
import { GetQueryOutput } from '../utils/duckdb-ast-parse-serialize';

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
