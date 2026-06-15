import {
  ExpressionClass,
  FunctionExpression,
  OperatorExpression,
  ParsedExpression,
} from '../../types/duckdb-serialization-types';

/**
 * Recursively checks if an expression contains an aggregate function anywhere
 * in its tree.
 *
 * Traversal rules:
 * - FUNCTION: check function_name against known aggregates; if not a match,
 *   recurse into children (handles COALESCE(SUM(x), 0) → contains aggregate)
 * - CAST: unwrap and recurse into child (handles CAST(COUNT(*) AS INT))
 * - OPERATOR: recurse into children (handles IS NULL wrapping an aggregate)
 *
 * Note: DuckDB represents arithmetic operators (+, -, *, /) as FUNCTION nodes
 * with is_operator=true — they fall into the FUNCTION branch and their children
 * are recursed naturally.
 *
 * @param expr - Any DuckDB AST expression node
 * @param aggregateFunctions - Set of lowercase aggregate function names from duckdb_functions()
 * @returns true if any node in the expression tree is a known aggregate
 */
export function isAggregateExpr(
  expr: ParsedExpression,
  aggregateFunctions: ReadonlySet<string>
): boolean {
  if (expr.class === ExpressionClass.FUNCTION) {
    const fn = expr as FunctionExpression;
    // Direct match: this function IS an aggregate (e.g. sum, count_star, avg)
    if (aggregateFunctions.has(fn.function_name.toLowerCase())) return true;
    // Not itself an aggregate, but might wrap one (e.g. ROUND(AVG(x), 2))
    return fn.children.some((child) => isAggregateExpr(child, aggregateFunctions));
  }
  if (expr.class === ExpressionClass.CAST) {
    // Unwrap CAST and check inner expression
    const cast = expr as ParsedExpression & { child?: ParsedExpression };
    return cast.child ? isAggregateExpr(cast.child, aggregateFunctions) : false;
  }
  if (expr.class === ExpressionClass.OPERATOR) {
    // OPERATOR class: IS NULL, IS NOT NULL, IN, NOT IN — check if any child is aggregate
    const op = expr as OperatorExpression;
    return op.children.some((child) => isAggregateExpr(child, aggregateFunctions));
  }
  // COLUMN_REF, CONSTANT, SUBQUERY, WINDOW, STAR, etc. — never aggregates
  return false;
}

/** Returns true if the expression is a window function (ROW_NUMBER, RANK, etc.) */
export function isWindowExpr(expr: ParsedExpression): boolean {
  return expr.class === ExpressionClass.WINDOW;
}

/** Returns true if the expression is SELECT * */
export function isStarExpr(expr: ParsedExpression): boolean {
  return expr.class === ExpressionClass.STAR;
}

/**
 * Checks if the TOP-LEVEL expression is directly an aggregate function.
 * Unlike isAggregateExpr, does NOT recurse — only checks the outermost node.
 * Used by isNestedAggregateExpr to find the outer aggregate before checking children.
 */
function isDirectAggregate(
  expr: ParsedExpression,
  aggregateFunctions: ReadonlySet<string>
): boolean {
  if (expr.class !== ExpressionClass.FUNCTION) return false;
  const fn = expr as FunctionExpression;
  return aggregateFunctions.has(fn.function_name.toLowerCase());
}

/**
 * Detects invalid nested aggregates like COUNT(SUM(x)).
 *
 * These are syntactically valid in DuckDB's parser but rejected at execution.
 * We skip them from the schema to avoid errors downstream.
 *
 * Logic: the expression must be a direct aggregate AND one of its children
 * must also contain an aggregate somewhere in its subtree.
 */
export function isNestedAggregateExpr(
  expr: ParsedExpression,
  aggregateFunctions: ReadonlySet<string>
): boolean {
  // First: is the outer expression itself an aggregate?
  if (!isDirectAggregate(expr, aggregateFunctions)) return false;
  // Second: do any of its children contain aggregates?
  const fn = expr as FunctionExpression;
  return fn.children.some((child) => isAggregateExpr(child, aggregateFunctions));
}
