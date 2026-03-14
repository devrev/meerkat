import { ParsedExpression } from '../types/duckdb-serialization-types';
import {
  isBetweenExpression,
  isCaseExpression,
  isCastExpression,
  isCollateExpression,
  isComparisonExpression,
  isConjunctionExpression,
  isFunctionExpression,
  isLambdaExpression,
  isOperatorExpression,
  isSubqueryExpression,
  isWindowExpression,
} from '../types/utils';

/**
 * Returns the direct child ParsedExpression nodes reachable from the given
 * expression node.  This centralises the "which fields hold children?" knowledge
 * so that every AST walker does not have to re-implement it.
 *
 * Leaf nodes (ColumnRef, Constant, etc.) return an empty array.
 *
 * NOTE: SubqueryExpression only yields `node.child` here; the nested
 * QueryNode (`node.subquery.node`) requires its own traversal because it is
 * not a ParsedExpression.
 */
export const getChildExpressions = (
  node: ParsedExpression
): ParsedExpression[] => {
  if (isBetweenExpression(node)) {
    return [node.input, node.lower, node.upper];
  }

  if (isCastExpression(node) || isCollateExpression(node)) {
    return [node.child];
  }

  if (isComparisonExpression(node)) {
    return [node.left, node.right];
  }

  if (isFunctionExpression(node)) {
    const children = [...node.children];
    if (node.filter) children.push(node.filter);
    return children;
  }

  if (isOperatorExpression(node) || isConjunctionExpression(node)) {
    return [...node.children];
  }

  if (isCaseExpression(node)) {
    const children: ParsedExpression[] = [];
    node.case_checks.forEach((check) => {
      children.push(check.then_expr, check.when_expr);
    });
    if (node.else_expr) children.push(node.else_expr as ParsedExpression);
    return children;
  }

  if (isWindowExpression(node)) {
    const children: ParsedExpression[] = [...node.children];
    children.push(...node.partitions);
    node.orders.forEach((order) => children.push(order.expression));
    if (node.start_expr) children.push(node.start_expr);
    if (node.end_expr) children.push(node.end_expr);
    if (node.offset_expr) children.push(node.offset_expr);
    if (node.default_expr) children.push(node.default_expr);
    if (node.filter_expr) children.push(node.filter_expr);
    return children;
  }

  if (isLambdaExpression(node)) {
    const children: ParsedExpression[] = [node.lhs];
    if (node.expr) children.push(node.expr as ParsedExpression);
    return children;
  }

  if (isSubqueryExpression(node)) {
    const children: ParsedExpression[] = [];
    if (node.child) children.push(node.child);
    return children;
  }

  return [];
};
