import { COLUMN_NAME_DELIMITER } from '../member-formatters';
import { ParsedExpression } from '../types/duckdb-serialization-types';
import {
  isBetweenExpression,
  isCaseExpression,
  isCastExpression,
  isCollateExpression,
  isColumnRefExpression,
  isComparisonExpression,
  isConjunctionExpression,
  isFunctionExpression,
  isOperatorExpression,
  isSelectNode,
  isSubqueryExpression,
  isWindowExpression,
} from '../types/utils';

export const getColumnNamesFromAst = (
  node: ParsedExpression,
  columnSet: Set<string> = new Set()
): string[] => {
  if (!node) return Array.from(columnSet);

  if (isBetweenExpression(node)) {
    getColumnNamesFromAst(node.input, columnSet);
    getColumnNamesFromAst(node.lower, columnSet);
    getColumnNamesFromAst(node.upper, columnSet);
  }

  if (isColumnRefExpression(node)) {
    columnSet.add(node.column_names.join(COLUMN_NAME_DELIMITER));
  }

  if (isCastExpression(node) || isCollateExpression(node)) {
    getColumnNamesFromAst(node.child, columnSet);
  }

  if (isComparisonExpression(node)) {
    getColumnNamesFromAst(node.left, columnSet);
    getColumnNamesFromAst(node.right, columnSet);
  }

  if (
    isFunctionExpression(node) ||
    isWindowExpression(node) ||
    isOperatorExpression(node) ||
    isConjunctionExpression(node)
  ) {
    node.children.forEach((child) => getColumnNamesFromAst(child, columnSet));
  }

  if (isCaseExpression(node)) {
    node.case_checks.forEach((check) => {
      getColumnNamesFromAst(check.then_expr, columnSet);
      getColumnNamesFromAst(check.when_expr, columnSet);
    });
    getColumnNamesFromAst(node.else_expr, columnSet);
  }

  if (isSubqueryExpression(node) && isSelectNode(node.subquery.node)) {
    node.subquery.node.select_list.forEach((node) => {
      getColumnNamesFromAst(node, columnSet);
    });
  }

  return Array.from(columnSet);
};
