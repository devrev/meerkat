import { COLUMN_NAME_DELIMITER } from '../member-formatters';
import { ParsedExpression } from '../types/duckdb-serialization-types';
import { isColumnRefExpression, isSelectNode, isSubqueryExpression } from '../types/utils';
import { getChildExpressions } from './get-child-expressions';

export const getColumnNamesFromAst = (
  node: ParsedExpression,
  columnSet: Set<string> = new Set()
): string[] => {
  if (!node) return Array.from(columnSet);

  if (isColumnRefExpression(node)) {
    columnSet.add(node.column_names.join(COLUMN_NAME_DELIMITER));
  }

  if (isSubqueryExpression(node) && isSelectNode(node.subquery.node)) {
    node.subquery.node.select_list.forEach((child) => {
      getColumnNamesFromAst(child, columnSet);
    });
  }

  getChildExpressions(node).forEach((child) =>
    getColumnNamesFromAst(child, columnSet)
  );

  return Array.from(columnSet);
};
