import {
  BaseTypeTableRef,
  ColumnRefExpression,
  ExpressionType,
  JoinRef,
  OrderModifier,
  ParsedExpression,
  QueryNode,
  QueryNodeType,
  ResultModifierType,
  SelectNode,
  SelectStatement,
  SetOperationNode,
  SubqueryRef,
  TableRef,
  TableReferenceType,
} from '@devrev/meerkat-core';
import { duckdbExec } from './duckdb-exec';

function getReferencedColumns(
  selectStatement: SelectStatement
): Map<string, Set<string>> {
  const referencedColumns = new Map<string, Set<string>>();
  const aliasedColumns = new Map<string, string>();

  function isExpressionWithChildren(
    expr: ParsedExpression
  ): expr is ParsedExpression & { children: ParsedExpression[] } {
    return 'children' in expr;
  }

  function isExpressionWithChild(
    expr: ParsedExpression
  ): expr is ParsedExpression & { child: ParsedExpression } {
    return 'child' in expr;
  }

  function processExpression(
    expr: ParsedExpression,
    currentTableAlias: string
  ) {
    if (expr.type === ExpressionType.COLUMN_REF) {
      const columnRef = expr as ColumnRefExpression;
      const columnName =
        columnRef.column_names[columnRef.column_names.length - 1];

      if (columnRef.alias) {
        aliasedColumns.set(columnRef.alias, columnName);
      }

      if (!referencedColumns.has(currentTableAlias)) {
        referencedColumns.set(currentTableAlias, new Set<string>());
      }

      if (aliasedColumns.has(columnName)) {
        referencedColumns
          .get(currentTableAlias)
          ?.add(aliasedColumns.get(columnName) as string);
      } else {
        referencedColumns.get(currentTableAlias)?.add(columnName);
      }
    } else if (isExpressionWithChildren(expr)) {
      expr.children.forEach((child) =>
        processExpression(child, currentTableAlias)
      );
    } else if (isExpressionWithChild(expr)) {
      processExpression(expr.child, currentTableAlias);
    }
  }

  function processTableRef(tableRef: TableRef, parentTableAlias: string) {
    if (tableRef.type === TableReferenceType.BASE_TABLE) {
      const baseTableRef = tableRef as BaseTypeTableRef;
      const tableName =
        (baseTableRef.schema_name ? baseTableRef.schema_name + '.' : '') +
        baseTableRef.table_name;
      referencedColumns.set(tableName, new Set<string>());
      return tableName;
    } else if (tableRef.type === TableReferenceType.JOIN) {
      const joinRef = tableRef as JoinRef;
      const leftTableAlias = processTableRef(joinRef.left, parentTableAlias);
      const rightTableAlias = processTableRef(joinRef.right, parentTableAlias);
      if (joinRef.condition) {
        processExpression(joinRef.condition, leftTableAlias);
        processExpression(joinRef.condition, rightTableAlias);
      }
      return parentTableAlias;
    } else if (tableRef.type === TableReferenceType.SUBQUERY) {
      const subqueryRef = tableRef as SubqueryRef;
      const subqueryTableAlias = processQueryNode(
        subqueryRef.subquery.node,
        subqueryRef.alias || ''
      );
      return subqueryTableAlias;
    }
    return parentTableAlias;
  }

  function processQueryNode(node: QueryNode, parentTableAlias: string) {
    if (node.type === QueryNodeType.SELECT_NODE) {
      const selectNode = node as SelectNode;
      let tableAlias = parentTableAlias;

      if (selectNode.from_table) {
        tableAlias = processTableRef(selectNode.from_table, tableAlias);
      }

      selectNode.select_list.forEach((expr) =>
        processExpression(expr, tableAlias)
      );

      if (selectNode.where_clause) {
        processExpression(selectNode.where_clause, tableAlias);
      }

      selectNode.group_expressions.forEach((expr) =>
        processExpression(expr, tableAlias)
      );

      if (selectNode.having) {
        processExpression(selectNode.having, tableAlias);
      }

      if (selectNode.qualify) {
        processExpression(selectNode.qualify, tableAlias);
      }

      return tableAlias;
    } else if (node.type === QueryNodeType.SET_OPERATION_NODE) {
      const setOpNode = node as SetOperationNode;
      processQueryNode(setOpNode.left, parentTableAlias);
      processQueryNode(setOpNode.right, parentTableAlias);
      return parentTableAlias;
    }

    if ('modifiers' in node) {
      node.modifiers.forEach((modifier) => {
        if (modifier.type === ResultModifierType.ORDER_MODIFIER) {
          const orderModifier = modifier as OrderModifier;
          orderModifier.orders.forEach((order) =>
            processExpression(order.expression, parentTableAlias)
          );
        }
      });
    }

    return parentTableAlias;
  }

  processQueryNode(selectStatement.node, '');

  return referencedColumns;
}

export const sqlQueryToAST = async (
  sql: string
): Promise<Record<string, string[]>> => {
  const escapedSQL = sql.replace(/'/g, "''");
  const result = (await duckdbExec(
    `SELECT json_serialize_sql('${escapedSQL}') as ast;`
  )) as any;
  const astString = result[0]['ast'];
  //   console.log(astString);
  const astParsed = JSON.parse(astString);

  const statements = astParsed?.statements as SelectStatement[];

  const allReferencedColumns: Record<string, string[]> = {};
  statements.forEach((statement) => {
    const referencedColumns = getReferencedColumns(statement);
    referencedColumns.forEach((value, key) => {
      if (!allReferencedColumns[key]) {
        allReferencedColumns[key] = [];
      }
      allReferencedColumns[key].push(...value);
    });
  });

  return allReferencedColumns;
};