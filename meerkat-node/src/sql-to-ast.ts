import {
  BaseTypeTableRef,
  CaseExpression,
  ColumnRefExpression,
  ComparisonExpression,
  ExpressionType,
  FunctionExpression,
  JoinRef,
  OrderModifier,
  ParsedExpression,
  QueryNode,
  QueryNodeType,
  ResultModifierType,
  SelectNode,
  SelectStatement,
  SetOperationNode,
  StarExpression,
  SubqueryExpression,
  SubqueryRef,
  TableRef,
  TableReferenceType,
} from '@devrev/meerkat-core';
import { duckdbExec } from './duckdb-exec';

function getReferencedColumns(
  selectStatement: SelectStatement
): Map<string, Set<string>> {
  const referencedColumns = new Map<string, Set<string>>();
  const aliasedColumns = new Map<string, Map<string, string>>();
  const availableSchema = new Set<string>();

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

  function resolveAliasedColumn(
    tableAlias: string,
    columnName: string,
    aliasedColumns: Map<string, Map<string, string>>,
    referencedColumns: Map<string, Set<string>>
  ): void {
    if (!referencedColumns.has(tableAlias)) {
      referencedColumns.set(tableAlias, new Set<string>());
    }

    if (aliasedColumns.has(tableAlias)) {
      const tableAliases = aliasedColumns.get(tableAlias);
      if (tableAliases?.has(columnName)) {
        const aliasedColumn = tableAliases.get(columnName) as string;
        resolveAliasedColumn(
          tableAlias,
          aliasedColumn,
          aliasedColumns,
          referencedColumns
        );
      } else {
        referencedColumns.get(tableAlias)?.add(columnName);
      }
    } else {
      referencedColumns.get(tableAlias)?.add(columnName);
    }
  }

  function processExpression(
    expr: ParsedExpression,
    currentTableAlias: string
  ) {
    if (!expr) return;
    if (expr.type === ExpressionType.COLUMN_REF) {
      const columnRef = expr as ColumnRefExpression;
      const columnName =
        columnRef.column_names[columnRef.column_names.length - 1];

      if (columnName === 'is_verified') {
        console.log('is_verified', columnRef);
      }

      if (columnRef.alias && columnRef.alias !== columnName) {
        const tableAliasMap =
          aliasedColumns.get(currentTableAlias) || new Map<string, string>();
        tableAliasMap.set(columnRef.alias, columnName);
        aliasedColumns.set(currentTableAlias, tableAliasMap);
      }

      let tableAlias = currentTableAlias;
      if (columnRef.column_names.length > 1) {
        let found = false;
        const refTableAlias = columnRef.column_names[0];
        if (!referencedColumns.has(refTableAlias)) {
          for (const schema of availableSchema) {
            const fullTableName = schema + '.' + refTableAlias;
            if (referencedColumns.has(fullTableName)) {
              tableAlias = fullTableName;
              found = true;
              break;
            }
          }
        }
        if (!found) {
          return;
        }
      }

      if (!referencedColumns.has(tableAlias)) {
        referencedColumns.set(tableAlias, new Set<string>());
      }

      resolveAliasedColumn(
        tableAlias,
        columnName,
        aliasedColumns,
        referencedColumns
      );
    } else if (expr.type === ExpressionType.STAR) {
      const starExpr = expr as StarExpression;
      starExpr.exclude_list.forEach((excludedColumn) => {
        if (!referencedColumns.has(currentTableAlias)) {
          referencedColumns.set(currentTableAlias, new Set<string>());
        }
        referencedColumns.get(currentTableAlias)?.add(excludedColumn);
      });
    } else if (expr.type === ExpressionType.SUBQUERY) {
      const subqueryExpr = expr as SubqueryExpression;
      processQueryNode(subqueryExpr.subquery.node, currentTableAlias);
    } else if (
      expr.type === ExpressionType.COMPARE_EQUAL ||
      expr.type === ExpressionType.COMPARE_NOTEQUAL ||
      expr.type === ExpressionType.COMPARE_LESSTHAN ||
      expr.type === ExpressionType.COMPARE_GREATERTHAN ||
      expr.type === ExpressionType.COMPARE_LESSTHANOREQUALTO ||
      expr.type === ExpressionType.COMPARE_GREATERTHANOREQUALTO
    ) {
      const comparison = expr as ComparisonExpression;
      processExpression(comparison.left, currentTableAlias);
      processExpression(comparison.right, currentTableAlias);
    } else if (expr.type === ExpressionType.CASE_EXPR) {
      const caseExpr = expr as CaseExpression;
      caseExpr.case_checks.forEach((check) => {
        processExpression(check.when_expr, currentTableAlias);
        processExpression(check.then_expr, currentTableAlias);
      });
      processExpression(caseExpr.else_expr, currentTableAlias);
    } else if (expr.type === ExpressionType.FUNCTION) {
      const functionExpr = expr as FunctionExpression;
      functionExpr.children.forEach((child) =>
        processExpression(child, currentTableAlias)
      );
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
      if (baseTableRef.schema_name) {
        availableSchema.add(baseTableRef.schema_name);
      }
      if (!referencedColumns.has(tableName)) {
        referencedColumns.set(tableName, new Set<string>());
      }
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

      if (selectNode.cte_map) {
        const cteMap = selectNode.cte_map.map;
        if (Array.isArray(cteMap)) {
          cteMap.forEach((cte) => {
            const cteAlias = cte.key;
            const cteQuery = cte.value.query;
            processQueryNode(cteQuery.node, cteAlias);
          });
        }
      }

      if (selectNode.from_table) {
        tableAlias = processTableRef(selectNode.from_table, tableAlias);
      }

      selectNode.select_list.forEach((expr) => {
        return processExpression(expr, tableAlias);
      });

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
  console.log(astString);
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
