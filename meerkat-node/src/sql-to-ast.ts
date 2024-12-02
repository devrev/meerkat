import {
  BaseTypeTableRef,
  CaseExpression,
  ColumnRefExpression,
  CommonTableExpressionMap,
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

  processQueryNode(
    selectStatement.node,
    '',
    referencedColumns,
    aliasedColumns,
    availableSchema
  );

  return referencedColumns;
}

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
}

function processExpression(
  expr: ParsedExpression,
  currentTableAlias: string,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
) {
  if (!expr) return;

  switch (expr.type) {
    case ExpressionType.COLUMN_REF:
      processColumnRef(
        expr as ColumnRefExpression,
        currentTableAlias,
        referencedColumns,
        aliasedColumns,
        availableSchema
      );
      break;
    case ExpressionType.STAR:
      processStarExpression(
        expr as StarExpression,
        currentTableAlias,
        referencedColumns
      );
      break;
    case ExpressionType.SUBQUERY:
      processSubquery(
        expr as SubqueryExpression,
        currentTableAlias,
        referencedColumns,
        aliasedColumns,
        availableSchema
      );
      break;
    case ExpressionType.COMPARE_EQUAL:
    case ExpressionType.COMPARE_NOTEQUAL:
    case ExpressionType.COMPARE_LESSTHAN:
    case ExpressionType.COMPARE_GREATERTHAN:
    case ExpressionType.COMPARE_LESSTHANOREQUALTO:
    case ExpressionType.COMPARE_GREATERTHANOREQUALTO:
      processComparison(
        expr as ComparisonExpression,
        currentTableAlias,
        referencedColumns,
        aliasedColumns,
        availableSchema
      );
      break;
    case ExpressionType.CASE_EXPR:
      processCaseExpression(
        expr as CaseExpression,
        currentTableAlias,
        referencedColumns,
        aliasedColumns,
        availableSchema
      );
      break;
    case ExpressionType.FUNCTION:
      processFunctionExpression(
        expr as FunctionExpression,
        currentTableAlias,
        referencedColumns,
        aliasedColumns,
        availableSchema
      );
      break;
    default:
      if (isExpressionWithChildren(expr)) {
        processExpressionWithChildren(
          expr,
          currentTableAlias,
          referencedColumns,
          aliasedColumns,
          availableSchema
        );
      } else if (isExpressionWithChild(expr)) {
        processExpressionWithChild(
          expr,
          currentTableAlias,
          referencedColumns,
          aliasedColumns,
          availableSchema
        );
      }
  }
}

function processColumnRef(
  columnRef: ColumnRefExpression,
  currentTableAlias: string,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
) {
  const columnName = columnRef.column_names[columnRef.column_names.length - 1];

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
}

function processStarExpression(
  starExpr: StarExpression,
  currentTableAlias: string,
  referencedColumns: Map<string, Set<string>>
) {
  starExpr.exclude_list.forEach((excludedColumn) => {
    if (!referencedColumns.has(currentTableAlias)) {
      referencedColumns.set(currentTableAlias, new Set<string>());
    }
    referencedColumns.get(currentTableAlias)?.add(excludedColumn);
  });
}

function processSubquery(
  subqueryExpr: SubqueryExpression,
  currentTableAlias: string,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
) {
  processQueryNode(
    subqueryExpr.subquery.node,
    currentTableAlias,
    referencedColumns,
    aliasedColumns,
    availableSchema
  );
}

function processComparison(
  comparison: ComparisonExpression,
  currentTableAlias: string,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
) {
  processExpression(
    comparison.left,
    currentTableAlias,
    referencedColumns,
    aliasedColumns,
    availableSchema
  );
  processExpression(
    comparison.right,
    currentTableAlias,
    referencedColumns,
    aliasedColumns,
    availableSchema
  );
}

function processCaseExpression(
  caseExpr: CaseExpression,
  currentTableAlias: string,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
) {
  caseExpr.case_checks.forEach((check) => {
    processExpression(
      check.when_expr,
      currentTableAlias,
      referencedColumns,
      aliasedColumns,
      availableSchema
    );
    processExpression(
      check.then_expr,
      currentTableAlias,
      referencedColumns,
      aliasedColumns,
      availableSchema
    );
  });
  processExpression(
    caseExpr.else_expr,
    currentTableAlias,
    referencedColumns,
    aliasedColumns,
    availableSchema
  );
}

function processFunctionExpression(
  functionExpr: FunctionExpression,
  currentTableAlias: string,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
) {
  functionExpr.children.forEach((child) =>
    processExpression(
      child,
      currentTableAlias,
      referencedColumns,
      aliasedColumns,
      availableSchema
    )
  );
}

function processExpressionWithChildren(
  expr: ParsedExpression & { children: ParsedExpression[] },
  currentTableAlias: string,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
) {
  expr.children.forEach((child) =>
    processExpression(
      child,
      currentTableAlias,
      referencedColumns,
      aliasedColumns,
      availableSchema
    )
  );
}

function processExpressionWithChild(
  expr: ParsedExpression & { child: ParsedExpression },
  currentTableAlias: string,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
) {
  processExpression(
    expr.child,
    currentTableAlias,
    referencedColumns,
    aliasedColumns,
    availableSchema
  );
}

function processTableRef(
  tableRef: TableRef,
  parentTableAlias: string,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
): string {
  switch (tableRef.type) {
    case TableReferenceType.BASE_TABLE:
      return processBaseTableRef(
        tableRef as BaseTypeTableRef,
        referencedColumns,
        availableSchema
      );
    case TableReferenceType.JOIN:
      return processJoinRef(
        tableRef as JoinRef,
        parentTableAlias,
        referencedColumns,
        aliasedColumns,
        availableSchema
      );
    case TableReferenceType.SUBQUERY:
      return processSubqueryRef(
        tableRef as SubqueryRef,
        referencedColumns,
        aliasedColumns,
        availableSchema
      );
    default:
      return parentTableAlias;
  }
}

function processBaseTableRef(
  baseTableRef: BaseTypeTableRef,
  referencedColumns: Map<string, Set<string>>,
  availableSchema: Set<string>
): string {
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
}

function processJoinRef(
  joinRef: JoinRef,
  parentTableAlias: string,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
): string {
  const leftTableAlias = processTableRef(
    joinRef.left,
    parentTableAlias,
    referencedColumns,
    aliasedColumns,
    availableSchema
  );
  const rightTableAlias = processTableRef(
    joinRef.right,
    parentTableAlias,
    referencedColumns,
    aliasedColumns,
    availableSchema
  );

  if (joinRef.condition) {
    processExpression(
      joinRef.condition,
      leftTableAlias,
      referencedColumns,
      aliasedColumns,
      availableSchema
    );
    processExpression(
      joinRef.condition,
      rightTableAlias,
      referencedColumns,
      aliasedColumns,
      availableSchema
    );
  }

  return parentTableAlias;
}

function processSubqueryRef(
  subqueryRef: SubqueryRef,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
): string {
  const subqueryTableAlias = processQueryNode(
    subqueryRef.subquery.node,
    subqueryRef.alias || '',
    referencedColumns,
    aliasedColumns,
    availableSchema
  );
  return subqueryTableAlias;
}

function processQueryNode(
  node: QueryNode,
  parentTableAlias: string,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
): string {
  if (node.type === QueryNodeType.SELECT_NODE) {
    return processSelectNode(
      node as SelectNode,
      parentTableAlias,
      referencedColumns,
      aliasedColumns,
      availableSchema
    );
  } else if (node.type === QueryNodeType.SET_OPERATION_NODE) {
    return processSetOperationNode(
      node as SetOperationNode,
      parentTableAlias,
      referencedColumns,
      aliasedColumns,
      availableSchema
    );
  }

  processResultModifiers(
    node,
    parentTableAlias,
    referencedColumns,
    aliasedColumns,
    availableSchema
  );

  return parentTableAlias;
}

function processSelectNode(
  selectNode: SelectNode,
  parentTableAlias: string,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
): string {
  let tableAlias = parentTableAlias;

  processCteMap(
    selectNode.cte_map,
    referencedColumns,
    aliasedColumns,
    availableSchema
  );

  if (selectNode.from_table) {
    tableAlias = processTableRef(
      selectNode.from_table,
      tableAlias,
      referencedColumns,
      aliasedColumns,
      availableSchema
    );
  }

  selectNode.select_list.forEach((expr) =>
    processExpression(
      expr,
      tableAlias,
      referencedColumns,
      aliasedColumns,
      availableSchema
    )
  );

  if (selectNode.where_clause) {
    processExpression(
      selectNode.where_clause,
      tableAlias,
      referencedColumns,
      aliasedColumns,
      availableSchema
    );
  }

  selectNode.group_expressions.forEach((expr) =>
    processExpression(
      expr,
      tableAlias,
      referencedColumns,
      aliasedColumns,
      availableSchema
    )
  );

  if (selectNode.having) {
    processExpression(
      selectNode.having,
      tableAlias,
      referencedColumns,
      aliasedColumns,
      availableSchema
    );
  }

  if (selectNode.qualify) {
    processExpression(
      selectNode.qualify,
      tableAlias,
      referencedColumns,
      aliasedColumns,
      availableSchema
    );
  }

  return tableAlias;
}

function processSetOperationNode(
  setOpNode: SetOperationNode,
  parentTableAlias: string,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
): string {
  processQueryNode(
    setOpNode.left,
    parentTableAlias,
    referencedColumns,
    aliasedColumns,
    availableSchema
  );
  processQueryNode(
    setOpNode.right,
    parentTableAlias,
    referencedColumns,
    aliasedColumns,
    availableSchema
  );
  return parentTableAlias;
}

function processCteMap(
  cteMap: CommonTableExpressionMap | undefined,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
) {
  if (cteMap) {
    const map = cteMap.map;
    if (Array.isArray(map)) {
      map.forEach((cte) => {
        const cteAlias = cte.key;
        const cteQuery = cte.value.query;
        processQueryNode(
          cteQuery.node,
          cteAlias,
          referencedColumns,
          aliasedColumns,
          availableSchema
        );
      });
    }
  }
}

function processResultModifiers(
  node: QueryNode,
  parentTableAlias: string,
  referencedColumns: Map<string, Set<string>>,
  aliasedColumns: Map<string, Map<string, string>>,
  availableSchema: Set<string>
) {
  if ('modifiers' in node) {
    node.modifiers.forEach((modifier) => {
      if (modifier.type === ResultModifierType.ORDER_MODIFIER) {
        const orderModifier = modifier as OrderModifier;
        orderModifier.orders.forEach((order) =>
          processExpression(
            order.expression,
            parentTableAlias,
            referencedColumns,
            aliasedColumns,
            availableSchema
          )
        );
      }
    });
  }
}

export const getDatasetColumnsFromSQL = async (
  sql: string
): Promise<Record<string, string[]>> => {
  const escapedSQL = sql.replace(/'/g, "''");
  const result = (await duckdbExec(
    `SELECT json_serialize_sql('${escapedSQL}') as ast;`
  )) as any;
  const astString = result[0]['ast'];
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
