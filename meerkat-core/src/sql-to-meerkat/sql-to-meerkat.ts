import {
  astDeserializerQuery,
  deserializeQuery,
} from '../ast-deserializer/ast-deserializer';
import { astSerializerQuery } from '../ast-serializer/ast-serializer';
import {
  LogicalAndFilter,
  LogicalOrFilter,
  MeerkatQueryFilter,
  Query,
  QueryFilterWithValues,
  QueryOrderType,
} from '../types/cube-types/query';
import { Dimension, Measure, TableSchema } from '../types/cube-types/table';
import {
  BetweenExpression,
  ColumnRefExpression,
  ComparisonExpression,
  ConjunctionExpression,
  ConstantExpression,
  ExpressionClass,
  ExpressionType,
  FunctionExpression,
  LimitModifier,
  OperatorExpression,
  OrderByNode,
  OrderModifier,
  OrderType,
  ParsedExpression,
  PositionalReferenceExpression,
  QueryNodeType,
  ResultModifierType,
  SelectNode,
} from '../types/duckdb-serialization-types';
import {
  GetQueryOutput,
  serializeExpressions,
} from '../utils/duckdb-ast-parse-serialize';
import { DecomposeOutput } from './types';

export interface SqlToMeerkatInput {
  sql: string;
  getQueryOutput: GetQueryOutput;
}

const AGGREGATE_FUNCTIONS = new Set([
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

export async function sqlToMeerkat(
  input: SqlToMeerkatInput
): Promise<DecomposeOutput> {
  const { sql, getQueryOutput } = input;
  const warnings: string[] = [];

  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- DuckDB JSON AST is untyped at top level
  let parsedAst: any;
  try {
    const serializeQuery = astSerializerQuery(sanitizeForSerialize(sql));
    const rows = await getQueryOutput(serializeQuery);
    const jsonStr = deserializeQuery(rows);
    parsedAst = JSON.parse(jsonStr);
  } catch (e) {
    return {
      success: false,
      reason: `DuckDB parse failed: ${(e as Error).message}`,
    };
  }

  if (parsedAst?.error) {
    return {
      success: false,
      reason: `DuckDB parse failed: ${
        parsedAst.error_message || 'unknown error'
      }`,
    };
  }

  const statement = parsedAst?.statements?.[0];
  if (!statement?.node) {
    return { success: false, reason: 'No statement found in parsed AST' };
  }

  const node = statement.node;

  if (node.type === QueryNodeType.SET_OPERATION_NODE) {
    return { success: false, reason: 'UNION/INTERSECT/EXCEPT not supported' };
  }
  if (node.type !== QueryNodeType.SELECT_NODE) {
    return { success: false, reason: `Unsupported query type: ${node.type}` };
  }

  if (hasRecursiveCteInMap(node)) {
    return { success: false, reason: 'WITH RECURSIVE not supported' };
  }

  const selectNode = node as SelectNode;
  const tableName = extractTableName(selectNode);

  const measures: Measure[] = [];
  const dimensions: Dimension[] = [];
  const queryMeasures: string[] = [];
  const queryDimensions: string[] = [];
  const usedNames = new Set<string>();

  const hasAggregates = selectNode.select_list.some(isAggregateExpr);
  const hasGroupBy = selectNode.group_expressions.length > 0;

  for (const expr of selectNode.select_list) {
    if (isWindowExpr(expr)) {
      warnings.push(
        `Skipped window function: ${expr.alias || exprToName(expr)}`
      );
      continue;
    }

    if (isNestedAggregateExpr(expr)) {
      warnings.push(
        `Skipped nested aggregation: ${expr.alias || exprToName(expr)}`
      );
      continue;
    }

    if (hasAggregates || hasGroupBy) {
      if (isAggregateExpr(expr)) {
        const name = deduplicateName(
          expr.alias || generateAggregateName(expr),
          usedNames
        );
        const measureSql = await exprToSql(expr, getQueryOutput);
        measures.push({ name, sql: measureSql, type: 'number' });
        queryMeasures.push(`${tableName}.${name}`);
      } else {
        const name = deduplicateName(expr.alias || exprToName(expr), usedNames);
        const dimSql = await exprToSql(expr, getQueryOutput);
        const dimType = inferTypeFromExpr(expr);
        dimensions.push({ name, sql: dimSql, type: dimType });
        queryDimensions.push(`${tableName}.${name}`);
      }
    } else {
      if (isStarExpr(expr)) continue;
      const name = deduplicateName(expr.alias || exprToName(expr), usedNames);
      const dimSql = await exprToSql(expr, getQueryOutput);
      const dimType = inferTypeFromExpr(expr);
      dimensions.push({ name, sql: dimSql, type: dimType });
      queryDimensions.push(`${tableName}.${name}`);
    }
  }

  const hasStar = selectNode.select_list.some(isStarExpr);
  if (measures.length === 0 && dimensions.length === 0 && !hasStar) {
    return {
      success: false,
      reason: 'No extractable columns after filtering incompatible expressions',
    };
  }

  // Extract WHERE filters, separating extractable from non-extractable
  let residualWhere: ParsedExpression | undefined;
  const allFilters: (QueryFilterWithValues | LogicalOrFilter)[] = [];
  if (selectNode.where_clause) {
    const extracted = extractFiltersFromAst(selectNode.where_clause, tableName);
    allFilters.push(...extracted.filters);
    residualWhere = extracted.residual;
    if (extracted.warnings.length > 0) {
      warnings.push(...extracted.warnings);
    }
    for (const f of extracted.filters) {
      if ('member' in f) {
        const newDims = ensureFilterColumnInSchema(
          f as QueryFilterWithValues,
          dimensions,
          tableName
        );
        if (newDims) dimensions.push(...newDims);
      } else if ('or' in f) {
        const newDims = ensureOrFilterColumnsInSchema(
          f as LogicalOrFilter,
          dimensions,
          tableName
        );
        if (newDims) dimensions.push(...newDims);
      }
    }
  }

  // Extract HAVING as measure filters
  if (selectNode.having) {
    const havingFilters = extractHavingFromAst(
      selectNode.having,
      tableName,
      measures
    );
    allFilters.push(...havingFilters);
  }

  const queryFilters: MeerkatQueryFilter[] = wrapFilters(allFilters);

  // Build base SQL using AST (keeps residual WHERE conditions)
  const baseSQL = await buildBaseSQL(
    sql,
    selectNode,
    hasAggregates || hasGroupBy,
    residualWhere,
    getQueryOutput
  );

  // Extract ORDER BY
  let order: Record<string, QueryOrderType> | undefined;
  const orderModifier = selectNode.modifiers.find(
    (m) => m.type === ResultModifierType.ORDER_MODIFIER
  ) as OrderModifier | undefined;
  if (orderModifier) {
    order = extractOrderFromAst(
      orderModifier.orders,
      tableName,
      dimensions,
      measures
    );
  }

  // Extract LIMIT/OFFSET
  let limit: number | undefined;
  let offset: number | undefined;
  const limitModifier = selectNode.modifiers.find(
    (m) => m.type === ResultModifierType.LIMIT_MODIFIER
  ) as LimitModifier | undefined;
  if (limitModifier) {
    if (limitModifier.limit?.class === ExpressionClass.CONSTANT) {
      const lConst = limitModifier.limit as ConstantExpression;
      limit = Number((lConst.value as { value?: string | number })?.value);
    }
    if (limitModifier.offset?.class === ExpressionClass.CONSTANT) {
      const oConst = limitModifier.offset as ConstantExpression;
      offset = Number((oConst.value as { value?: string | number })?.value);
    }
  }

  const tableSchema: TableSchema = {
    name: tableName,
    sql: baseSQL,
    measures,
    dimensions,
  };

  const query: Query = {
    measures: queryMeasures,
    dimensions: queryDimensions.length > 0 ? queryDimensions : undefined,
    filters: queryFilters.length > 0 ? queryFilters : undefined,
    order,
    limit,
    offset,
  };

  return { success: true, tableSchema, query, warnings };
}

function extractTableName(selectNode: SelectNode): string {
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
function hasRecursiveCteInMap(node: any): boolean {
  const cteMap = node.cte_map?.map;
  if (!Array.isArray(cteMap)) return false;
  return cteMap.some(
    (entry: any) =>
      entry.value?.query?.node?.type === QueryNodeType.RECURSIVE_CTE_NODE
  );
}

function isAggregateExpr(expr: ParsedExpression): boolean {
  if (expr.class === ExpressionClass.FUNCTION) {
    const fn = expr as FunctionExpression;
    return AGGREGATE_FUNCTIONS.has(fn.function_name.toLowerCase());
  }
  return false;
}

function isWindowExpr(expr: ParsedExpression): boolean {
  return expr.class === ExpressionClass.WINDOW;
}

function isStarExpr(expr: ParsedExpression): boolean {
  return expr.class === ExpressionClass.STAR;
}

function isNestedAggregateExpr(expr: ParsedExpression): boolean {
  if (!isAggregateExpr(expr)) return false;
  const fn = expr as FunctionExpression;
  return fn.children.some(isAggregateExpr);
}

function exprToName(expr: ParsedExpression): string {
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

function generateAggregateName(expr: ParsedExpression): string {
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

function deduplicateName(name: string, existing: Set<string>): string {
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

function inferTypeFromExpr(expr: ParsedExpression): Dimension['type'] {
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
      colName === 'created_at' ||
      colName === 'updated_at'
    ) {
      return 'time';
    }
  }
  return 'string';
}

async function exprToSql(
  expr: ParsedExpression,
  getQueryOutput: GetQueryOutput
): Promise<string> {
  const copy = JSON.parse(JSON.stringify(expr));
  copy.alias = '';
  stripQueryLocationInPlace(copy);
  const results = await serializeExpressions([copy], getQueryOutput);
  return results[0];
}

async function buildBaseSQL(
  originalSql: string,
  selectNode: SelectNode,
  hadAggregation: boolean,
  residualWhere: ParsedExpression | undefined,
  getQueryOutput: GetQueryOutput
): Promise<string> {
  const fromOnlyNode = {
    ...selectNode,
    select_list: [
      {
        class: ExpressionClass.STAR,
        type: ExpressionType.COLUMN_REF,
        alias: '',
        relation_name: '',
        exclude_list: [],
        replace_list: [],
        columns: false,
      },
    ],
    where_clause: hadAggregation || residualWhere ? residualWhere : undefined,
    group_expressions: [],
    group_sets: [],
    having: null,
    qualify: null,
    modifiers: [],
  };

  stripQueryLocationInPlace(fromOnlyNode);

  try {
    const query = astDeserializerQuery({ node: fromOnlyNode } as never);
    const rows = await getQueryOutput(query);
    const baseSql = deserializeQuery(rows).replace(/;\s*$/, '');
    return baseSql;
  } catch {
    return `SELECT * FROM (${originalSql.replace(/'/g, "''")}) AS _base`;
  }
}

interface FilterExtractionResult {
  filters: (QueryFilterWithValues | LogicalOrFilter)[];
  residual: ParsedExpression | undefined;
  warnings: string[];
}

function extractFiltersFromAst(
  whereExpr: ParsedExpression,
  tableName: string
): FilterExtractionResult {
  const filters: (QueryFilterWithValues | LogicalOrFilter)[] = [];
  const residualParts: ParsedExpression[] = [];
  const warnings: string[] = [];

  if (whereExpr.class === ExpressionClass.CONJUNCTION) {
    const conj = whereExpr as ConjunctionExpression;
    if (whereExpr.type === ExpressionType.CONJUNCTION_AND) {
      for (const child of conj.children) {
        const extracted = tryExtractSingleFilter(child, tableName);
        if (extracted) {
          filters.push(extracted);
        } else {
          residualParts.push(child);
          warnings.push('Non-extractable WHERE condition retained in base SQL');
        }
      }
    } else if (whereExpr.type === ExpressionType.CONJUNCTION_OR) {
      const orFilter = extractOrFilter(conj, tableName);
      if (orFilter) {
        filters.push(orFilter);
      } else {
        residualParts.push(whereExpr);
        warnings.push(
          'OR condition partially non-extractable, retained in base SQL'
        );
      }
    }
  } else {
    const extracted = tryExtractSingleFilter(whereExpr, tableName);
    if (extracted) {
      filters.push(extracted);
    } else {
      residualParts.push(whereExpr);
      warnings.push('Non-extractable WHERE condition retained in base SQL');
    }
  }

  const residual = buildResidualConjunction(residualParts);
  return { filters, residual, warnings };
}

function buildResidualConjunction(
  parts: ParsedExpression[]
): ParsedExpression | undefined {
  if (parts.length === 0) return undefined;
  if (parts.length === 1) return parts[0];
  return {
    class: ExpressionClass.CONJUNCTION,
    type: ExpressionType.CONJUNCTION_AND,
    alias: '',
    children: parts,
  } as ConjunctionExpression;
}

function extractOrFilter(
  conj: ConjunctionExpression,
  tableName: string
): LogicalOrFilter | null {
  const orChildren: (QueryFilterWithValues | LogicalAndFilter)[] = [];
  for (const child of conj.children) {
    if (
      child.class === ExpressionClass.CONJUNCTION &&
      child.type === ExpressionType.CONJUNCTION_AND
    ) {
      const andChildren: QueryFilterWithValues[] = [];
      const andConj = child as ConjunctionExpression;
      let allExtracted = true;
      for (const grandchild of andConj.children) {
        const extracted = tryExtractSingleFilter(grandchild, tableName);
        if (extracted) {
          andChildren.push(extracted);
        } else {
          allExtracted = false;
          break;
        }
      }
      if (!allExtracted) return null;
      orChildren.push({ and: andChildren });
    } else {
      const extracted = tryExtractSingleFilter(child, tableName);
      if (!extracted) return null;
      orChildren.push(extracted);
    }
  }
  return { or: orChildren };
}

function tryExtractSingleFilter(
  expr: ParsedExpression,
  tableName: string
): QueryFilterWithValues | null {
  if (expr.class === ExpressionClass.COMPARISON) {
    return extractComparisonFilter(expr as ComparisonExpression, tableName);
  }
  if (expr.class === ExpressionClass.BETWEEN) {
    return extractBetweenFilter(expr as BetweenExpression, tableName);
  }
  if (expr.class === ExpressionClass.OPERATOR) {
    return extractOperatorFilter(expr as OperatorExpression, tableName);
  }
  if (expr.class === ExpressionClass.FUNCTION) {
    return extractFunctionFilter(expr as FunctionExpression, tableName);
  }
  return null;
}

function extractComparisonFilter(
  expr: ComparisonExpression,
  tableName: string
): QueryFilterWithValues | null {
  const colName = getColumnName(expr.left);
  if (!colName) return null;

  const value = getConstantValue(expr.right);
  if (value === null && expr.right.class !== ExpressionClass.CONSTANT)
    return null;

  const opMap: Record<string, QueryFilterWithValues['operator']> = {
    [ExpressionType.COMPARE_EQUAL]: 'equals',
    [ExpressionType.COMPARE_NOTEQUAL]: 'notEquals',
    [ExpressionType.COMPARE_GREATERTHAN]: 'gt',
    [ExpressionType.COMPARE_GREATERTHANOREQUALTO]: 'gte',
    [ExpressionType.COMPARE_LESSTHAN]: 'lt',
    [ExpressionType.COMPARE_LESSTHANOREQUALTO]: 'lte',
  };

  const operator = opMap[expr.type];
  if (!operator) return null;

  return {
    member: `${tableName}.${colName}`,
    operator,
    values: value !== null ? [String(value)] : [],
  };
}

function extractBetweenFilter(
  expr: BetweenExpression,
  tableName: string
): QueryFilterWithValues | null {
  const colName = getColumnName(expr.input);
  if (!colName) return null;

  const lower = getConstantValue(expr.lower);
  const upper = getConstantValue(expr.upper);
  if (lower === null || upper === null) return null;

  const isNumeric = typeof lower === 'number' && typeof upper === 'number';
  if (isNumeric) {
    // Numeric BETWEEN: decompose into gte + lte via AND
    // Return as gte (caller should handle both bounds, but we pick gte for lower)
    // Actually, return inDateRange only for date-like columns, otherwise use gte
    const colNameLower = colName.toLowerCase();
    const looksLikeDate =
      colNameLower.endsWith('_at') ||
      colNameLower.endsWith('_date') ||
      colNameLower.includes('time') ||
      colNameLower === 'created_at' ||
      colNameLower === 'updated_at';
    if (!looksLikeDate) {
      return {
        member: `${tableName}.${colName}`,
        operator: 'gte',
        values: [String(lower), String(upper)],
      };
    }
  }

  return {
    member: `${tableName}.${colName}`,
    operator: 'inDateRange',
    values: [String(lower), String(upper)],
  };
}

function extractOperatorFilter(
  expr: OperatorExpression,
  tableName: string
): QueryFilterWithValues | null {
  // IS NULL / IS NOT NULL
  if (
    expr.type === ExpressionType.OPERATOR_IS_NULL &&
    expr.children?.length === 1
  ) {
    const colName = getColumnName(expr.children[0]);
    if (!colName) return null;
    return {
      member: `${tableName}.${colName}`,
      operator: 'notSet',
      values: [],
    };
  }
  if (
    expr.type === ExpressionType.OPERATOR_IS_NOT_NULL &&
    expr.children?.length === 1
  ) {
    const colName = getColumnName(expr.children[0]);
    if (!colName) return null;
    return { member: `${tableName}.${colName}`, operator: 'set', values: [] };
  }

  // IN: children = [column, value1, value2, ...]
  if (expr.type === ExpressionType.COMPARE_IN && expr.children?.length >= 2) {
    const colName = getColumnName(expr.children[0]);
    if (!colName) return null;
    const values: string[] = [];
    for (let i = 1; i < expr.children.length; i++) {
      const val = getConstantValue(expr.children[i]);
      if (val === null) return null;
      values.push(String(val));
    }
    return { member: `${tableName}.${colName}`, operator: 'in', values };
  }

  // NOT IN
  if (
    expr.type === ExpressionType.COMPARE_NOT_IN &&
    expr.children?.length >= 2
  ) {
    const colName = getColumnName(expr.children[0]);
    if (!colName) return null;
    const values: string[] = [];
    for (let i = 1; i < expr.children.length; i++) {
      const val = getConstantValue(expr.children[i]);
      if (val === null) return null;
      values.push(String(val));
    }
    return { member: `${tableName}.${colName}`, operator: 'notIn', values };
  }

  return null;
}

function extractFunctionFilter(
  expr: FunctionExpression,
  tableName: string
): QueryFilterWithValues | null {
  const fnName = expr.function_name.toLowerCase();

  // LIKE (~~) / ILIKE (~~*) → contains
  if (fnName === '~~' || fnName === '~~*') {
    if (expr.children.length >= 2) {
      const colName = getColumnName(expr.children[0]);
      if (!colName) return null;
      const patternVal = getConstantValue(expr.children[1]);
      if (patternVal === null) return null;
      const pattern = String(patternVal);
      // Only extract simple %value% patterns as "contains"
      if (
        pattern.startsWith('%') &&
        pattern.endsWith('%') &&
        pattern.length > 2
      ) {
        const inner = pattern.slice(1, -1);
        if (!inner.includes('%') && !inner.includes('_')) {
          return {
            member: `${tableName}.${colName}`,
            operator: 'contains',
            values: [inner],
          };
        }
      }
    }
    return null;
  }

  // NOT LIKE (!~~) / NOT ILIKE (!~~*) → notContains
  if (fnName === '!~~' || fnName === '!~~*') {
    if (expr.children.length >= 2) {
      const colName = getColumnName(expr.children[0]);
      if (!colName) return null;
      const patternVal = getConstantValue(expr.children[1]);
      if (patternVal === null) return null;
      const pattern = String(patternVal);
      if (
        pattern.startsWith('%') &&
        pattern.endsWith('%') &&
        pattern.length > 2
      ) {
        const inner = pattern.slice(1, -1);
        if (!inner.includes('%') && !inner.includes('_')) {
          return {
            member: `${tableName}.${colName}`,
            operator: 'notContains',
            values: [inner],
          };
        }
      }
    }
    return null;
  }

  return null;
}

function extractHavingFromAst(
  havingExpr: ParsedExpression,
  tableName: string,
  measures: readonly Measure[]
): QueryFilterWithValues[] {
  const filters: QueryFilterWithValues[] = [];

  if (havingExpr.class === ExpressionClass.COMPARISON) {
    const comp = havingExpr as ComparisonExpression;
    const measure = matchMeasureFromExpr(comp.left, measures);
    if (measure) {
      const value = getConstantValue(comp.right);
      const opMap: Record<string, QueryFilterWithValues['operator']> = {
        [ExpressionType.COMPARE_GREATERTHAN]: 'gt',
        [ExpressionType.COMPARE_GREATERTHANOREQUALTO]: 'gte',
        [ExpressionType.COMPARE_LESSTHAN]: 'lt',
        [ExpressionType.COMPARE_LESSTHANOREQUALTO]: 'lte',
        [ExpressionType.COMPARE_EQUAL]: 'equals',
        [ExpressionType.COMPARE_NOTEQUAL]: 'notEquals',
      };
      const op = opMap[comp.type];
      if (op && value !== null) {
        filters.push({
          member: `${tableName}.${measure.name}`,
          operator: op,
          values: [String(value)],
        });
      }
    }
  } else if (havingExpr.class === ExpressionClass.CONJUNCTION) {
    const conj = havingExpr as ConjunctionExpression;
    for (const child of conj.children) {
      filters.push(...extractHavingFromAst(child, tableName, measures));
    }
  }

  return filters;
}

function matchMeasureFromExpr(
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

function extractOrderFromAst(
  orders: OrderByNode[],
  tableName: string,
  dimensions: readonly Dimension[],
  measures: readonly Measure[]
): Record<string, QueryOrderType> {
  const result: Record<string, QueryOrderType> = {};

  for (const orderEntry of orders) {
    const expr = orderEntry.expression;
    const direction: QueryOrderType =
      orderEntry.type === OrderType.DESCENDING ? 'desc' : 'asc';

    if (expr?.class === ExpressionClass.COLUMN_REF) {
      const colName = getColumnName(expr);
      if (colName) {
        const measure = measures.find((m) => m.name === colName);
        const dimension = dimensions.find((d) => d.name === colName);
        const memberName = measure?.name || dimension?.name || colName;
        result[`${tableName}.${memberName}`] = direction;
      }
    } else if (expr?.class === ExpressionClass.FUNCTION) {
      const measure = matchMeasureFromExpr(expr, measures);
      if (measure) {
        result[`${tableName}.${measure.name}`] = direction;
      }
    } else if (expr?.class === ExpressionClass.POSITIONAL_REFERENCE) {
      // ORDER BY 1, ORDER BY 2, etc.
      const posExpr = expr as PositionalReferenceExpression;
      const idx = posExpr.index - 1;
      const allMembers = [
        ...dimensions.map((d) => d.name),
        ...measures.map((m) => m.name),
      ];
      if (idx >= 0 && idx < allMembers.length) {
        result[`${tableName}.${allMembers[idx]}`] = direction;
      }
    }
  }

  return result;
}

function getColumnName(expr: ParsedExpression): string | null {
  if (expr.class !== ExpressionClass.COLUMN_REF) return null;
  const col = expr as ColumnRefExpression;
  return col.column_names[col.column_names.length - 1];
}

function getConstantValue(expr: ParsedExpression): string | number | null {
  if (expr.class !== ExpressionClass.CONSTANT) return null;
  const constant = expr as ConstantExpression;
  const val = constant.value as
    | { is_null?: boolean; value?: string | number }
    | undefined;
  if (val?.is_null) return null;
  return val?.value ?? null;
}

function ensureFilterColumnInSchema(
  filter: QueryFilterWithValues,
  dimensions: readonly Dimension[],
  tableName: string
): Dimension[] | null {
  const colName = filter.member.replace(`${tableName}.`, '');
  const exists = dimensions.some(
    (d) => d.name === colName || d.sql === colName
  );
  if (!exists) {
    return [{ name: colName, sql: colName, type: 'string' }];
  }
  return null;
}

function ensureOrFilterColumnsInSchema(
  orFilter: LogicalOrFilter,
  dimensions: readonly Dimension[],
  tableName: string
): Dimension[] | null {
  const newDims: Dimension[] = [];
  for (const child of orFilter.or) {
    if ('member' in child) {
      const added = ensureFilterColumnInSchema(
        child as QueryFilterWithValues,
        [...dimensions, ...newDims],
        tableName
      );
      if (added) newDims.push(...added);
    } else if ('and' in child) {
      for (const grandchild of (child as LogicalAndFilter).and) {
        if ('member' in grandchild) {
          const added = ensureFilterColumnInSchema(
            grandchild as QueryFilterWithValues,
            [...dimensions, ...newDims],
            tableName
          );
          if (added) newDims.push(...added);
        }
      }
    }
  }
  return newDims.length > 0 ? newDims : null;
}

function wrapFilters(
  filters: (QueryFilterWithValues | LogicalOrFilter)[]
): MeerkatQueryFilter[] {
  if (filters.length === 0) return [];
  if (filters.length === 1) return [filters[0]];
  const andFilter: LogicalAndFilter = { and: filters };
  return [andFilter];
}

function sanitizeForSerialize(sql: string): string {
  return sql.replace(/'/g, "''");
}

function stripQueryLocationInPlace(root: unknown): void {
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
