import {
  LogicalAndFilter,
  LogicalOrFilter,
  MeerkatQueryFilter,
  Query,
  QueryFilter,
  QueryOrderType,
} from '../types/cube-types/query';
import { Dimension, Measure, TableSchema } from '../types/cube-types/table';
import {
  ExpressionClass,
  ExpressionType,
  ParsedExpression,
  FunctionExpression,
  ColumnRefExpression,
  ComparisonExpression,
  ConjunctionExpression,
  ConstantExpression,
  BetweenExpression,
  StarExpression,
  QueryNodeType,
  SelectNode,
  ResultModifierType,
} from '../types/duckdb-serialization-types';
import { astSerializerQuery } from '../ast-serializer/ast-serializer';
import { astDeserializerQuery, deserializeQuery } from '../ast-deserializer/ast-deserializer';
import {
  GetQueryOutput,
  serializeExpressions,
} from '../utils/duckdb-ast-parse-serialize';
import { DecomposeOutput, ExtractedFilter } from './types';

export interface SqlToMeerkatInput {
  sql: string;
  getQueryOutput: GetQueryOutput;
}

const AGGREGATE_FUNCTIONS = new Set([
  'count', 'count_star', 'sum', 'avg', 'min', 'max', 'stddev', 'stddev_pop',
  'stddev_samp', 'variance', 'var_pop', 'var_samp', 'median',
  'percentile_cont', 'percentile_disc', 'approx_count_distinct',
  'list', 'array_agg', 'string_agg', 'group_concat', 'listagg',
  'first', 'last', 'any_value', 'bit_and', 'bit_or', 'bit_xor',
  'bool_and', 'bool_or',
]);

export async function sqlToMeerkat(input: SqlToMeerkatInput): Promise<DecomposeOutput> {
  const { sql, getQueryOutput } = input;
  const warnings: string[] = [];

  let parsedAst: any;
  try {
    const serializeQuery = astSerializerQuery(sanitizeForSerialize(sql));
    const rows = await getQueryOutput(serializeQuery);
    const jsonStr = deserializeQuery(rows);
    parsedAst = JSON.parse(jsonStr);
  } catch (e) {
    return { success: false, reason: `DuckDB parse failed: ${(e as Error).message}` };
  }

  if (parsedAst?.error) {
    return { success: false, reason: `DuckDB parse failed: ${parsedAst.error_message || 'unknown error'}` };
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

  // Check for WITH RECURSIVE inside cte_map
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
      warnings.push(`Skipped window function: ${expr.alias || exprToName(expr)}`);
      continue;
    }

    if (isNestedAggregateExpr(expr)) {
      warnings.push(`Skipped nested aggregation: ${expr.alias || exprToName(expr)}`);
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
        const name = deduplicateName(
          expr.alias || exprToName(expr),
          usedNames
        );
        const dimSql = await exprToSql(expr, getQueryOutput);
        const dimType = inferTypeFromExpr(expr);
        dimensions.push({ name, sql: dimSql, type: dimType });
        queryDimensions.push(`${tableName}.${name}`);
      }
    } else {
      if (isStarExpr(expr)) continue;
      const name = deduplicateName(
        expr.alias || exprToName(expr),
        usedNames
      );
      const dimSql = await exprToSql(expr, getQueryOutput);
      const dimType = inferTypeFromExpr(expr);
      dimensions.push({ name, sql: dimSql, type: dimType });
      queryDimensions.push(`${tableName}.${name}`);
    }
  }

  const hasStar = selectNode.select_list.some(isStarExpr);
  if (measures.length === 0 && dimensions.length === 0 && !hasStar) {
    return { success: false, reason: 'No extractable columns after filtering incompatible expressions' };
  }

  // Build base SQL: original SQL stripped of GROUP BY, aggregates, ORDER BY, HAVING, LIMIT
  // We reconstruct by asking DuckDB to deserialize a modified AST
  const baseSQL = await buildBaseSQL(sql, selectNode, hasAggregates || hasGroupBy, getQueryOutput);

  // Extract WHERE filters
  const allFilters: (QueryFilter | LogicalOrFilter)[] = [];
  if (selectNode.where_clause) {
    const extracted = extractFiltersFromAst(selectNode.where_clause, tableName);
    allFilters.push(...extracted.filters);
    if (extracted.warnings.length > 0) {
      warnings.push(...extracted.warnings);
    }
    for (const f of extracted.filters) {
      if ('member' in f) {
        ensureFilterColumnInSchema(f as QueryFilter, dimensions, tableName);
      } else if ('or' in f) {
        ensureOrFilterColumnsInSchema(f as LogicalOrFilter, dimensions, tableName);
      }
    }
  }

  // Extract HAVING as measure filters
  if (selectNode.having) {
    const havingFilters = extractHavingFromAst(selectNode.having, tableName, measures);
    allFilters.push(...havingFilters);
  }

  const queryFilters: MeerkatQueryFilter[] = wrapFilters(allFilters);

  // Extract ORDER BY
  let order: Record<string, QueryOrderType> | undefined;
  const orderModifier = selectNode.modifiers.find(
    (m) => m.type === ResultModifierType.ORDER_MODIFIER
  );
  if (orderModifier && 'orders' in orderModifier) {
    order = extractOrderFromAst(
      (orderModifier as any).orders,
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
  );
  if (limitModifier && 'limit' in limitModifier) {
    const lm = limitModifier as any;
    if (lm.limit?.class === ExpressionClass.CONSTANT) {
      limit = Number(lm.limit.value?.value);
    }
    if (lm.offset?.class === ExpressionClass.CONSTANT) {
      offset = Number(lm.offset.value?.value);
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

function hasRecursiveCteInMap(node: any): boolean {
  const cteMap = node.cte_map?.map;
  if (!Array.isArray(cteMap)) return false;
  return cteMap.some(
    (entry: any) => entry.value?.query?.node?.type === QueryNodeType.RECURSIVE_CTE_NODE
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

  if (fnName === 'count_star' || fn.children.length === 0 || isStarExpr(fn.children[0])) {
    return 'count';
  }

  const childName = exprToName(fn.children[0]);
  return `${fnName}_${childName}`;
}

function deduplicateName(name: string, existing: Set<string>): string {
  const clean = name.replace(/[^a-zA-Z0-9_]/g, '_').replace(/^_+|_+$/g, '') || 'col';
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
    const cast = expr as any;
    const typeName = cast.cast_type?.id?.toLowerCase() || '';
    if (typeName.includes('timestamp') || typeName.includes('date')) {
      return 'time';
    }
  }
  if (expr.class === ExpressionClass.COLUMN_REF) {
    const col = expr as ColumnRefExpression;
    const colName = col.column_names[col.column_names.length - 1].toLowerCase();
    if (colName.endsWith('_at') || colName.endsWith('_date') ||
        colName === 'created_at' || colName === 'updated_at') {
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
  _selectNode: SelectNode,
  hadAggregation: boolean,
  getQueryOutput: GetQueryOutput
): Promise<string> {
  if (!hadAggregation) {
    // No aggregation — base SQL is the original without WHERE/ORDER/LIMIT
    // For simplicity, use SELECT * FROM (original_select_items) pattern
    return originalSql.replace(/\s*(WHERE|ORDER\s+BY|LIMIT|OFFSET)\s+.*/is, '').trim();
  }

  // For aggregated queries: SELECT * FROM <from_clause>
  // Use DuckDB to deserialize a modified AST with just FROM
  const fromOnlyNode = {
    ..._selectNode,
    select_list: [{
      class: ExpressionClass.STAR,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      relation_name: '',
      exclude_list: [],
      replace_list: [],
      columns: false,
    }],
    where_clause: undefined,
    group_expressions: [],
    group_sets: [],
    having: null,
    qualify: null,
    modifiers: [],
  };

  stripQueryLocationInPlace(fromOnlyNode);

  try {
    const query = astDeserializerQuery({ node: fromOnlyNode } as any);
    const rows = await getQueryOutput(query);
    const baseSql = deserializeQuery(rows).replace(/;\s*$/, '');
    return baseSql;
  } catch {
    return `SELECT * FROM (${originalSql}) AS _base`;
  }
}

function extractFiltersFromAst(
  whereExpr: ParsedExpression,
  tableName: string
): { filters: (QueryFilter | LogicalOrFilter)[]; warnings: string[] } {
  const filters: (QueryFilter | LogicalOrFilter)[] = [];
  const warnings: string[] = [];

  if (whereExpr.class === ExpressionClass.CONJUNCTION) {
    const conj = whereExpr as ConjunctionExpression;
    if (whereExpr.type === ExpressionType.CONJUNCTION_AND) {
      for (const child of conj.children) {
        const extracted = tryExtractSingleFilter(child, tableName);
        if (extracted) {
          filters.push(extracted);
        } else {
          warnings.push('Non-extractable WHERE condition kept in base SQL');
        }
      }
    } else if (whereExpr.type === ExpressionType.CONJUNCTION_OR) {
      const orFilter = extractOrFilter(conj, tableName);
      if (orFilter) {
        filters.push(orFilter);
      } else {
        warnings.push('OR condition partially non-extractable');
      }
    }
  } else {
    const extracted = tryExtractSingleFilter(whereExpr, tableName);
    if (extracted) {
      filters.push(extracted);
    }
  }

  return { filters, warnings };
}

function extractOrFilter(
  conj: ConjunctionExpression,
  tableName: string
): LogicalOrFilter | null {
  const orChildren: (QueryFilter | LogicalAndFilter)[] = [];
  for (const child of conj.children) {
    if (child.class === ExpressionClass.CONJUNCTION && child.type === ExpressionType.CONJUNCTION_AND) {
      const andChildren: QueryFilter[] = [];
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
): QueryFilter | null {
  if (expr.class === ExpressionClass.COMPARISON) {
    return extractComparisonFilter(expr as ComparisonExpression, tableName);
  }
  if (expr.class === ExpressionClass.BETWEEN) {
    return extractBetweenFilter(expr as BetweenExpression, tableName);
  }
  if (expr.class === ExpressionClass.OPERATOR) {
    return extractOperatorFilter(expr as any, tableName);
  }
  if (expr.class === ExpressionClass.FUNCTION) {
    // IN, NOT IN are represented as functions in some DuckDB versions
    return extractFunctionFilter(expr as FunctionExpression, tableName);
  }
  return null;
}

function extractComparisonFilter(
  expr: ComparisonExpression,
  tableName: string
): QueryFilter | null {
  const colName = getColumnName(expr.left);
  if (!colName) return null;

  const value = getConstantValue(expr.right);
  if (value === null && expr.right.class !== ExpressionClass.CONSTANT) return null;

  const opMap: Record<string, QueryFilter['operator']> = {
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
): QueryFilter | null {
  const colName = getColumnName(expr.input);
  if (!colName) return null;

  const lower = getConstantValue(expr.lower);
  const upper = getConstantValue(expr.upper);
  if (lower === null || upper === null) return null;

  return {
    member: `${tableName}.${colName}`,
    operator: 'inDateRange',
    values: [String(lower), String(upper)],
  };
}

function extractOperatorFilter(
  expr: { type: ExpressionType; children: ParsedExpression[] },
  tableName: string
): QueryFilter | null {
  // IS NULL / IS NOT NULL
  if (expr.type === ExpressionType.OPERATOR_IS_NULL && expr.children?.length === 1) {
    const colName = getColumnName(expr.children[0]);
    if (!colName) return null;
    return { member: `${tableName}.${colName}`, operator: 'notSet', values: [] };
  }
  if (expr.type === ExpressionType.OPERATOR_IS_NOT_NULL && expr.children?.length === 1) {
    const colName = getColumnName(expr.children[0]);
    if (!colName) return null;
    return { member: `${tableName}.${colName}`, operator: 'set', values: [] };
  }
  return null;
}

function extractFunctionFilter(
  expr: FunctionExpression,
  tableName: string
): QueryFilter | null {
  // DuckDB represents IN as a function in some cases
  if (expr.function_name === 'in' || expr.function_name === '=') {
    // Not reliably structured — skip for now
  }
  return null;
}

function extractHavingFromAst(
  havingExpr: ParsedExpression,
  tableName: string,
  measures: Measure[]
): QueryFilter[] {
  const filters: QueryFilter[] = [];

  if (havingExpr.class === ExpressionClass.COMPARISON) {
    const comp = havingExpr as ComparisonExpression;
    const measure = matchMeasureFromExpr(comp.left, measures);
    if (measure) {
      const value = getConstantValue(comp.right);
      const opMap: Record<string, QueryFilter['operator']> = {
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

function matchMeasureFromExpr(expr: ParsedExpression, measures: Measure[]): Measure | null {
  if (expr.class === ExpressionClass.FUNCTION) {
    const fn = expr as FunctionExpression;
    return measures.find(
      (m) => m.sql.toLowerCase().startsWith(fn.function_name.toLowerCase() + '(')
    ) || null;
  }
  if (expr.alias) {
    return measures.find((m) => m.name === expr.alias) || null;
  }
  return null;
}

function extractOrderFromAst(
  orders: any[],
  tableName: string,
  dimensions: Dimension[],
  measures: Measure[]
): Record<string, QueryOrderType> {
  const result: Record<string, QueryOrderType> = {};

  for (const orderEntry of orders) {
    const expr = orderEntry.expression;
    const direction: QueryOrderType = orderEntry.type === 'DESCENDING' ? 'desc' : 'asc';

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
    }
  }

  return result;
}

function getColumnName(expr: ParsedExpression): string | null {
  if (expr.class !== ExpressionClass.COLUMN_REF) return null;
  const col = expr as ColumnRefExpression;
  // Return the last element (column name without table qualifier)
  return col.column_names[col.column_names.length - 1];
}

function getConstantValue(expr: ParsedExpression): string | number | null {
  if (expr.class !== ExpressionClass.CONSTANT) return null;
  const constant = expr as any;
  if (constant.value?.is_null) return null;
  return constant.value?.value ?? null;
}

function ensureFilterColumnInSchema(
  filter: QueryFilter,
  dimensions: Dimension[],
  tableName: string
): void {
  const colName = filter.member.replace(`${tableName}.`, '');
  const exists = dimensions.some((d) => d.name === colName || d.sql === colName);
  if (!exists) {
    dimensions.push({ name: colName, sql: colName, type: 'string' });
  }
}

function ensureOrFilterColumnsInSchema(
  orFilter: LogicalOrFilter,
  dimensions: Dimension[],
  tableName: string
): void {
  for (const child of orFilter.or) {
    if ('member' in child) {
      ensureFilterColumnInSchema(child as QueryFilter, dimensions, tableName);
    } else if ('and' in child) {
      for (const grandchild of (child as LogicalAndFilter).and) {
        if ('member' in grandchild) {
          ensureFilterColumnInSchema(grandchild as QueryFilter, dimensions, tableName);
        }
      }
    }
  }
}

function wrapFilters(filters: (QueryFilter | LogicalOrFilter)[]): MeerkatQueryFilter[] {
  if (filters.length === 0) return [];
  if (filters.length === 1) return [filters[0]];
  const andFilter: LogicalAndFilter = { and: filters as (QueryFilter | LogicalOrFilter)[] };
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
