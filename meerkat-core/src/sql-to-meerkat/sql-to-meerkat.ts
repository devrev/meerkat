import { astSerializerQuery } from '../ast-serializer/ast-serializer';
import { deserializeQuery } from '../ast-deserializer/ast-deserializer';
import {
  LogicalOrFilter,
  MeerkatQueryFilter,
  Query,
  QueryFilterWithValues,
  QueryOrderType,
} from '../types/cube-types/query';
import { Dimension, Measure, TableSchema } from '../types/cube-types/table';
import {
  LimitModifier,
  OrderModifier,
  ParsedExpression,
  QueryNodeType,
  ResultModifierType,
  SelectNode,
} from '../types/duckdb-serialization-types';
import { GetQueryOutput } from '../utils/duckdb-ast-parse-serialize';
import { DecomposeOutput } from './types';
import { buildBaseSQL } from './build-base-sql';
import {
  ensureFilterColumnInSchema,
  ensureOrFilterColumnsInSchema,
  extractFiltersFromAst,
  extractHavingFromAst,
} from './extract-filters';
import { extractOrderFromAst } from './extract-order';
import {
  deduplicateName,
  exprToName,
  exprToSql,
  extractTableName,
  generateAggregateName,
  getConstantValue,
  hasRecursiveCteInMap,
  inferTypeFromExpr,
  isAggregateExpr,
  isNestedAggregateExpr,
  isStarExpr,
  isWindowExpr,
  sanitizeForSerialize,
} from './helpers';

export interface SqlToMeerkatInput {
  sql: string;
  getQueryOutput: GetQueryOutput;
}

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
  const selectListOrder: string[] = [];
  const usedNames = new Set<string>();

  const hasAggregates = selectNode.select_list.some(isAggregateExpr);
  const hasGroupBy = selectNode.group_expressions.length > 0;

  for (const expr of selectNode.select_list) {
    if (isStarExpr(expr)) continue;

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
        selectListOrder.push(name);
      } else {
        const name = deduplicateName(expr.alias || exprToName(expr), usedNames);
        const dimSql = await exprToSql(expr, getQueryOutput);
        const dimType = inferTypeFromExpr(expr);
        dimensions.push({ name, sql: dimSql, type: dimType });
        queryDimensions.push(`${tableName}.${name}`);
        selectListOrder.push(name);
      }
    } else {
      const name = deduplicateName(expr.alias || exprToName(expr), usedNames);
      const dimSql = await exprToSql(expr, getQueryOutput);
      const dimType = inferTypeFromExpr(expr);
      dimensions.push({ name, sql: dimSql, type: dimType });
      queryDimensions.push(`${tableName}.${name}`);
      selectListOrder.push(name);
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

  const baseSQL = await buildBaseSQL(
    sql,
    selectNode,
    residualWhere,
    getQueryOutput
  );

  // Extract ORDER BY
  let order: Record<string, QueryOrderType> | undefined;
  const orderModifier = selectNode.modifiers.find(
    (m) => m.type === ResultModifierType.ORDER_MODIFIER
  ) as OrderModifier | undefined;
  if (orderModifier) {
    const extracted = extractOrderFromAst(
      orderModifier.orders,
      tableName,
      dimensions,
      measures,
      selectListOrder
    );
    if (Object.keys(extracted).length > 0) {
      order = extracted;
    }
  }

  // Extract LIMIT/OFFSET
  let limit: number | undefined;
  let offset: number | undefined;
  const limitModifier = selectNode.modifiers.find(
    (m) => m.type === ResultModifierType.LIMIT_MODIFIER
  ) as LimitModifier | undefined;
  if (limitModifier) {
    if (limitModifier.limit) {
      limit = parseIntConstant(getConstantValue(limitModifier.limit));
    }
    if (limitModifier.offset) {
      offset = parseIntConstant(getConstantValue(limitModifier.offset));
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

function wrapFilters(
  filters: (QueryFilterWithValues | LogicalOrFilter)[]
): MeerkatQueryFilter[] {
  if (filters.length === 0) return [];
  if (filters.length === 1) return [filters[0]];
  return [{ and: filters }];
}

function parseIntConstant(value: string | number | null): number | undefined {
  if (value === null) return undefined;
  const n = typeof value === 'number' ? value : Number(value);
  if (isNaN(n)) return undefined;
  return Math.trunc(n);
}
