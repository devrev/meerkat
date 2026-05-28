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
import { fetchDuckDBFunctions, fetchDuckDBTypes, GetQueryOutput, serializeExpressions } from '../utils/duckdb-ast-parse-serialize';
import { getNamespacedKey } from '../member-formatters/get-namespaced-key';
import { sanitizeStringValue } from '../member-formatters/sanitize-value';
import { DecomposeOutput, DuckDBSerializedAST } from './types';
import {
  buildBaseSQL,
  deduplicateName,
  ensureFilterColumnInSchema,
  ensureOrFilterColumnsInSchema,
  exprToName,
  extractFiltersFromAst,
  extractHavingFromAst,
  extractOrderFromAst,
  extractTableName,
  generateAggregateName,
  getConstantValue,
  hasRecursiveCteInMap,
  inferTypeFromExpr,
  isAggregateExpr,
  isNestedAggregateExpr,
  isStarExpr,
  isWindowExpr,
  stripQueryLocationInPlace,
} from './helpers';

/**
 * Decomposes a raw SQL SELECT query into a Meerkat TableSchema + Query pair.
 *
 * Strategy:
 * 1. Parse SQL into DuckDB's JSON AST via json_serialize_sql
 * 2. Classify SELECT expressions as measures (aggregates) or dimensions
 * 3. Extract WHERE filters that can be represented as Meerkat QueryFilters
 * 4. Extract HAVING filters matching known measures
 * 5. Build a base SQL containing only FROM/JOINs and non-extractable conditions
 * 6. Extract ORDER BY, LIMIT, OFFSET
 *
 * Non-extractable conditions (subqueries, complex expressions, cross-table refs)
 * are retained in the base SQL as residual clauses, ensuring correctness.
 *
 * Limitations:
 * - UNION/INTERSECT/EXCEPT and WITH RECURSIVE are rejected
 * - DISTINCT and SAMPLE are dropped from round-trip
 * - Window functions are skipped (kept in base SQL when QUALIFY exists)
 * - WHERE is not extracted when QUALIFY is present (affects window semantics)
 */

export interface SqlToMeerkatInput {
  sql: string;
  getQueryOutput: GetQueryOutput;
}

export async function sqlToMeerkat(
  input: SqlToMeerkatInput
): Promise<DecomposeOutput> {
  const { sql, getQueryOutput } = input;
  const warnings: string[] = [];

  let parsedAst: DuckDBSerializedAST;
  try {
    const serializeQuery = astSerializerQuery(sanitizeStringValue(sql));
    const rows = await getQueryOutput(serializeQuery);
    const jsonStr = deserializeQuery(rows);
    parsedAst = JSON.parse(jsonStr) as DuckDBSerializedAST;
  } catch (e) {
    return {
      success: false,
      reason: `DuckDB parse failed: ${(e as Error).message}`,
    };
  }

  if (parsedAst.error) {
    return {
      success: false,
      reason: `DuckDB parse failed: ${
        parsedAst.error_message || 'unknown error'
      }`,
    };
  }

  const statement = parsedAst.statements?.[0];
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

  const selectNode = node as SelectNode;

  if (hasRecursiveCteInMap(selectNode)) {
    return { success: false, reason: 'WITH RECURSIVE not supported' };
  }

  const tableName = extractTableName(selectNode);

  const [aggregateFunctions, numericTypes, datetimeTypes] = await Promise.all([
    fetchDuckDBFunctions(getQueryOutput, 'aggregate'),
    fetchDuckDBTypes(getQueryOutput, 'NUMERIC'),
    fetchDuckDBTypes(getQueryOutput, 'DATETIME'),
  ]);
  const typeSets = { numeric: numericTypes, datetime: datetimeTypes };

  const measures: Measure[] = [];
  const dimensions: Dimension[] = [];
  const queryMeasures: string[] = [];
  const queryDimensions: string[] = [];
  const selectListOrder: (string | null)[] = [];
  const usedNames = new Set<string>();

  const hasAggregates = selectNode.select_list.some(
    (expr) => isAggregateExpr(expr, aggregateFunctions)
  );
  const hasGroupBy = selectNode.group_expressions.length > 0;

  // Collect serializable expressions, tracking their position in the select list
  const exprBatch: { expr: ParsedExpression; isMeasure: boolean; selectIndex: number }[] = [];

  for (let idx = 0; idx < selectNode.select_list.length; idx++) {
    const expr = selectNode.select_list[idx];

    if (isStarExpr(expr)) {
      selectListOrder.push(null);
      continue;
    }

    if (isWindowExpr(expr)) {
      warnings.push(
        `Skipped window function: ${expr.alias || exprToName(expr)}`
      );
      selectListOrder.push(null);
      continue;
    }

    if (isNestedAggregateExpr(expr, aggregateFunctions)) {
      warnings.push(
        `Skipped nested aggregation: ${expr.alias || exprToName(expr)}`
      );
      selectListOrder.push(null);
      continue;
    }

    const isMeasure = (hasAggregates || hasGroupBy) && isAggregateExpr(expr, aggregateFunctions);
    exprBatch.push({ expr, isMeasure, selectIndex: selectListOrder.length });
    selectListOrder.push(null);
  }

  // Single round-trip for all expression serialization
  const exprsToSerialize = exprBatch.map(({ expr }) => {
    const copy = JSON.parse(JSON.stringify(expr));
    copy.alias = '';
    stripQueryLocationInPlace(copy);
    return copy;
  });
  const serializedSqls = await serializeExpressions(exprsToSerialize, getQueryOutput);

  for (let i = 0; i < exprBatch.length; i++) {
    const { expr, isMeasure, selectIndex } = exprBatch[i];
    const exprSql = serializedSqls[i];

    if (isMeasure) {
      const name = deduplicateName(
        expr.alias || generateAggregateName(expr),
        usedNames
      );
      measures.push({ name, sql: exprSql, type: 'number' });
      queryMeasures.push(getNamespacedKey(tableName, name));
      selectListOrder[selectIndex] = name;
    } else {
      const name = deduplicateName(expr.alias || exprToName(expr), usedNames);
      const dimType = inferTypeFromExpr(expr);
      dimensions.push({ name, sql: exprSql, type: dimType });
      queryDimensions.push(getNamespacedKey(tableName, name));
      selectListOrder[selectIndex] = name;
    }
  }

  const hasStar = selectNode.select_list.some(isStarExpr);
  if (measures.length === 0 && dimensions.length === 0 && !hasStar) {
    return {
      success: false,
      reason: 'No extractable columns after filtering incompatible expressions',
    };
  }

  // Extract WHERE filters, separating extractable from non-extractable.
  // Skip extraction when QUALIFY exists — WHERE affects window function
  // computation and must stay in the base SQL.
  let residualWhere: ParsedExpression | undefined;
  const allFilters: (QueryFilterWithValues | LogicalOrFilter)[] = [];
  const hasQualify = !!selectNode.qualify;
  if (selectNode.where_clause && !hasQualify) {
    const extracted = extractFiltersFromAst(selectNode.where_clause, tableName, typeSets);
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
          tableName,
          extracted.memberTypes
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
  } else if (selectNode.where_clause && hasQualify) {
    residualWhere = selectNode.where_clause;
  }

  // Extract HAVING as measure filters (all-or-nothing: if any condition
  // can't be matched to a known measure, keep entire HAVING in base SQL
  // and demote all measures to dimensions to prevent re-aggregation)
  let residualHaving: ParsedExpression | undefined;
  let havingDemoted = false;
  if (selectNode.having) {
    const havingFilters = extractHavingFromAst(
      selectNode.having,
      tableName,
      measures
    );
    if (havingFilters.length > 0) {
      allFilters.push(...havingFilters);
    } else {
      residualHaving = selectNode.having;
      havingDemoted = true;
      warnings.push('Non-extractable HAVING condition retained in base SQL');
      for (const m of measures) {
        dimensions.push({ name: m.name, sql: m.name, type: 'number' });
        queryDimensions.push(getNamespacedKey(tableName, m.name));
      }
    }
  }

  const finalMeasures = havingDemoted ? [] : measures;
  const finalQueryMeasures = havingDemoted ? [] : queryMeasures;

  const queryFilters: MeerkatQueryFilter[] = wrapFilters(allFilters);

  const baseSQL = await buildBaseSQL(
    selectNode,
    residualWhere,
    residualHaving,
    getQueryOutput,
    havingDemoted ? selectListOrder.map((name) => name || '') : undefined
  );
  if (baseSQL === null) {
    return {
      success: false,
      reason: 'Failed to serialize base SQL from AST',
    };
  }

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
      finalMeasures,
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
    measures: finalMeasures,
    dimensions,
  };

  const query: Query = {
    measures: finalQueryMeasures,
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
  // Downstream cubeFilterToDuckdbAST requires a single-element array
  return [{ and: filters }];
}

function parseIntConstant(value: string | number | null): number | undefined {
  if (value === null) return undefined;
  const n = typeof value === 'number' ? value : Number(value);
  if (isNaN(n)) return undefined;
  return Math.trunc(n);
}
