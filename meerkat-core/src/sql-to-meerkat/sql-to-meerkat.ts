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
 * Pipeline:
 * 1. Parse SQL → DuckDB JSON AST (via json_serialize_sql)
 * 2. Validate: reject UNION, WITH RECURSIVE, non-SELECT
 * 3. Fetch DuckDB catalog metadata (aggregate functions, type sets)
 * 4. Classify SELECT expressions → measures (aggregates) or dimensions
 * 5. Extract WHERE filters → Meerkat QueryFilters + residual for base SQL
 * 6. Extract HAVING filters → measure filters (all-or-nothing)
 * 7. Build base SQL (FROM/JOINs + residual WHERE/HAVING/QUALIFY)
 * 8. Extract ORDER BY, LIMIT, OFFSET
 * 9. Assemble final DecomposeOutput
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

  // ═══ STEP 1: Parse SQL into DuckDB's JSON AST ═══════════════════════════════
  // Uses json_serialize_sql('...') which parses syntactically (no binding/execution)
  let parsedAst: DuckDBSerializedAST;
  try {
    // Escape single quotes in the SQL for the json_serialize_sql('...') wrapper
    const serializeQuery = astSerializerQuery(sanitizeStringValue(sql));
    // Execute the parse query via the provided DuckDB connection
    const rows = await getQueryOutput(serializeQuery);
    // DuckDB returns the AST as a JSON string in the query result
    const jsonStr = deserializeQuery(rows);
    parsedAst = JSON.parse(jsonStr) as DuckDBSerializedAST;
  } catch (e) {
    return {
      success: false,
      reason: `DuckDB parse failed: ${(e as Error).message}`,
    };
  }

  // ═══ STEP 2: Validate the parsed AST ════════════════════════════════════════
  // json_serialize_sql sets error=true for syntax errors
  if (parsedAst.error) {
    return {
      success: false,
      reason: `DuckDB parse failed: ${
        parsedAst.error_message || 'unknown error'
      }`,
    };
  }

  // Extract the first statement (multi-statement SQL uses only the first)
  const statement = parsedAst.statements?.[0];
  if (!statement?.node) {
    return { success: false, reason: 'No statement found in parsed AST' };
  }

  const node = statement.node;

  // Reject set operations (UNION, INTERSECT, EXCEPT) — can't decompose into single schema
  if (node.type === QueryNodeType.SET_OPERATION_NODE) {
    return { success: false, reason: 'UNION/INTERSECT/EXCEPT not supported' };
  }
  // Only SELECT statements are supported
  if (node.type !== QueryNodeType.SELECT_NODE) {
    return { success: false, reason: `Unsupported query type: ${node.type}` };
  }

  const selectNode = node as SelectNode;

  // Reject WITH RECURSIVE — can't represent recursive computation in Meerkat schema
  if (hasRecursiveCteInMap(selectNode)) {
    return { success: false, reason: 'WITH RECURSIVE not supported' };
  }

  // Resolve the primary table name from the FROM clause
  // This becomes the namespace prefix for all members (e.g. "tickets.status")
  const tableName = extractTableName(selectNode);

  // ═══ STEP 3: Fetch DuckDB catalog metadata ══════════════════════════════════
  // Parallel queries to DuckDB's system tables for dynamic classification
  const [aggregateFunctions, numericTypes, datetimeTypes] = await Promise.all([
    fetchDuckDBFunctions(getQueryOutput, 'aggregate'), // e.g. sum, count, avg...
    fetchDuckDBTypes(getQueryOutput, 'NUMERIC'),       // e.g. INTEGER, DOUBLE...
    fetchDuckDBTypes(getQueryOutput, 'DATETIME'),      // e.g. DATE, TIMESTAMP...
  ]);
  const typeSets = { numeric: numericTypes, datetime: datetimeTypes };

  // ═══ STEP 4: Classify SELECT list expressions ══════════════════════════════
  const measures: Measure[] = [];
  const dimensions: Dimension[] = [];
  const queryMeasures: string[] = [];       // Qualified measure refs: "table.name"
  const queryDimensions: string[] = [];     // Qualified dimension refs: "table.name"
  const selectListOrder: (string | null)[] = []; // Position → name mapping for ORDER BY
  const usedNames = new Set<string>();      // Tracks used names to prevent collisions

  // Determine if this query has any aggregate context
  const hasAggregates = selectNode.select_list.some(
    (expr) => isAggregateExpr(expr, aggregateFunctions)
  );
  const hasGroupBy = selectNode.group_expressions.length > 0;

  // First pass: classify each expression and collect for batch serialization
  const exprBatch: { expr: ParsedExpression; isMeasure: boolean; selectIndex: number }[] = [];

  for (let idx = 0; idx < selectNode.select_list.length; idx++) {
    const expr = selectNode.select_list[idx];

    // SELECT * — skip (can't classify, but allows hasStar check later)
    if (isStarExpr(expr)) {
      selectListOrder.push(null);
      continue;
    }

    // Window functions (ROW_NUMBER, RANK, etc.) — skip from schema
    // They're kept in base SQL when QUALIFY exists
    if (isWindowExpr(expr)) {
      warnings.push(
        `Skipped window function: ${expr.alias || exprToName(expr)}`
      );
      selectListOrder.push(null);
      continue;
    }

    // Nested aggregates like COUNT(SUM(x)) — invalid, skip
    if (isNestedAggregateExpr(expr, aggregateFunctions)) {
      warnings.push(
        `Skipped nested aggregation: ${expr.alias || exprToName(expr)}`
      );
      selectListOrder.push(null);
      continue;
    }

    // Classify: measure if query has aggregate context AND this expression contains an aggregate
    const isMeasure = (hasAggregates || hasGroupBy) && isAggregateExpr(expr, aggregateFunctions);
    exprBatch.push({ expr, isMeasure, selectIndex: selectListOrder.length });
    selectListOrder.push(null); // Placeholder — filled after serialization
  }

  // Batch-serialize all expressions in a single DuckDB round-trip
  // Strips aliases and query_location to get clean SQL like "sum(amount)"
  const exprsToSerialize = exprBatch.map(({ expr }) => {
    const copy = JSON.parse(JSON.stringify(expr));
    copy.alias = '';                  // Remove alias so we get bare expression SQL
    stripQueryLocationInPlace(copy);  // Remove byte offsets that affect serialization
    return copy;
  });
  const serializedSqls = await serializeExpressions(exprsToSerialize, getQueryOutput);

  // Second pass: assign names and build schema entries
  for (let i = 0; i < exprBatch.length; i++) {
    const { expr, isMeasure, selectIndex } = exprBatch[i];
    const exprSql = serializedSqls[i]; // The serialized SQL expression

    if (isMeasure) {
      // Generate a unique name: use alias if available, otherwise auto-generate
      const name = deduplicateName(
        expr.alias || generateAggregateName(expr),
        usedNames
      );
      measures.push({ name, sql: exprSql, type: 'number' });
      queryMeasures.push(getNamespacedKey(tableName, name));
      selectListOrder[selectIndex] = name; // Record position for ORDER BY resolution
    } else {
      const name = deduplicateName(expr.alias || exprToName(expr), usedNames);
      const dimType = inferTypeFromExpr(expr); // 'time' for date functions, else 'string'
      dimensions.push({ name, sql: exprSql, type: dimType });
      queryDimensions.push(getNamespacedKey(tableName, name));
      selectListOrder[selectIndex] = name;
    }
  }

  // Bail if nothing was extractable (unless SELECT * is present)
  const hasStar = selectNode.select_list.some(isStarExpr);
  if (measures.length === 0 && dimensions.length === 0 && !hasStar) {
    return {
      success: false,
      reason: 'No extractable columns after filtering incompatible expressions',
    };
  }

  // ═══ STEP 5: Extract WHERE filters ══════════════════════════════════════════
  // Skip extraction when QUALIFY exists — WHERE affects window function
  // computation (execution order: WHERE → WINDOW → QUALIFY)
  let residualWhere: ParsedExpression | undefined;
  const allFilters: (QueryFilterWithValues | LogicalOrFilter)[] = [];
  const hasQualify = !!selectNode.qualify;

  if (selectNode.where_clause && !hasQualify) {
    // Extract simple conditions as Meerkat filters; keep complex ones as residual
    const extracted = extractFiltersFromAst(selectNode.where_clause, tableName, typeSets);
    allFilters.push(...extracted.filters);
    residualWhere = extracted.residual;
    if (extracted.warnings.length > 0) {
      warnings.push(...extracted.warnings);
    }
    // Add dimensions for filter columns not already in the SELECT list
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
    // QUALIFY present — keep entire WHERE in base SQL to preserve window semantics
    residualWhere = selectNode.where_clause;
  }

  // ═══ STEP 6: Extract HAVING filters ═════════════════════════════════════════
  // All-or-nothing: if any HAVING condition can't be matched to a known measure,
  // keep entire HAVING in base SQL and demote all measures to dimensions
  let residualHaving: ParsedExpression | undefined;
  let havingDemoted = false;

  if (selectNode.having) {
    const havingFilters = extractHavingFromAst(
      selectNode.having,
      tableName,
      measures
    );
    if (havingFilters.length > 0) {
      // All conditions matched measures — extract as filters
      allFilters.push(...havingFilters);
    } else {
      // Some conditions couldn't be matched — keep HAVING in base SQL
      residualHaving = selectNode.having;
      havingDemoted = true;
      warnings.push('Non-extractable HAVING condition retained in base SQL');
      // Demote measures to dimensions (base SQL handles aggregation + HAVING)
      for (const m of measures) {
        dimensions.push({ name: m.name, sql: m.name, type: 'number' });
        queryDimensions.push(getNamespacedKey(tableName, m.name));
      }
    }
  }

  // After demotion, measures become empty — use immutable derived values
  const finalMeasures = havingDemoted ? [] : measures;
  const finalQueryMeasures = havingDemoted ? [] : queryMeasures;

  // Wrap multiple filters in an AND container (downstream requires single-element array)
  const queryFilters: MeerkatQueryFilter[] = wrapFilters(allFilters);

  // ═══ STEP 7: Build base SQL ═════════════════════════════════════════════════
  // The base SQL is what cubeQueryToSQL wraps as the innermost subquery
  const baseSQL = await buildBaseSQL(
    selectNode,
    residualWhere,
    residualHaving,
    getQueryOutput,
    // When HAVING demoted: pass cleaned names so base SQL aliases match dimensions
    havingDemoted ? selectListOrder.map((name) => name || '') : undefined
  );
  if (baseSQL === null) {
    return {
      success: false,
      reason: 'Failed to serialize base SQL from AST',
    };
  }

  // ═══ STEP 8: Extract ORDER BY, LIMIT, OFFSET ═══════════════════════════════

  // ORDER BY — resolve column refs, expressions, and positional references
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

  // LIMIT and OFFSET — extract from constant AST nodes
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

  // ═══ STEP 9: Assemble output ═══════════════════════════════════════════════

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

// Wraps multiple filters in {and: [...]} for downstream compatibility.
// cubeFilterToDuckdbAST expects a single-element filters array.
function wrapFilters(
  filters: (QueryFilterWithValues | LogicalOrFilter)[]
): MeerkatQueryFilter[] {
  if (filters.length === 0) return [];
  if (filters.length === 1) return [filters[0]];
  return [{ and: filters }];
}

// Converts a constant value (string or number) to an integer for LIMIT/OFFSET.
// Returns undefined if the value can't be parsed as an integer.
function parseIntConstant(value: string | number | null): number | undefined {
  if (value === null) return undefined;
  const n = typeof value === 'number' ? value : Number(value);
  if (isNaN(n)) return undefined;
  return Math.trunc(n);
}
