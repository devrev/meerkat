import {
  LogicalOrFilter,
  MeerkatQueryFilter,
  Query,
  QueryFilterWithValues,
} from '../types/cube-types/query';
import { Dimension, TableSchema } from '../types/cube-types/table';
import { ParsedExpression } from '../types/duckdb-serialization-types';
import { fetchDuckDBFunctions, fetchDuckDBTypes, GetQueryOutput } from '../utils/duckdb-ast-parse-serialize';
import { getNamespacedKey } from '../member-formatters/get-namespaced-key';
import { DecomposeOutput } from './types';
import {
  buildBaseSQL,
  ensureFilterColumnInSchema,
  ensureOrFilterColumnsInSchema,
  extractFiltersFromAst,
  extractHavingFromAst,
  isStarExpr,
} from './helpers';
import { parseAndValidateAst } from './steps/parse-ast';
import { classifyExpressions } from './steps/classify-expressions';
import { extractModifiers } from './steps/extract-modifiers';

/**
 * Decomposes a raw SQL SELECT query into a Meerkat TableSchema + Query pair.
 *
 * Pipeline:
 * 1. Parse + validate AST (reject UNION, RECURSIVE, non-SELECT)
 * 2. Fetch DuckDB catalog metadata (aggregate functions, type sets)
 * 3. Classify SELECT expressions → measures/dimensions
 * 4. Extract WHERE filters
 * 5. Extract HAVING filters (all-or-nothing)
 * 6. Build base SQL (FROM/JOINs + residuals)
 * 7. Extract ORDER BY, LIMIT, OFFSET
 * 8. Assemble output
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

  // ═══ STEP 1: Parse and validate ═════════════════════════════════════════════
  const parseResult = await parseAndValidateAst(sql, getQueryOutput);
  if (typeof parseResult === 'string') {
    return { success: false, reason: parseResult };
  }
  const { selectNode, tableName } = parseResult;

  // ═══ STEP 2: Fetch DuckDB catalog metadata ══════════════════════════════════
  const [aggregateFunctions, numericTypes, datetimeTypes] = await Promise.all([
    fetchDuckDBFunctions(getQueryOutput, 'aggregate'),
    fetchDuckDBTypes(getQueryOutput, 'NUMERIC'),
    fetchDuckDBTypes(getQueryOutput, 'DATETIME'),
  ]);
  const typeSets = { numeric: numericTypes, datetime: datetimeTypes };

  // ═══ STEP 3: Classify SELECT expressions ═══════════════════════════════════
  const classification = await classifyExpressions(
    selectNode, tableName, aggregateFunctions, getQueryOutput
  );
  const { measures, dimensions, queryMeasures, queryDimensions, selectListOrder } = classification;
  warnings.push(...classification.warnings);

  // Bail if nothing was extractable (unless SELECT * is present)
  const hasStar = selectNode.select_list.some(isStarExpr);
  if (measures.length === 0 && dimensions.length === 0 && !hasStar) {
    return {
      success: false,
      reason: 'No extractable columns after filtering incompatible expressions',
    };
  }

  // ═══ STEP 4: Extract WHERE filters ══════════════════════════════════════════
  // Skip when QUALIFY exists — WHERE affects window computation order
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
    // Add dimensions for filter columns not in the SELECT list
    for (const f of extracted.filters) {
      if ('member' in f) {
        const newDims = ensureFilterColumnInSchema(
          f as QueryFilterWithValues, dimensions, tableName, extracted.memberTypes
        );
        if (newDims) dimensions.push(...newDims);
      } else if ('or' in f) {
        const newDims = ensureOrFilterColumnsInSchema(
          f as LogicalOrFilter, dimensions, tableName
        );
        if (newDims) dimensions.push(...newDims);
      }
    }
  } else if (selectNode.where_clause && hasQualify) {
    residualWhere = selectNode.where_clause;
  }

  // ═══ STEP 5: Extract HAVING filters ═════════════════════════════════════════
  // All-or-nothing: demote measures if any HAVING condition can't be matched
  let residualHaving: ParsedExpression | undefined;
  let havingDemoted = false;

  if (selectNode.having) {
    const havingFilters = extractHavingFromAst(selectNode.having, tableName, measures);
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

  // ═══ STEP 6: Build base SQL ═════════════════════════════════════════════════
  const baseSQL = await buildBaseSQL(
    selectNode, residualWhere, residualHaving, getQueryOutput,
    havingDemoted ? selectListOrder.map((name) => name || '') : undefined
  );
  if (baseSQL === null) {
    return { success: false, reason: 'Failed to serialize base SQL from AST' };
  }

  // ═══ STEP 7: Extract ORDER BY, LIMIT, OFFSET ═══════════════════════════════
  const { order, limit, offset } = extractModifiers(
    selectNode, tableName, dimensions, finalMeasures, selectListOrder
  );

  // ═══ STEP 8: Assemble output ═══════════════════════════════════════════════
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

// Wraps multiple filters in {and: [...]} for downstream compatibility
function wrapFilters(
  filters: (QueryFilterWithValues | LogicalOrFilter)[]
): MeerkatQueryFilter[] {
  if (filters.length === 0) return [];
  if (filters.length === 1) return [filters[0]];
  return [{ and: filters }];
}
