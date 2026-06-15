import { Dimension, Measure } from '../../types/cube-types/table';
import { ParsedExpression, SelectNode } from '../../types/duckdb-serialization-types';
import { GetQueryOutput, serializeExpressions } from '../../utils/duckdb-ast-parse-serialize';
import { getNamespacedKey } from '../../member-formatters/get-namespaced-key';
import {
  deduplicateName,
  exprToName,
  generateAggregateName,
  inferTypeFromExpr,
  isAggregateExpr,
  isNestedAggregateExpr,
  isStarExpr,
  isWindowExpr,
  stripQueryLocationInPlace,
} from '../helpers';

export interface ClassificationResult {
  measures: Measure[];
  dimensions: Dimension[];
  queryMeasures: string[];
  queryDimensions: string[];
  selectListOrder: (string | null)[];
  warnings: string[];
}

/**
 * Classifies SELECT list expressions into measures (aggregates) and dimensions.
 * Batch-serializes all expressions in a single DuckDB round-trip.
 */
export async function classifyExpressions(
  selectNode: SelectNode,
  tableName: string,
  aggregateFunctions: ReadonlySet<string>,
  getQueryOutput: GetQueryOutput
): Promise<ClassificationResult> {
  const measures: Measure[] = [];
  const dimensions: Dimension[] = [];
  const queryMeasures: string[] = [];
  const queryDimensions: string[] = [];
  const selectListOrder: (string | null)[] = [];
  const usedNames = new Set<string>();
  const warnings: string[] = [];

  // Determine if this query has aggregate context
  const hasAggregates = selectNode.select_list.some(
    (expr) => isAggregateExpr(expr, aggregateFunctions)
  );
  const hasGroupBy = selectNode.group_expressions.length > 0;

  // First pass: classify and collect for batch serialization
  const exprBatch: { expr: ParsedExpression; isMeasure: boolean; selectIndex: number }[] = [];

  for (let idx = 0; idx < selectNode.select_list.length; idx++) {
    const expr = selectNode.select_list[idx];

    if (isStarExpr(expr)) {
      selectListOrder.push(null);
      continue;
    }

    if (isWindowExpr(expr)) {
      warnings.push(`Skipped window function: ${expr.alias || exprToName(expr)}`);
      selectListOrder.push(null);
      continue;
    }

    if (isNestedAggregateExpr(expr, aggregateFunctions)) {
      warnings.push(`Skipped nested aggregation: ${expr.alias || exprToName(expr)}`);
      selectListOrder.push(null);
      continue;
    }

    const isMeasure = (hasAggregates || hasGroupBy) && isAggregateExpr(expr, aggregateFunctions);
    exprBatch.push({ expr, isMeasure, selectIndex: selectListOrder.length });
    selectListOrder.push(null);
  }

  // Batch-serialize all expressions (single DuckDB round-trip)
  const exprsToSerialize = exprBatch.map(({ expr }) => {
    const copy = JSON.parse(JSON.stringify(expr));
    copy.alias = '';
    stripQueryLocationInPlace(copy);
    return copy;
  });
  const serializedSqls = await serializeExpressions(exprsToSerialize, getQueryOutput);

  // Second pass: assign names and build schema entries
  for (let i = 0; i < exprBatch.length; i++) {
    const { expr, isMeasure, selectIndex } = exprBatch[i];
    const exprSql = serializedSqls[i];

    if (isMeasure) {
      const name = deduplicateName(expr.alias || generateAggregateName(expr), usedNames);
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

  return { measures, dimensions, queryMeasures, queryDimensions, selectListOrder, warnings };
}
