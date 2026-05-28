import { QueryOrderType } from '../../types/cube-types/query';
import { Dimension, Measure } from '../../types/cube-types/table';
import {
  ConstantExpression,
  ExpressionClass,
  OrderByNode,
  OrderType,
  ParsedExpression,
} from '../../types/duckdb-serialization-types';
import { getColumnName, matchMeasureFromExpr } from './ast-utils';

/**
 * Extracts ORDER BY clauses into Meerkat's order format.
 *
 * Resolves ORDER BY entries by:
 * - Column name → matches against known dimensions/measures
 * - Aggregate expression → matches against measures via matchMeasureFromExpr
 * - Positional reference (ORDER BY 1) → maps to selectListOrder index
 *
 * Unresolvable ORDER BY entries (expressions, unknown columns) are silently dropped.
 */

export function extractOrderFromAst(
  orders: OrderByNode[],
  tableName: string,
  dimensions: readonly Dimension[],
  measures: readonly Measure[],
  selectListOrder?: readonly (string | null)[]
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
    } else if (expr?.class === ExpressionClass.CONSTANT) {
      // ORDER BY 1, ORDER BY 2 — positional reference (1-indexed in SQL, 0-indexed here)
      const idx = resolvePositionalIndex(expr);
      if (idx !== null) {
        const orderedNames = selectListOrder ?? [
          ...dimensions.map((d) => d.name),
          ...measures.map((m) => m.name),
        ];
        if (idx >= 0 && idx < orderedNames.length) {
          const name = orderedNames[idx];
          // null entries are skipped items (STAR, WINDOW) — can't order by them
          if (name !== null) {
            result[`${tableName}.${name}`] = direction;
          }
        }
      }
    }
  }

  return result;
}

function resolvePositionalIndex(expr: ParsedExpression): number | null {
  const constant = expr as ConstantExpression;
  const val = constant.value as { is_null?: boolean; value?: number } | undefined;
  if (val?.is_null || typeof val?.value !== 'number') return null;
  return val.value - 1;
}

