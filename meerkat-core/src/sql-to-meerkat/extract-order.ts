import { QueryOrderType } from '../types/cube-types/query';
import { Dimension, Measure } from '../types/cube-types/table';
import {
  ExpressionClass,
  FunctionExpression,
  OrderByNode,
  OrderType,
  ParsedExpression,
  PositionalReferenceExpression,
} from '../types/duckdb-serialization-types';
import { getColumnName } from './helpers';

export function extractOrderFromAst(
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
