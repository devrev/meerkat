import { QueryOrderType } from '../../types/cube-types/query';
import { Dimension, Measure } from '../../types/cube-types/table';
import {
  LimitModifier,
  ParsedExpression,
  ResultModifierType,
  OrderModifier,
  SelectNode,
} from '../../types/duckdb-serialization-types';
import { extractOrderFromAst, getConstantValue } from '../helpers';

export interface ModifierResult {
  order: Record<string, QueryOrderType> | undefined;
  limit: number | undefined;
  offset: number | undefined;
}

/**
 * Extracts ORDER BY, LIMIT, and OFFSET from the SelectNode's modifiers.
 */
export function extractModifiers(
  selectNode: SelectNode,
  tableName: string,
  dimensions: readonly Dimension[],
  measures: readonly Measure[],
  selectListOrder: readonly (string | null)[]
): ModifierResult {
  // ORDER BY
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

  // LIMIT and OFFSET
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

  return { order, limit, offset };
}

function parseIntConstant(value: string | number | null): number | undefined {
  if (value === null) return undefined;
  const n = typeof value === 'number' ? value : Number(value);
  if (isNaN(n)) return undefined;
  return Math.trunc(n);
}
