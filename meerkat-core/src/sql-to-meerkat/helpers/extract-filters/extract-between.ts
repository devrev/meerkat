import { QueryFilterWithValues } from '../../../types/cube-types/query';
import { Dimension } from '../../../types/cube-types/table';
import {
  BetweenExpression,
  ExpressionClass,
  ParsedExpression,
} from '../../../types/duckdb-serialization-types';
import { getConstantValue, getConstantTypeId } from '../../../utils/ast-constants';
import { resolveMemberName } from './extract-comparison';
import { typeFromConstantExpr, TypeSets } from './filter-schema';

function isDateTypedConstant(expr: ParsedExpression): boolean {
  if (expr.class === ExpressionClass.CAST) {
    const cast = expr as ParsedExpression & { cast_type?: { id?: string }; child?: ParsedExpression };
    const castId = (cast.cast_type?.id || '').toUpperCase();
    if (castId.includes('DATE') || castId.includes('TIMESTAMP')) return true;
    if (cast.child) return isDateTypedConstant(cast.child);
    return false;
  }
  const typeId = getConstantTypeId(expr);
  if (!typeId) return false;
  const upper = typeId.toUpperCase();
  return upper.includes('DATE') || upper.includes('TIMESTAMP') || upper === 'INTERVAL';
}

function isBetweenDateRange(expr: BetweenExpression): boolean {
  return isDateTypedConstant(expr.lower) || isDateTypedConstant(expr.upper);
}

export function extractBetweenFilter(
  expr: BetweenExpression,
  tableName: string,
  memberTypes: Record<string, Dimension['type']>,
  typeSets: TypeSets
): QueryFilterWithValues[] | null {
  const member = resolveMemberName(expr.input, tableName);
  if (!member) return null;

  const lower = getConstantValue(expr.lower);
  const upper = getConstantValue(expr.upper);
  if (lower === null || upper === null) return null;

  if (isBetweenDateRange(expr)) {
    if (!memberTypes[member]) memberTypes[member] = 'time';
    return [
      { member, operator: 'inDateRange', values: [String(lower), String(upper)] },
    ];
  }

  if (!memberTypes[member]) memberTypes[member] = typeFromConstantExpr(expr.lower, typeSets);
  return [
    { member, operator: 'gte', values: [String(lower)] },
    { member, operator: 'lte', values: [String(upper)] },
  ];
}
