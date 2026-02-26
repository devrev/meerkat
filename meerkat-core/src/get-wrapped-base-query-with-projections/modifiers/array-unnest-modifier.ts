import { isArrayTypeMember } from '../../utils/is-array-member-type';
import { DimensionModifier, Modifier } from '../types';

export const arrayFieldUnNestModifier = ({
  sqlExpression,
}: DimensionModifier): string => {
  return `array[unnest(${sqlExpression})]`;
};

export const shouldUnnest = ({
  dimension,
  query,
}: DimensionModifier): boolean => {
  const isArrayType = isArrayTypeMember(dimension.type);
  const hasUnNestedGroupBy = dimension.modifier?.shouldUnnestGroupBy;
  return !!(isArrayType && hasUnNestedGroupBy && query.measures.length > 0);
};

export const arrayUnnestModifier: Modifier = {
  name: 'shouldUnnestGroupBy',
  matcher: shouldUnnest,
  modifier: arrayFieldUnNestModifier,
};
