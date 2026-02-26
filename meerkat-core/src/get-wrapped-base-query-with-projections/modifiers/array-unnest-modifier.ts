import { isArrayTypeMember } from '../../utils/is-array-member-type';
import { DimensionModifier, Modifier } from '../types';

export const arrayFieldUnNestModifier = ({
  sqlExpression,
}: DimensionModifier): string => {
  // Ensure NULL or empty arrays produce at least one row with NULL value
  // This prevents rows from being dropped when arrays are NULL or empty
  // COALESCE handles NULL, and len() = 0 check handles empty arrays []
  return `array[unnest(CASE WHEN ${sqlExpression} IS NULL OR len(COALESCE(${sqlExpression}, [])) = 0 THEN [NULL] ELSE ${sqlExpression} END)]`;
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
