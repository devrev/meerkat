import { isArrayTypeMember } from '../../utils/is-array-member-type';
import { DimensionModifier, Modifier } from '../types';

export const arrayFlattenModifier = ({
  sqlExpression,
}: DimensionModifier): string => {
  // Ensure NULL or empty arrays produce at least one row with NULL value
  // This prevents rows from being dropped when arrays are NULL or empty
  // COALESCE handles NULL, and len() = 0 check handles empty arrays []
  return `unnest(CASE WHEN ${sqlExpression} IS NULL OR len(COALESCE(${sqlExpression}, [])) = 0 THEN [NULL] ELSE ${sqlExpression} END)`;
};

export const shouldFlattenArray = ({
  dimension,
}: DimensionModifier): boolean => {
  const isArrayType = isArrayTypeMember(dimension.type);
  const shouldFlattenArray = dimension.modifier?.shouldFlattenArray;
  return !!(isArrayType && shouldFlattenArray);
};

export const arrayFlattenModifierConfig: Modifier = {
  name: 'shouldFlattenArray',
  matcher: shouldFlattenArray,
  modifier: arrayFlattenModifier,
};
