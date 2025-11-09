import { Dimension, Query } from '../types/cube-types';
import { isArrayTypeMember } from '../utils/is-array-member-type';

export interface DimensionModifier {
  sqlExpression: string;
  dimension: Dimension;
  key: string;
  query: Query;
}

export const arrayFieldUnNestModifier = ({
  sqlExpression,
}: DimensionModifier): string => {
  return `array[unnest(${sqlExpression})]`;
};

export const arrayFlattenModifier = ({
  sqlExpression,
}: DimensionModifier): string => {
  // Ensure NULL or empty arrays produce at least one row with NULL value
  // This prevents rows from being dropped when arrays are NULL or empty
  // COALESCE handles NULL, and len() = 0 check handles empty arrays []
  return `unnest(CASE WHEN ${sqlExpression} IS NULL OR len(COALESCE(${sqlExpression}, [])) = 0 THEN [NULL] ELSE ${sqlExpression} END)`;
};

export const shouldUnnest = ({
  dimension,
  query,
}: DimensionModifier): boolean => {
  const isArrayType = isArrayTypeMember(dimension.type);
  const hasUnNestedGroupBy = dimension.modifier?.shouldUnnestGroupBy;
  return !!(isArrayType && hasUnNestedGroupBy && query.measures.length > 0);
};

export const shouldFlattenArray = ({
  dimension,
}: DimensionModifier): boolean => {
  const isArrayType = isArrayTypeMember(dimension.type);
  const hasUnNestedGroupBy = dimension.modifier?.shouldFlattenArray;
  return !!(isArrayType && hasUnNestedGroupBy);
};

export type Modifier = {
  name: string;
  matcher: (modifier: DimensionModifier) => boolean;
  modifier: (modifier: DimensionModifier) => string;
};

export const MODIFIERS: Modifier[] = [
  {
    name: 'shouldUnnestGroupBy',
    matcher: shouldUnnest,
    modifier: arrayFieldUnNestModifier,
  },
  {
    name: 'shouldFlattenArray',
    matcher: shouldFlattenArray,
    modifier: arrayFlattenModifier,
  },
];

export const getModifiedSqlExpression = ({
  sqlExpression,
  dimension,
  key,
  modifiers,
  query,
}: DimensionModifier & {
  modifiers: Modifier[];
}) => {
  let finalDimension: string = sqlExpression;
  modifiers.forEach(({ modifier, matcher }) => {
    const shouldModify = matcher({
      sqlExpression: finalDimension,
      dimension,
      key,
      query,
    });
    if (shouldModify) {
      finalDimension = modifier({
        sqlExpression: finalDimension,
        dimension,
        key,
        query,
      });
    }
  });
  return finalDimension;
};
