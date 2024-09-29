import { Dimension, Query } from "../types/cube-types";
import { isArrayTypeMember } from "../utils/is-array-member-type";

export interface DimensionModifier {
  sqlExpression: string,
  dimension: Dimension,
  key: string,
  query: Query
}

export const arrayFieldUnNestModifier = ({ sqlExpression }: DimensionModifier): string => {
 return `array[unnest(${sqlExpression})]`;
}

export const shouldUnnest = ({ dimension, query }: DimensionModifier): boolean => {
  const isArrayType =  isArrayTypeMember(dimension.type);
  const hasUnNestedGroupBy = dimension.modifier?.unNestedGroupBy;
  return !!(isArrayType && hasUnNestedGroupBy && query.measures.length > 0);
}


export type Modifier = {
  name: string,
  matcher: (modifier: DimensionModifier) => boolean,
  modifier: (modifier: DimensionModifier) => string
}

export const MODIFIERS: Modifier[] = [{
  name: 'unNestedGroupBy',
  matcher: shouldUnnest,
  modifier: arrayFieldUnNestModifier
}]


export const getModifiedSqlExpression = ({ sqlExpression, dimension, key, modifiers, query }: DimensionModifier & {
  modifiers: Modifier[]
}) => {
  let finalDimension: string = sqlExpression;
  modifiers.forEach(({ modifier, matcher }) => {
    const shouldModify = matcher({ sqlExpression: finalDimension, dimension, key, query });
    if (shouldModify) {
      finalDimension = modifier({ sqlExpression: finalDimension, dimension, key, query });
    }
  })
  return finalDimension;
} 