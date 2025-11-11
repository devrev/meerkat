import {
  arrayFlattenModifier,
  arrayFlattenModifierConfig,
  shouldFlattenArray,
} from './modifiers/array-flatten-modifier';
import {
  arrayFieldUnNestModifier,
  arrayUnnestModifier,
  shouldUnnest,
} from './modifiers/array-unnest-modifier';
import { DimensionModifier, Modifier } from './types';

// Re-export types for backward compatibility
export type { DimensionModifier, Modifier };

export const MODIFIERS = [arrayUnnestModifier, arrayFlattenModifierConfig];

// Export individual modifier functions for backward compatibility
export {
  arrayFieldUnNestModifier,
  arrayFlattenModifier,
  shouldFlattenArray,
  shouldUnnest,
};

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
