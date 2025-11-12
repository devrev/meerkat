import { arrayFlattenModifierConfig } from './modifiers/array-flatten-modifier';
import { arrayUnnestModifier } from './modifiers/array-unnest-modifier';
import { DimensionModifier, Modifier } from './types';

export const MODIFIERS = [arrayUnnestModifier, arrayFlattenModifierConfig];

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
