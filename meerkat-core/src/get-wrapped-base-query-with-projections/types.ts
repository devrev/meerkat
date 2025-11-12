import { Dimension, Query } from '../types/cube-types';

export interface DimensionModifier {
  sqlExpression: string;
  dimension: Dimension;
  key: string;
  query: Query;
}

export type Modifier = {
  name: string;
  matcher: (modifier: DimensionModifier) => boolean;
  modifier: (modifier: DimensionModifier) => string;
};
