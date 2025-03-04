export const NAME_PARTIAL_MATCH = 0.1;
export const NAME_EXACT_MATCH = 0.2;
export const TYPE_COMPATIBILITY_MATCH = 0.4;
export const SCHEMA_COMPATIBILITY_MATCH = 0.4;

export const MIN_COMPATIBILITY_SCORE = 0.2;

export const DEFAULT_COMPATIBILITY_SCORE = {
  typeScore: 0,
  nameScore: 0,
  schemaScore: 0,
  totalScore: 0,
} as const;
