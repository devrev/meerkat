import {
  ExpressionClass,
  ExpressionType,
  ResultModifierType,
} from '@devrev/duckdb-serialization-types';

export const cubeLimitOffsetToAST = (
  limit?: number | null,
  offset?: number | null
) => {
  return {
    type: ResultModifierType.LIMIT_MODIFIER,
    limit: limit
      ? {
          class: ExpressionClass.CONSTANT,
          type: ExpressionType.VALUE_CONSTANT,
          alias: '',
          value: {
            type: {
              id: 'INTEGER',
              type_info: null,
            },
            is_null: false,
            value: limit,
          },
        }
      : null,
    offset: offset
      ? {
          class: ExpressionClass.CONSTANT,
          type: ExpressionType.VALUE_CONSTANT,
          alias: '',
          value: {
            type: {
              id: 'INTEGER',
              type_info: null,
            },
            is_null: false,
            value: offset,
          },
        }
      : null,
  };
};
