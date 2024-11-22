import {
  ExpressionType,
  ParsedExpression,
} from '../types/duckdb-serialization-types';
import { ExpressionClass } from '../types/duckdb-serialization-types/serialization/Expression';
import { validator } from './dimension-validator';

describe('dimension validator', () => {
  it('should return true for node type COLUMN_REF', () => {
    const COLUMN_REF_NODE: ParsedExpression = {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      query_location: 0,
      column_names: ['avg_avg_time_spent_on_dashboard_val'],
    };

    expect(validator(COLUMN_REF_NODE)).toBe(true);
  });

  it('should return true for node type VALUE_CONSTANT', () => {
    const VALUE_CONSTANT_NODE: ParsedExpression = {
      class: ExpressionClass.CONSTANT,
      type: ExpressionType.VALUE_CONSTANT,
      alias: '',
      query_location: 0,
      value: '1',
    };

    expect(validator(VALUE_CONSTANT_NODE)).toBe(true);
  });

  it('should return true for node type OPERATOR_CAST', () => {
    const OPERATOR_CAST_NODE: ParsedExpression = {
      class: ExpressionClass.CAST,
      type: ExpressionType.OPERATOR_CAST,
      alias: '',
      query_location: 7,
      child: {
        class: ExpressionClass.COLUMN_REF,
        type: ExpressionType.COLUMN_REF,
        alias: '',
        query_location: 12,
        column_names: ['partition_ts'],
      },
      cast_type: {
        id: 1,
      },
      try_cast: false,
    };

    expect(validator(OPERATOR_CAST_NODE)).toBe(true);
  });
});
