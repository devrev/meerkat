import { Member, QueryFilter } from '@devrev/cube-types';
import {
  ConjunctionExpression,
  ParsedExpression,
} from '@devrev/duckdb-serialization-types';
import {
  ExpressionClass,
  ExpressionType,
} from 'duckdb-serialization-types/src/serialization/Expression';
import { orDuckdbCondition } from '../or/or';

export interface EqualsFilters extends QueryFilter {
  member: Member;
  operator: 'equals';
  values: string[];
}

export const equalDuckdbCondition = (columnName: string, value: string) => {
  return {
    class: ExpressionClass.COMPARISON,
    type: ExpressionType.COMPARE_EQUAL,
    alias: '',
    left: {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      column_names: [columnName],
    },
    right: {
      class: ExpressionClass.COLUMN_REF,
      type: ExpressionType.COLUMN_REF,
      alias: '',
      column_names: [value],
    },
  };
};

export const equalsTransform = (query: EqualsFilters): ParsedExpression => {
  const { member, values } = query;

  if (!values || values.length === 0) {
    throw new Error('Equals filter must have at least one value');
  }

  /**
   * If there is only one value, we can create a simple equals condition
   */
  if (values.length === 1) {
    return equalDuckdbCondition(member, values[0]);
  }

  /**
   * If there are multiple values, we need to create an OR condition
   */
  const orCondition = orDuckdbCondition();
  values.forEach((value) => {
    orCondition.children.push(equalDuckdbCondition(member, value));
  });
  return orCondition;
};
