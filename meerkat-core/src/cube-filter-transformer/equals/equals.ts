import { Member, QueryFilter } from '@devrev/cube-types';
import { ParsedExpression } from '@devrev/duckdb-serialization-types';
import { ExpressionType } from 'duckdb-serialization-types/src/serialization/Expression';
import { baseDuckdbCondition } from '../base-condition-builder/base-condition-builder';
import { orDuckdbCondition } from '../or/or';

export interface EqualsFilters extends QueryFilter {
  member: Member;
  operator: 'equals';
  values: string[];
}

export const equalsTransform = (query: EqualsFilters): ParsedExpression => {
  const { member, values } = query;

  if (!values || values.length === 0) {
    throw new Error('Equals filter must have at least one value');
  }

  /**
   * If there is only one value, we can create a simple equals condition
   */
  if (values.length === 1) {
    return baseDuckdbCondition(member, ExpressionType.COMPARE_EQUAL, values[0]);
  }

  /**
   * If there are multiple values, we need to create an OR condition
   */
  const orCondition = orDuckdbCondition();
  values.forEach((value) => {
    orCondition.children.push(
      baseDuckdbCondition(member, ExpressionType.COMPARE_EQUAL, value)
    );
  });
  return orCondition;
};
