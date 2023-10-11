import { ExpressionType } from '../../types/duckdb-serialization-types/serialization/Expression';
import { baseArrayDuckdbCondition } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';
import { orDuckdbCondition } from '../or/or';

export const equalsArrayTransform: CubeToParseExpressionTransform = (query) => {
  const { member, values } = query;

  if (!values || values.length === 0) {
    throw new Error('Equals filter must have at least one value');
  }

  /**
   * If there is only one value, we can create a simple equals condition
   */
  if (values.length === 1) {
    return baseArrayDuckdbCondition(
      member,
      ExpressionType.COMPARE_EQUAL,
      values[0],
      query.memberInfo
    );
  }

  /**
   * If there are multiple values, we need to create an OR condition
   */
  const orCondition = orDuckdbCondition();
  values.forEach((value) => {
    orCondition.children.push(
      baseArrayDuckdbCondition(
        member,
        ExpressionType.COMPARE_EQUAL,
        value,
        query.memberInfo
      )
    );
  });
  return orCondition;
};
