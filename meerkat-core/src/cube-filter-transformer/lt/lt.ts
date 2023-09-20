import { ExpressionType } from '@devrev/duckdb-serialization-types';
import { baseDuckdbCondition } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';
import { orDuckdbCondition } from '../or/or';

export const ltTransform: CubeToParseExpressionTransform = (query) => {
  const { member, values } = query;

  if (!values || values.length === 0) {
    throw new Error('lt filter must have at least one value');
  }

  /**
   * If there is only one value, we can create a simple equals condition
   */
  if (values.length === 1) {
    return baseDuckdbCondition(
      member,
      ExpressionType.COMPARE_LESSTHAN,
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
      baseDuckdbCondition(
        member,
        ExpressionType.COMPARE_LESSTHAN,
        value,
        query.memberInfo
      )
    );
  });
  return orCondition;
};
