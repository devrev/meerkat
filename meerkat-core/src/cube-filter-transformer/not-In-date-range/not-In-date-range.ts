import { ExpressionType } from '../../types/duckdb-serialization-types/serialization/Expression';
import { baseDuckdbCondition } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';
import { orDuckdbCondition } from '../or/or';

export const notInDataRangeTransform: CubeToParseExpressionTransform = (
  query
) => {
  const { member, values } = query;

  if (!values || values.length === 0) {
    throw new Error('GT filter must have at least one value');
  }

  /**
   * If there are multiple values, we need to create an OR condition
   */
  const andCondition = orDuckdbCondition();

  andCondition.children.push(
    baseDuckdbCondition(
      member,
      ExpressionType.COMPARE_LESSTHAN,
      values[0],
      query.memberInfo
    )
  );

  andCondition.children.push(
    baseDuckdbCondition(
      member,
      ExpressionType.COMPARE_GREATERTHAN,
      values[1],
      query.memberInfo
    )
  );

  return andCondition;
};
