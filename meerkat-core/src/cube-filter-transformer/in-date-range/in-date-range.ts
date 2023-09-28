import { ExpressionType } from '../../types/duckdb-serialization-types/serialization/Expression';
import { andDuckdbCondition } from '../and/and';
import { baseDuckdbCondition } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';

export const inDataRangeTransform: CubeToParseExpressionTransform = (query) => {
  const { member, values } = query;

  if (!values || values.length === 0) {
    throw new Error('GT filter must have at least one value');
  }

  /**
   * If there are multiple values, we need to create an OR condition
   */
  const andCondition = andDuckdbCondition();

  andCondition.children.push(
    baseDuckdbCondition(
      member,
      ExpressionType.COMPARE_GREATERTHANOREQUALTO,
      values[0],
      query.memberInfo
    )
  );

  andCondition.children.push(
    baseDuckdbCondition(
      member,
      ExpressionType.COMPARE_LESSTHANOREQUALTO,
      values[1],
      query.memberInfo
    )
  );

  return andCondition;
};
