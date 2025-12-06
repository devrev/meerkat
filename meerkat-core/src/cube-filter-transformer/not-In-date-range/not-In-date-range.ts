import { isQueryOperatorsWithSQLInfo } from '../../cube-to-duckdb/cube-filter-to-duckdb';
import { ExpressionType } from '../../types/duckdb-serialization-types/serialization/Expression';
import { baseDuckdbCondition } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';
import { orDuckdbCondition } from '../or/or';

export const notInDataRangeTransform: CubeToParseExpressionTransform = (
  query
) => {
  const { member } = query;

  // SQL expressions not supported for notInDateRange operator
  if (isQueryOperatorsWithSQLInfo(query)) {
    throw new Error(
      'SQL expressions are not supported for notInDateRange operator. Only "in" and "notIn" operators support SQL expressions.'
    );
  }

  // Otherwise, use values
  if (!query.values || query.values.length === 0) {
    throw new Error('GT filter must have at least one value');
  }

  const values = query.values;

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
