import { isQueryOperatorsWithSQLInfo } from '../../cube-to-duckdb/cube-filter-to-duckdb';
import { ExpressionType } from '../../types/duckdb-serialization-types/serialization/Expression';
import { andDuckdbCondition } from '../and/and';
import { baseDuckdbCondition } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';

export const inDataRangeTransform: CubeToParseExpressionTransform = (query) => {
  const { member } = query;

  // SQL expressions not supported for inDateRange operator
  if (isQueryOperatorsWithSQLInfo(query)) {
    throw new Error(
      'SQL expressions are not supported for inDateRange operator. Only "in" and "notIn" operators support SQL expressions.'
    );
  }

  // Otherwise, use values
  if (!query.values || query.values.length === 0) {
    throw new Error('GT filter must have at least one value');
  }

  const values = query.values;

  /**
   * If there are multiple values, we need to create an AND condition
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
