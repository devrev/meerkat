import { isQueryOperatorsWithSQLInfo } from '../../cube-to-duckdb/cube-filter-to-duckdb';
import { ExpressionType } from '../../types/duckdb-serialization-types/serialization/Expression';
import { isArrayTypeMember } from '../../utils/is-array-member-type';
import {
  baseDuckdbCondition,
  CreateColumnRefOptions,
} from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';
import { orDuckdbCondition } from '../or/or';
import { getSQLExpressionAST } from '../sql-expression/sql-expression-parser';
import { notEqualsArrayTransform } from './not-equals-array';

export const notEqualsTransform: CubeToParseExpressionTransform = (
  query,
  options
) => {
  const { member, memberInfo } = query;

  // SQL expressions not supported for notEquals operator
  if (isQueryOperatorsWithSQLInfo(query)) {
    return getSQLExpressionAST(member, query.sqlExpression, 'notEquals');
  }

  // Otherwise, use values
  if (!query.values || query.values.length === 0) {
    throw new Error('NotEquals filter must have at least one value');
  }

  const values = query.values;

  /**
   * If the member is an array, we need to use the array transform
   */
  if (isArrayTypeMember(memberInfo.type)) {
    return notEqualsArrayTransform(query, options);
  }

  /**
   * If there is only one value, we can create a simple equals condition
   */
  if (values.length === 1) {
    return baseDuckdbCondition(
      member,
      ExpressionType.COMPARE_NOTEQUAL,
      values[0],
      memberInfo,
      options
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
        ExpressionType.COMPARE_NOTEQUAL,
        value,
        memberInfo,
        options
      )
    );
  });
  return orCondition;
};
