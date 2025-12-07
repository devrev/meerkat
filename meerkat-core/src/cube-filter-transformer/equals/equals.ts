import { isQueryOperatorsWithSQLInfo } from '../../cube-to-duckdb/cube-filter-to-duckdb';
import { ExpressionType } from '../../types/duckdb-serialization-types/serialization/Expression';
import { isArrayTypeMember } from '../../utils/is-array-member-type';
import { andDuckdbCondition } from '../and/and';
import { baseDuckdbCondition } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';
import { getSQLExpressionAST } from '../sql-expression/sql-expression-parser';
import { equalsArrayTransform } from './equals-array';

export const equalsTransform: CubeToParseExpressionTransform = (query) => {
  const { member, memberInfo } = query;

  // SQL expressions not supported for equals operator
  if (isQueryOperatorsWithSQLInfo(query)) {
    return getSQLExpressionAST(member, query.sqlExpression, 'equals');
  }

  const values = query.values;

  /**
   * If the member is an array, we need to use the array transform
   */
  if (isArrayTypeMember(memberInfo.type)) {
    return equalsArrayTransform(query);
  }
  if (!values || values.length === 0) {
    throw new Error('Equals filter must have at least one value');
  }
  /**
   * If there is only one value, we can create a simple equals condition
   */
  if (values.length === 1) {
    return baseDuckdbCondition(
      member,
      ExpressionType.COMPARE_EQUAL,
      values[0],
      query.memberInfo
    );
  }

  /**
   * If there are multiple values, we need to create an AND condition
   */
  const andCondition = andDuckdbCondition();
  values.forEach((value) => {
    andCondition.children.push(
      baseDuckdbCondition(
        member,
        ExpressionType.COMPARE_EQUAL,
        value,
        query.memberInfo
      )
    );
  });
  return andCondition;
};
