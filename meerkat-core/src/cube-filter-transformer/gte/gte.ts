import { isQueryOperatorsWithSQLInfo } from '../../cube-to-duckdb/cube-filter-to-duckdb';
import { ExpressionType } from '../../types/duckdb-serialization-types/serialization/Expression';
import { baseDuckdbCondition } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';
import { orDuckdbCondition } from '../or/or';
import { getSQLExpressionAST } from '../sql-expression/sql-expression-parser';

export const gteTransform: CubeToParseExpressionTransform = (query) => {
  const { member } = query;

  // SQL expressions not supported for gte operator
  if (isQueryOperatorsWithSQLInfo(query)) {
    return getSQLExpressionAST(member, query.sqlExpression, 'gte');
  }

  // Otherwise, use values
  if (!query.values || query.values.length === 0) {
    throw new Error('GTE filter must have at least one value');
  }

  const { values } = query;

  /**
   * If there is only one value, we can create a simple equals condition
   */
  if (values.length === 1) {
    return baseDuckdbCondition(
      member,
      ExpressionType.COMPARE_GREATERTHANOREQUALTO,
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
        ExpressionType.COMPARE_GREATERTHANOREQUALTO,
        value,
        query.memberInfo
      )
    );
  });
  return orCondition;
};
