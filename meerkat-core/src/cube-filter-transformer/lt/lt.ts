import { isQueryOperatorsWithSQLInfo } from '../../cube-to-duckdb/cube-filter-to-duckdb';
import { ExpressionType } from '../../types/duckdb-serialization-types/serialization/Expression';
import { baseDuckdbCondition } from '../base-condition-builder/base-condition-builder';
import { CubeToParseExpressionTransform } from '../factory';
import { orDuckdbCondition } from '../or/or';
import { getSQLExpressionAST } from '../sql-expression/sql-expression-parser';

export const ltTransform: CubeToParseExpressionTransform = (query, options) => {
  const { member } = query;

  // SQL expressions not supported for lt operator
  if (isQueryOperatorsWithSQLInfo(query)) {
    return getSQLExpressionAST(member, query.sqlExpression, 'lt', options);
  }

  // Otherwise, use values
  if (!query.values || query.values.length === 0) {
    throw new Error('lt filter must have at least one value');
  }

  const values = query.values;

  /**
   * If there is only one value, we can create a simple equals condition
   */
  if (values.length === 1) {
    return baseDuckdbCondition(
      member,
      ExpressionType.COMPARE_LESSTHAN,
      values[0],
      query.memberInfo,
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
        ExpressionType.COMPARE_LESSTHAN,
        value,
        query.memberInfo,
        options
      )
    );
  });
  return orCondition;
};
