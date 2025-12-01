import { isQueryOperatorsWithSQLInfo } from '../../cube-to-duckdb/cube-filter-to-duckdb';
import { equalsArrayTransform } from '../equals/equals-array';
import { CubeToParseExpressionTransform } from '../factory';
import { notDuckdbCondition } from '../not/not';
import { getSQLExpressionAST } from '../sql-expression/sql-expression-parser';

export const notEqualsArrayTransform: CubeToParseExpressionTransform = (
  query
) => {
  // Check if this is a SQL expression
  if (isQueryOperatorsWithSQLInfo(query)) {
    return getSQLExpressionAST(query.member, query.sqlExpression, 'notEquals');
  }

  if (!query.values || query.values.length === 0) {
    throw new Error('Equals filter must have at least one value');
  }

  const notWrapper = notDuckdbCondition();
  const equalsCondition = equalsArrayTransform(query);
  /**
   * We need to wrap the equals condition in a not condition
   * Which basically means ! of ANY of the values
   */
  notWrapper.children = [equalsCondition];

  return notWrapper;
};
