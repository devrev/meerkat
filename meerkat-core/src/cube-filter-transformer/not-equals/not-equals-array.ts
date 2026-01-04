import { isQueryOperatorsWithSQLInfo } from '../../cube-to-duckdb/cube-filter-to-duckdb';
import { equalsArrayTransform } from '../equals/equals-array';
import { CubeToParseExpressionTransform } from '../factory';
import { notDuckdbCondition } from '../not/not';

export const notEqualsArrayTransform: CubeToParseExpressionTransform = (
  query,
  options
) => {
  // SQL expressions not supported for notEquals operator
  if (isQueryOperatorsWithSQLInfo(query)) {
    throw new Error(
      'SQL expressions are not supported for notEquals operator. Only "in" and "notIn" operators support SQL expressions.'
    );
  }

  if (!query.values || query.values.length === 0) {
    throw new Error('Equals filter must have at least one value');
  }

  const notWrapper = notDuckdbCondition();
  const equalsCondition = equalsArrayTransform(query, options);
  /**
   * We need to wrap the equals condition in a not condition
   * Which basically means ! of ANY of the values
   */
  notWrapper.children = [equalsCondition];

  return notWrapper;
};
