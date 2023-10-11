import { equalsArrayTransform } from '../equals/equals-array';
import { CubeToParseExpressionTransform } from '../factory';
import { notDuckdbCondition } from '../not/not';

export const notEqualsArrayTransform: CubeToParseExpressionTransform = (
  query
) => {
  const { values } = query;

  if (!values || values.length === 0) {
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
