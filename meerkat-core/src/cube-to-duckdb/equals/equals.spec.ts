import {
  ConjunctionExpression,
  ExpressionClass,
  ExpressionType,
} from '@devrev/duckdb-serialization-types';
import { equalsTransform, equalDuckdbCondition } from './equals'; // replace with your module name

describe('Equals Transform Tests', () => {
  it('Should throw error if values are empty', () => {
    expect(() =>
      equalsTransform({
        member: 'country',
        operator: 'equals',
        values: [],
      })
    ).toThrow();
  });

  it('Should create a simple equals condition if there is only one value', () => {
    const expectedOutput = equalDuckdbCondition('country', 'US');
    expect(
      equalsTransform({
        member: 'country',
        operator: 'equals',
        values: ['US'],
      })
    ).toEqual(expectedOutput);
  });

  it('Should create an OR condition if there are multiple values', () => {
    const output = equalsTransform({
      member: 'country',
      operator: 'equals',
      values: ['US', 'Germany', 'Israel'],
    }) as ConjunctionExpression;
    expect(output.class).toEqual(ExpressionClass.CONJUNCTION);
    expect(output.type).toEqual(ExpressionType.CONJUNCTION_OR);
    expect(output.children.length).toEqual(3);
  });
});
