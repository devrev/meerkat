import {
  ConjunctionExpression,
  ExpressionClass,
  ExpressionType,
} from '@devrev/duckdb-serialization-types';
import { equalsTransform } from './equals'; // replace with your module name
import { baseDuckdbCondition } from '../base-condition-builder/base-condition-builder';

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
    const expectedOutput = baseDuckdbCondition(
      'country',
      ExpressionType.COMPARE_EQUAL,
      'US'
    );
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
