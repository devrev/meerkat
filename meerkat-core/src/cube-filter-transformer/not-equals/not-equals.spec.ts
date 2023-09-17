import {
  ConjunctionExpression,
  ExpressionClass,
  ExpressionType,
} from '@devrev/duckdb-serialization-types';
import { baseDuckdbCondition } from '../base-condition-builder/base-condition-builder';
import { notEqualsTransform } from './not-equals';

describe('Not Equals Transform Tests', () => {
  it('Should throw error if values are empty', () => {
    expect(() =>
      notEqualsTransform({
        member: 'country',
        operator: 'notEquals',
        values: [],
      })
    ).toThrow();
  });

  it('Should create a simple equals condition if there is only one value', () => {
    const expectedOutput = baseDuckdbCondition(
      'country',
      ExpressionType.COMPARE_NOTEQUAL,
      'US'
    );
    expect(
      notEqualsTransform({
        member: 'country',
        operator: 'notEquals',
        values: ['US'],
      })
    ).toEqual(expectedOutput);
  });

  it('Should create an OR condition if there are multiple values', () => {
    const output = notEqualsTransform({
      member: 'country',
      operator: 'notEquals',
      values: ['US', 'Germany', 'Israel'],
    }) as ConjunctionExpression;
    expect(output.class).toEqual(ExpressionClass.CONJUNCTION);
    expect(output.type).toEqual(ExpressionType.CONJUNCTION_OR);
    expect(output.children.length).toEqual(3);
  });
});
