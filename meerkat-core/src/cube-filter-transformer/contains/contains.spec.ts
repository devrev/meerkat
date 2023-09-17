import {
  ConjunctionExpression,
  ExpressionClass,
  ExpressionType,
} from '@devrev/duckdb-serialization-types';
import { containsDuckdbCondition, containsTransform } from './contains';

describe('Contains Transform Tests', () => {
  it('Should throw error if values are empty', () => {
    expect(() =>
      containsTransform({
        member: 'country',
        operator: 'contains',
        values: [],
      })
    ).toThrow();
  });

  it('Should create a simple Contains condition if there is only one value', () => {
    const expectedOutput = containsDuckdbCondition('country', 'US');
    expect(
      containsTransform({
        member: 'country',
        operator: 'contains',
        values: ['US'],
      })
    ).toEqual(expectedOutput);
  });

  it('Should create an OR condition if there are multiple values', () => {
    const output = containsTransform({
      member: 'country',
      operator: 'contains',
      values: ['US', 'Germany', 'Israel'],
    }) as ConjunctionExpression;
    expect(output.class).toEqual(ExpressionClass.CONJUNCTION);
    expect(output.type).toEqual(ExpressionType.CONJUNCTION_OR);
    expect(output.children.length).toEqual(3);
  });
});
