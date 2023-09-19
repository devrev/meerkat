import {
  ConjunctionExpression,
  ExpressionClass,
  ExpressionType,
} from '@devrev/duckdb-serialization-types';
import { baseDuckdbCondition } from '../base-condition-builder/base-condition-builder';
import { equalsTransform } from './equals'; // replace with your module name

describe('Equals Transform Tests', () => {
  it('Should throw error if values are empty', () => {
    expect(() =>
      equalsTransform({
        member: 'country',
        operator: 'equals',
        values: [],
        memberInfo: {
          sql: 'temp.country',
          type: 'string',
        },
      })
    ).toThrow();
  });

  it('Should create a simple equals condition if there is only one value', () => {
    const expectedOutput = baseDuckdbCondition(
      'country',
      ExpressionType.COMPARE_EQUAL,
      'US',
      {
        sql: 'temp.country',
        type: 'string',
      }
    );
    expect(
      equalsTransform({
        member: 'country',
        operator: 'equals',
        values: ['US'],
        memberInfo: {
          sql: 'temp.country',
          type: 'string',
        },
      })
    ).toEqual(expectedOutput);
  });

  it('Should create an OR condition if there are multiple values', () => {
    const output = equalsTransform({
      member: 'country',
      operator: 'equals',
      values: ['US', 'Germany', 'Israel'],
      memberInfo: {
        sql: 'temp.country',
        type: 'string',
      },
    }) as ConjunctionExpression;
    expect(output.class).toEqual(ExpressionClass.CONJUNCTION);
    expect(output.type).toEqual(ExpressionType.CONJUNCTION_OR);
    expect(output.children.length).toEqual(3);
  });
});
