import { ConjunctionExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';
import { ExpressionClass, ExpressionType } from '../../types/duckdb-serialization-types/serialization/Expression';
import { baseDuckdbCondition } from '../base-condition-builder/base-condition-builder';
import { notEqualsTransform } from './not-equals';

describe('Not Equals Transform Tests', () => {
  it('Should throw error if values are empty', () => {
    expect(() =>
      notEqualsTransform({
        member: 'country',
        operator: 'notEquals',
        values: [],
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'string',
        },
      })
    ).toThrow();
  });

  it('Should create a simple equals condition if there is only one value', () => {
    const expectedOutput = baseDuckdbCondition(
      'country',
      ExpressionType.COMPARE_NOTEQUAL,
      'US',
      {
        name: 'country',
        sql: 'table.country',
        type: 'string',
      }
    );
    expect(
      notEqualsTransform({
        member: 'country',
        operator: 'notEquals',
        values: ['US'],
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'string',
        },
      })
    ).toEqual(expectedOutput);
  });

  it('Should create an OR condition if there are multiple values', () => {
    const output = notEqualsTransform({
      member: 'country',
      operator: 'notEquals',
      values: ['US', 'Germany', 'Israel'],
      memberInfo: {
        name: 'country',
        sql: 'table.country',
        type: 'string',
      },
    }) as ConjunctionExpression;
    expect(output.class).toEqual(ExpressionClass.CONJUNCTION);
    expect(output.type).toEqual(ExpressionType.CONJUNCTION_OR);
    expect(output.children.length).toEqual(3);
  });
});
