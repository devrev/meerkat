import { ConjunctionExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';
import { ExpressionClass, ExpressionType } from '../../types/duckdb-serialization-types/serialization/Expression';
import { containsDuckdbCondition, containsTransform } from './contains';

describe('Contains Transform Tests', () => {
  it('Should throw error if values are empty', () => {
    expect(() =>
      containsTransform({
        member: 'country',
        operator: 'contains',
        values: [],
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'string',
        },
      })
    ).toThrow();
  });

  it('Should create a simple Contains condition if there is only one value', () => {
    const expectedOutput = containsDuckdbCondition('country', 'US', {
      name: 'country',
      sql: 'table.country',
      type: 'string',
    });
    expect(
      containsTransform({
        member: 'country',
        operator: 'contains',
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
    const output = containsTransform({
      member: 'country',
      operator: 'contains',
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
