import { ConjunctionExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';
import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { CreateColumnRefOptions } from '../base-condition-builder/base-condition-builder';
import {
  notContainsDuckdbCondition,
  notContainsTransform,
} from './not-contains';

const defaultOptions: CreateColumnRefOptions = {
  isAlias: false,
  useDotNotation: false,
};

describe('Contains Transform Tests', () => {
  it('Should throw error if values are empty', () => {
    expect(() =>
      notContainsTransform(
        {
          member: 'country',
          operator: 'notContains',
          values: [],
          memberInfo: {
            name: 'country',
            sql: 'table.country',
            type: 'string',
          },
        },
        defaultOptions
      )
    ).toThrow();
  });

  it('Should create a simple Contains condition if there is only one value', () => {
    const expectedOutput = notContainsDuckdbCondition(
      'country',
      'US',
      {
        name: 'country',
        sql: 'table.country',
        type: 'string',
      },
      defaultOptions
    );
    expect(
      notContainsTransform(
        {
          member: 'country',
          operator: 'notContains',
          values: ['US'],
          memberInfo: {
            name: 'country',
            sql: 'table.country',
            type: 'string',
          },
        },
        defaultOptions
      )
    ).toEqual(expectedOutput);
  });

  it('Should create an OR condition if there are multiple values', () => {
    const output = notContainsTransform(
      {
        member: 'country',
        operator: 'notContains',
        values: ['US', 'Germany', 'Israel'],
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'string',
        },
      },
      defaultOptions
    ) as ConjunctionExpression;
    expect(output.class).toEqual(ExpressionClass.CONJUNCTION);
    expect(output.type).toEqual(ExpressionType.CONJUNCTION_OR);
    expect(output.children.length).toEqual(3);
  });
});
