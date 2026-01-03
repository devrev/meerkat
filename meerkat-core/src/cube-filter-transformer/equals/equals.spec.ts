import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { ConjunctionExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';
import {
  baseDuckdbCondition,
  CreateColumnRefOptions,
} from '../base-condition-builder/base-condition-builder';
import { equalsTransform } from './equals'; // replace with your module name

describe('Equals Transform Tests', () => {
  describe('useDotNotation: false', () => {
    const options: CreateColumnRefOptions = {
      isAlias: false,
    };

    it('Should throw error if values are empty', () => {
      expect(() =>
        equalsTransform(
          {
            member: 'country',
            operator: 'equals',
            values: [],
            memberInfo: {
              sql: 'temp.country',
              type: 'string',
            },
          },
          options
        )
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
        },
        options
      );
      expect(
        equalsTransform(
          {
            member: 'country',
            operator: 'equals',
            values: ['US'],
            memberInfo: {
              sql: 'temp.country',
              type: 'string',
            },
          },
          options
        )
      ).toEqual(expectedOutput);
    });

    it('Should create an OR condition if there are multiple values', () => {
      const output = equalsTransform(
        {
          member: 'country',
          operator: 'equals',
          values: ['US', 'Germany', 'Israel'],
          memberInfo: {
            sql: 'temp.country',
            type: 'string',
          },
        },
        options
      ) as ConjunctionExpression;
      expect(output.class).toEqual(ExpressionClass.CONJUNCTION);
      expect(output.type).toEqual(ExpressionType.CONJUNCTION_AND);
      expect(output.children.length).toEqual(3);
    });
  });

  describe('useDotNotation: true', () => {
    const options: CreateColumnRefOptions = {
      isAlias: true,
    };

    it('Should throw error if values are empty', () => {
      expect(() =>
        equalsTransform(
          {
            member: 'country',
            operator: 'equals',
            values: [],
            memberInfo: {
              sql: 'temp.country',
              type: 'string',
            },
          },
          options
        )
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
        },
        options
      );
      expect(
        equalsTransform(
          {
            member: 'country',
            operator: 'equals',
            values: ['US'],
            memberInfo: {
              sql: 'temp.country',
              type: 'string',
            },
          },
          options
        )
      ).toEqual(expectedOutput);
    });

    it('Should create an OR condition if there are multiple values', () => {
      const output = equalsTransform(
        {
          member: 'country',
          operator: 'equals',
          values: ['US', 'Germany', 'Israel'],
          memberInfo: {
            sql: 'temp.country',
            type: 'string',
          },
        },
        options
      ) as ConjunctionExpression;
      expect(output.class).toEqual(ExpressionClass.CONJUNCTION);
      expect(output.type).toEqual(ExpressionType.CONJUNCTION_AND);
      expect(output.children.length).toEqual(3);
    });
  });
});
