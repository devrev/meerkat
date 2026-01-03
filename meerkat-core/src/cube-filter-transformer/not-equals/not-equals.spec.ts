import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { ConjunctionExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';
import {
  baseDuckdbCondition,
  CreateColumnRefOptions,
} from '../base-condition-builder/base-condition-builder';
import { notEqualsTransform } from './not-equals';

describe('Not Equals Transform Tests', () => {
  describe('useDotNotation: false', () => {
    const options: CreateColumnRefOptions = {
      isAlias: false,
    };

    it('Should throw error if values are empty', () => {
      expect(() =>
        notEqualsTransform(
          {
            member: 'country',
            operator: 'notEquals',
            values: [],
            memberInfo: {
              name: 'country',
              sql: 'table.country',
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
        ExpressionType.COMPARE_NOTEQUAL,
        'US',
        {
          name: 'country',
          sql: 'table.country',
          type: 'string',
        },
        options
      );
      expect(
        notEqualsTransform(
          {
            member: 'country',
            operator: 'notEquals',
            values: ['US'],
            memberInfo: {
              name: 'country',
              sql: 'table.country',
              type: 'string',
            },
          },
          options
        )
      ).toEqual(expectedOutput);
    });

    it('Should create an OR condition if there are multiple values', () => {
      const output = notEqualsTransform(
        {
          member: 'country',
          operator: 'notEquals',
          values: ['US', 'Germany', 'Israel'],
          memberInfo: {
            name: 'country',
            sql: 'table.country',
            type: 'string',
          },
        },
        options
      ) as ConjunctionExpression;
      expect(output.class).toEqual(ExpressionClass.CONJUNCTION);
      expect(output.type).toEqual(ExpressionType.CONJUNCTION_OR);
      expect(output.children.length).toEqual(3);
    });
  });

  describe('useDotNotation: true', () => {
    const options: CreateColumnRefOptions = {
      isAlias: true,
    };

    it('Should throw error if values are empty', () => {
      expect(() =>
        notEqualsTransform(
          {
            member: 'country',
            operator: 'notEquals',
            values: [],
            memberInfo: {
              name: 'country',
              sql: 'table.country',
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
        ExpressionType.COMPARE_NOTEQUAL,
        'US',
        {
          name: 'country',
          sql: 'table.country',
          type: 'string',
        },
        options
      );
      expect(
        notEqualsTransform(
          {
            member: 'country',
            operator: 'notEquals',
            values: ['US'],
            memberInfo: {
              name: 'country',
              sql: 'table.country',
              type: 'string',
            },
          },
          options
        )
      ).toEqual(expectedOutput);
    });

    it('Should create an OR condition if there are multiple values', () => {
      const output = notEqualsTransform(
        {
          member: 'country',
          operator: 'notEquals',
          values: ['US', 'Germany', 'Israel'],
          memberInfo: {
            name: 'country',
            sql: 'table.country',
            type: 'string',
          },
        },
        options
      ) as ConjunctionExpression;
      expect(output.class).toEqual(ExpressionClass.CONJUNCTION);
      expect(output.type).toEqual(ExpressionType.CONJUNCTION_OR);
      expect(output.children.length).toEqual(3);
    });
  });
});
