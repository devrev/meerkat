import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { ConjunctionExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';
import { CreateColumnRefOptions } from '../base-condition-builder/base-condition-builder';
import {
  notContainsDuckdbCondition,
  notContainsTransform,
} from './not-contains';

describe('Not Contains Transform Tests', () => {
  describe('isAlias: false (base column refs)', () => {
    const options: CreateColumnRefOptions = {
      isAlias: false,
    };

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
          options
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
        options
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
          options
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
        options
      ) as ConjunctionExpression;
      expect(output.class).toEqual(ExpressionClass.CONJUNCTION);
      expect(output.type).toEqual(ExpressionType.CONJUNCTION_OR);
      expect(output.children.length).toEqual(3);
    });
  });

  describe('isAlias: true (projection alias refs)', () => {
    const options: CreateColumnRefOptions = {
      isAlias: true,
    };

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
          options
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
        options
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
          options
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
        options
      ) as ConjunctionExpression;
      expect(output.class).toEqual(ExpressionClass.CONJUNCTION);
      expect(output.type).toEqual(ExpressionType.CONJUNCTION_OR);
      expect(output.children.length).toEqual(3);
    });
  });
});
