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
  describe('isAlias: false (base column refs)', () => {
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

    // ISS-245695: notEquals with multiple values should use NOT IN (not OR)
    it('Should use NOT IN for multiple values to correctly exclude all', () => {
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
      // Should produce (NOT IN (...)) OR (IS NULL) — same as notIn
      expect(output.class).toEqual(ExpressionClass.CONJUNCTION);
      expect(output.type).toEqual(ExpressionType.CONJUNCTION_OR);
      expect(output.children.length).toEqual(2);
      // First child: NOT IN condition
      expect(output.children[0].type).toEqual(ExpressionType.COMPARE_NOT_IN);
      // Second child: IS NULL for proper null handling
      expect(output.children[1].type).toEqual(ExpressionType.OPERATOR_IS_NULL);
    });
  });

  describe('isAlias: true (projection alias refs)', () => {
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

    it('Should create a simple notEquals condition if there is only one value', () => {
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

    // ISS-245695: notEquals with multiple values should use NOT IN (not OR)
    it('Should use NOT IN for multiple values to correctly exclude all', () => {
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
      expect(output.children.length).toEqual(2);
      expect(output.children[0].type).toEqual(ExpressionType.COMPARE_NOT_IN);
      expect(output.children[1].type).toEqual(ExpressionType.OPERATOR_IS_NULL);
    });
  });
});
