import {
  ExpressionClass,
  ExpressionType,
} from '../../types/duckdb-serialization-types/serialization/Expression';
import {
  ConjunctionExpression,
  FunctionExpression,
  OperatorExpression,
} from '../../types/duckdb-serialization-types/serialization/ParsedExpression';
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

  describe('Array type members', () => {
    const options: CreateColumnRefOptions = {
      isAlias: false,
    };

    it('Should use NOT(list_has_all) for string_array with single value', () => {
      const output = notEqualsTransform(
        {
          member: 'tags',
          operator: 'notEquals',
          values: ['bug'],
          memberInfo: {
            name: 'tags',
            sql: 'table.tags',
            type: 'string_array',
          },
        },
        options
      ) as OperatorExpression;
      expect(output.class).toEqual(ExpressionClass.OPERATOR);
      expect(output.type).toEqual(ExpressionType.OPERATOR_NOT);
      const inner = output.children[0] as FunctionExpression;
      expect(inner.class).toEqual(ExpressionClass.FUNCTION);
      expect(inner.function_name).toEqual('list_has_all');
    });

    it('Should use NOT(list_has_all) for string_array with multiple values', () => {
      const output = notEqualsTransform(
        {
          member: 'tags',
          operator: 'notEquals',
          values: ['bug', 'feature', 'enhancement'],
          memberInfo: {
            name: 'tags',
            sql: 'table.tags',
            type: 'string_array',
          },
        },
        options
      ) as OperatorExpression;
      expect(output.class).toEqual(ExpressionClass.OPERATOR);
      expect(output.type).toEqual(ExpressionType.OPERATOR_NOT);
      const inner = output.children[0] as FunctionExpression;
      expect(inner.function_name).toEqual('list_has_all');
      expect(inner.children.length).toEqual(2);
      const listValue = inner.children[1] as FunctionExpression;
      expect(listValue.function_name).toEqual('list_value');
      expect(listValue.children.length).toEqual(3);
    });

    it('Should use NOT(list_has_all) for number_array with multiple values', () => {
      const output = notEqualsTransform(
        {
          member: 'scores',
          operator: 'notEquals',
          values: [1, 2, 3],
          memberInfo: {
            name: 'scores',
            sql: 'table.scores',
            type: 'number_array',
          },
        },
        options
      ) as OperatorExpression;
      expect(output.class).toEqual(ExpressionClass.OPERATOR);
      expect(output.type).toEqual(ExpressionType.OPERATOR_NOT);
      const inner = output.children[0] as FunctionExpression;
      expect(inner.function_name).toEqual('list_has_all');
    });
  });
});
