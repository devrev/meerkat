import { CreateColumnRefOptions } from '../base-condition-builder/base-condition-builder';
import { notSetTransform } from './not-set';

describe('notSetTransform', () => {
  describe('useDotNotation: false', () => {
    const options: CreateColumnRefOptions = {
      isAlias: false,
    };

    it('should return the correct expression for a given query', () => {
      const query = {
        member: 'table.column',
      };

      const expectedExpression = {
        class: 'OPERATOR',
        type: 'OPERATOR_IS_NULL',
        alias: '',
        children: [
          {
            class: 'COLUMN_REF',
            type: 'COLUMN_REF',
            alias: '',
            column_names: ['table', 'column'],
          },
        ],
      };

      const result = notSetTransform(query, options);

      expect(result).toEqual(expectedExpression);
    });

    it('should return the correct expression for a __ delimited query', () => {
      const query = {
        member: 'table__column',
      };

      const expectedExpression = {
        class: 'OPERATOR',
        type: 'OPERATOR_IS_NULL',
        alias: '',
        children: [
          {
            class: 'COLUMN_REF',
            type: 'COLUMN_REF',
            alias: '',
            column_names: ['table__column'],
          },
        ],
      };

      const result = notSetTransform(query, options);

      expect(result).toEqual(expectedExpression);
    });
  });

  describe('useDotNotation: true', () => {
    const options: CreateColumnRefOptions = {
      isAlias: true,
    };

    it('should return the correct expression for a given query with alias', () => {
      const query = {
        member: 'table.column',
      };

      const expectedExpression = {
        class: 'OPERATOR',
        type: 'OPERATOR_IS_NULL',
        alias: '',
        children: [
          {
            class: 'COLUMN_REF',
            type: 'COLUMN_REF',
            alias: '',
            column_names: ['table.column'],
          },
        ],
      };

      const result = notSetTransform(query, options);

      expect(result).toEqual(expectedExpression);
    });

    it('should return the correct expression for a __ delimited query with alias', () => {
      const query = {
        member: 'table__column',
      };

      const expectedExpression = {
        class: 'OPERATOR',
        type: 'OPERATOR_IS_NULL',
        alias: '',
        children: [
          {
            class: 'COLUMN_REF',
            type: 'COLUMN_REF',
            alias: '',
            column_names: ['table__column'],
          },
        ],
      };

      const result = notSetTransform(query, options);

      expect(result).toEqual(expectedExpression);
    });
  });
});
