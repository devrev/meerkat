import { CreateColumnRefOptions } from '../base-condition-builder/base-condition-builder';
import { setTransform } from './set';

describe('setTransform', () => {
  describe('isAlias: false (base column refs)', () => {
    const options: CreateColumnRefOptions = {
      isAlias: false,
    };

    it('should return the correct expression object', () => {
      const query = {
        member: 'table.column',
      };

      const expected = {
        class: 'OPERATOR',
        type: 'OPERATOR_IS_NOT_NULL',
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

      const result = setTransform(query, options);

      expect(result).toEqual(expected);
    });

    it('should handle __ delimited query', () => {
      const query = {
        member: 'table__column',
      };

      const expected = {
        class: 'OPERATOR',
        type: 'OPERATOR_IS_NOT_NULL',
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

      const result = setTransform(query, options);

      expect(result).toEqual(expected);
    });
  });

  describe('isAlias: true (projection alias refs)', () => {
    const options: CreateColumnRefOptions = {
      isAlias: true,
    };

    it('should return the correct expression object with alias', () => {
      const query = {
        member: 'table.column',
      };

      const expected = {
        class: 'OPERATOR',
        type: 'OPERATOR_IS_NOT_NULL',
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

      const result = setTransform(query, options);

      expect(result).toEqual(expected);
    });

    it('should handle __ delimited query with alias', () => {
      const query = {
        member: 'table__column',
      };

      const expected = {
        class: 'OPERATOR',
        type: 'OPERATOR_IS_NOT_NULL',
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

      const result = setTransform(query, options);

      expect(result).toEqual(expected);
    });
  });
});
