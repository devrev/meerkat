import { CreateColumnRefOptions } from '../base-condition-builder/base-condition-builder';
import { setTransform } from './set';

const defaultOptions: CreateColumnRefOptions = {
  isAlias: false,
  useDotNotation: false,
};

describe('setTransform', () => {
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

    const result = setTransform(query, defaultOptions);

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

    const result = setTransform(query, defaultOptions);

    expect(result).toEqual(expected);
  });
});
