import { likeTransform } from './like';

describe('like transforms Tests', () => {
  it('Should throw error if values are undefined', () => {
    expect(() =>
      likeTransform({
        member: 'country',
        operator: 'contains',
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'string',
        },
      })
    ).toThrow();
  });

  it('Should throw error for non string member', () => {
    expect(() =>
      likeTransform({
        member: 'country',
        operator: 'contains',
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'boolean',
        },
      })
    ).toThrow();
    expect(() =>
      likeTransform({
        member: 'country',
        operator: 'contains',
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'number',
        },
      })
    ).toThrow();
    expect(() =>
      likeTransform({
        member: 'country',
        operator: 'contains',
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'number_array',
        },
      })
    ).toThrow();
    expect(() =>
      likeTransform({
        member: 'country',
        operator: 'contains',
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'string_array',
        },
      })
    ).toThrow();
    expect(() =>
      likeTransform({
        member: 'country',
        operator: 'contains',
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'time',
        },
      })
    ).toThrow();
  });

  it('Should return the correct value for string member', () => {
    expect(
      likeTransform({
        member: 'country',
        operator: 'contains',
        values: ['US'],
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'string',
        },
      })
    ).toEqual({
      alias: '',
      catalog: '',
      children: [
        {
          alias: '',
          class: 'COLUMN_REF',
          column_names: ['country'],
          type: 'COLUMN_REF',
        },
        {
          alias: '',
          class: 'CONSTANT',
          type: 'VALUE_CONSTANT',
          value: {
            is_null: false,
            type: {
              id: 'VARCHAR',
              type_info: null,
            },
            value: 'US',
          },
        },
      ],
      class: 'FUNCTION',
      distinct: false,
      export_state: false,
      filter: null,
      function_name: '~~',
      is_operator: true,
      order_bys: {
        orders: [],
        type: 'ORDER_MODIFIER',
      },
      schema: '',
      type: 'FUNCTION',
    });
  });

  it('Should return the correct value for multiple string member', () => {
    expect(
      likeTransform({
        member: 'country',
        operator: 'contains',
        values: ['US', '%Germany%'],
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'string',
        },
      })
    ).toEqual({
      alias: '',
      children: [
        {
          alias: '',
          catalog: '',
          children: [
            {
              alias: '',
              class: 'COLUMN_REF',
              column_names: ['country'],
              type: 'COLUMN_REF',
            },
            {
              alias: '',
              class: 'CONSTANT',
              type: 'VALUE_CONSTANT',
              value: {
                is_null: false,
                type: { id: 'VARCHAR', type_info: null },
                value: 'US',
              },
            },
          ],
          class: 'FUNCTION',
          distinct: false,
          export_state: false,
          filter: null,
          function_name: '~~',
          is_operator: true,
          order_bys: { orders: [], type: 'ORDER_MODIFIER' },
          schema: '',
          type: 'FUNCTION',
        },
        {
          alias: '',
          catalog: '',
          children: [
            {
              alias: '',
              class: 'COLUMN_REF',
              column_names: ['country'],
              type: 'COLUMN_REF',
            },
            {
              alias: '',
              class: 'CONSTANT',
              type: 'VALUE_CONSTANT',
              value: {
                is_null: false,
                type: { id: 'VARCHAR', type_info: null },
                value: '%Germany%',
              },
            },
          ],
          class: 'FUNCTION',
          distinct: false,
          export_state: false,
          filter: null,
          function_name: '~~',
          is_operator: true,
          order_bys: { orders: [], type: 'ORDER_MODIFIER' },
          schema: '',
          type: 'FUNCTION',
        },
      ],
      class: 'CONJUNCTION',
      type: 'CONJUNCTION_OR',
    });
  });
});
