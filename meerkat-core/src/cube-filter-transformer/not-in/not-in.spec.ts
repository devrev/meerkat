import { ConjunctionExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';
import { notInTransform } from './not-in';

describe('Not In transforms Tests', () => {
  it('Should throw error if values are undefined', () => {
    expect(() =>
      notInTransform({
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

  it('Should return optimized string_split approach for string type', () => {
    const result = notInTransform({
      member: 'country',
      operator: 'notIn',
      values: ['US', 'Canada', 'Mexico'],
      memberInfo: {
        name: 'country',
        sql: 'table.country',
        type: 'string',
      },
    });

    // Check it returns OPERATOR_NOT wrapping a subquery
    expect(result).toHaveProperty('class', 'OPERATOR');
    expect(result).toHaveProperty('type', 'OPERATOR_NOT');
    expect(result.children[0]).toHaveProperty('class', 'SUBQUERY');

    // Verify it's using string_split
    const subquery = (result as any).children[0];
    const selectList = subquery.subquery.node.select_list[0];
    expect(selectList.function_name).toBe('unnest');
    expect(selectList.children[0].function_name).toBe('string_split');

    // Verify no CAST for strings
    expect(selectList.type).toBe('FUNCTION');
  });

  it('Should return optimized string_split approach with CAST for number type', () => {
    const result = notInTransform({
      member: 'order_id',
      operator: 'notIn',
      values: [1, 2, 3],
      memberInfo: {
        name: 'order_id',
        sql: 'table.order_id',
        type: 'number',
      },
    });

    // Check it returns OPERATOR_NOT wrapping a subquery
    expect(result).toHaveProperty('class', 'OPERATOR');
    expect(result).toHaveProperty('type', 'OPERATOR_NOT');

    // Verify it's using string_split with CAST
    const subquery = (result as any).children[0];
    const selectList = subquery.subquery.node.select_list[0];
    expect(selectList.type).toBe('OPERATOR_CAST');
    expect(selectList.cast_type.id).toBe('DOUBLE');
    expect(selectList.child.function_name).toBe('unnest');
    expect(selectList.child.children[0].function_name).toBe('string_split');
  });

  it('Should return standard COMPARE_NOT_IN for other types (default case)', () => {
    const output = notInTransform({
      member: 'some_field',
      operator: 'notIn',
      values: ['val1', 'val2'],
      memberInfo: {
        name: 'some_field',
        sql: 'table.some_field',
        type: 'time' as any, // Unknown type to trigger default case
      },
    });

    // Default case should use COMPARE_NOT_IN
    expect(output).toHaveProperty('type', 'COMPARE_NOT_IN');
    expect(output).toHaveProperty('class', 'OPERATOR');
    expect((output as any).children.length).toBe(3); // column + 2 values
  });

  it('Should handle large value lists efficiently with string_split', () => {
    const largeValueList = Array.from({ length: 1000 }, (_, i) => `value${i}`);
    const result = notInTransform({
      member: 'country',
      operator: 'notIn',
      values: largeValueList,
      memberInfo: {
        name: 'country',
        sql: 'table.country',
        type: 'string',
      },
    });

    // Should use OPERATOR_NOT wrapping subquery
    expect(result).toHaveProperty('type', 'OPERATOR_NOT');
    expect(result.children[0]).toHaveProperty('class', 'SUBQUERY');

    // Verify only 2 VALUE_CONSTANT nodes (joined string + delimiter)
    const subquery = (result as any).children[0];
    const selectList = subquery.subquery.node.select_list[0];
    const stringSplitChildren = selectList.children[0].children;
    expect(stringSplitChildren.length).toBe(2);
    expect(stringSplitChildren[0].value.value).toContain('§§');
  });

  it('Should use delimiter to join values', () => {
    const result = notInTransform({
      member: 'country',
      operator: 'notIn',
      values: ['US', 'Canada'],
      memberInfo: {
        name: 'country',
        sql: 'table.country',
        type: 'string',
      },
    });

    const subquery = (result as any).children[0];
    const selectList = subquery.subquery.node.select_list[0];
    const joinedValue = selectList.children[0].children[0].value.value;
    const delimiter = selectList.children[0].children[1].value.value;

    expect(delimiter).toBe('§§');
    expect(joinedValue).toBe('US§§Canada');
  });

  it('Should return the correct value for string_array member', () => {
    const output = notInTransform({
      member: 'country',
      operator: 'contains',
      values: ['US', 'Germany', 'Israel'],
      memberInfo: {
        name: 'country',
        sql: 'table.country',
        type: 'string_array',
      },
    }) as ConjunctionExpression;
    expect(output).toEqual({
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
              children: [
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
                    value: 'Germany',
                  },
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
                    value: 'Israel',
                  },
                },
              ],
              class: 'OPERATOR',
              type: 'ARRAY_CONSTRUCTOR',
            },
          ],
          class: 'FUNCTION',
          distinct: false,
          export_state: false,
          filter: null,
          function_name: '&&',
          is_operator: true,
          order_bys: {
            orders: [],
            type: 'ORDER_MODIFIER',
          },
          schema: '',
          type: 'FUNCTION',
        },
      ],
      class: 'OPERATOR',
      type: 'OPERATOR_NOT',
    });
  });

  it('Should throw error if values array is empty', () => {
    expect(() =>
      notInTransform({
        member: 'country',
        operator: 'notIn',
        values: [],
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'string',
        },
      })
    ).toThrow('Not in filter must have at least one value');
  });

  it('Should throw error if values contain the reserved delimiter', () => {
    expect(() =>
      notInTransform({
        member: 'country',
        operator: 'notIn',
        values: ['US', 'has§§delimiter', 'Mexico'],
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'string',
        },
      })
    ).toThrow("Filter values cannot contain the reserved delimiter '§§'");
  });

  it('Should throw error if numeric values contain the reserved delimiter', () => {
    // This could happen if someone passes a string representation of a number
    expect(() =>
      notInTransform({
        member: 'order_id',
        operator: 'notIn',
        values: ['123§§456', '789'],
        memberInfo: {
          name: 'order_id',
          sql: 'table.order_id',
          type: 'number',
        },
      })
    ).toThrow("Filter values cannot contain the reserved delimiter '§§'");
  });
});
