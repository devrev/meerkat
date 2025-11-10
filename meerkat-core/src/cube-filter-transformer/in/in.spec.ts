import { ConjunctionExpression } from '../../types/duckdb-serialization-types/serialization/ParsedExpression';
import { inTransform } from './in';

describe('In transforms Tests', () => {
  it('Should throw error if values are undefined', () => {
    expect(() =>
      inTransform({
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
    const result = inTransform({
      member: 'country',
      operator: 'in',
      values: ['US', 'Canada', 'Mexico'],
      memberInfo: {
        name: 'country',
        sql: 'table.country',
        type: 'string',
      },
    });

    // Check it returns a subquery structure with string_split
    expect(result).toHaveProperty('class', 'SUBQUERY');
    expect(result).toHaveProperty('type', 'SUBQUERY');
    expect(result).toHaveProperty('subquery_type', 'ANY');

    // Verify it's using string_split
    const selectList = (result as any).subquery.node.select_list[0];
    expect(selectList.function_name).toBe('unnest');
    expect(selectList.children[0].function_name).toBe('string_split');

    // Verify no CAST for strings
    expect(selectList.type).toBe('FUNCTION');
  });

  it('Should return optimized string_split approach with CAST for number type', () => {
    const result = inTransform({
      member: 'order_id',
      operator: 'in',
      values: [1, 2, 3],
      memberInfo: {
        name: 'order_id',
        sql: 'table.order_id',
        type: 'number',
      },
    });

    // Check it returns a subquery structure
    expect(result).toHaveProperty('class', 'SUBQUERY');
    expect(result).toHaveProperty('type', 'SUBQUERY');
    expect(result).toHaveProperty('subquery_type', 'ANY');

    // Verify it's using string_split with CAST
    const selectList = (result as any).subquery.node.select_list[0];
    expect(selectList.type).toBe('OPERATOR_CAST');
    expect(selectList.cast_type.id).toBe('DOUBLE');
    expect(selectList.child.function_name).toBe('unnest');
    expect(selectList.child.children[0].function_name).toBe('string_split');
  });

  it('Should return standard ARRAY_CONSTRUCTOR for string_array type', () => {
    const output = inTransform({
      member: 'country',
      operator: 'in',
      values: ['US', 'Germany', 'Israel'],
      memberInfo: {
        name: 'country',
        sql: 'table.country',
        type: 'string_array',
      },
    }) as ConjunctionExpression;

    // For array types, should use && operator with ARRAY_CONSTRUCTOR
    expect(output.function_name).toBe('&&');
    expect(output.children[1].type).toBe('ARRAY_CONSTRUCTOR');
    expect(output.children[1].children.length).toBe(3);
  });

  it('Should return standard COMPARE_IN for other types (default case)', () => {
    const output = inTransform({
      member: 'some_field',
      operator: 'in',
      values: ['val1', 'val2'],
      memberInfo: {
        name: 'some_field',
        sql: 'table.some_field',
        type: 'time' as any, // Unknown type to trigger default case
      },
    });

    // Default case should use COMPARE_IN
    expect(output).toHaveProperty('type', 'COMPARE_IN');
    expect(output).toHaveProperty('class', 'OPERATOR');
    expect((output as any).children.length).toBe(3); // column + 2 values
  });

  it('Should handle large value lists efficiently with string_split', () => {
    const largeValueList = Array.from({ length: 1000 }, (_, i) => `value${i}`);
    const result = inTransform({
      member: 'country',
      operator: 'in',
      values: largeValueList,
      memberInfo: {
        name: 'country',
        sql: 'table.country',
        type: 'string',
      },
    });

    // Should still use subquery approach
    expect(result).toHaveProperty('class', 'SUBQUERY');

    // Verify only 2 VALUE_CONSTANT nodes (joined string + delimiter)
    const selectList = (result as any).subquery.node.select_list[0];
    const stringSplitChildren = selectList.children[0].children;
    expect(stringSplitChildren.length).toBe(2);
    expect(stringSplitChildren[0].value.value).toContain('§‡¶'); // Contains delimiter
  });

  it('Should use delimiter to join values', () => {
    const result = inTransform({
      member: 'country',
      operator: 'in',
      values: ['US', 'Canada'],
      memberInfo: {
        name: 'country',
        sql: 'table.country',
        type: 'string',
      },
    });

    const selectList = (result as any).subquery.node.select_list[0];
    const joinedValue = selectList.children[0].children[0].value.value;
    const delimiter = selectList.children[0].children[1].value.value;

    expect(delimiter).toBe('§‡¶');
    expect(joinedValue).toBe('US§‡¶Canada');
  });

  it('Should handle the original test case structure for reference', () => {
    const output = inTransform({
      member: 'country',
      operator: 'in',
      values: ['US', 'Germany', 'Israel'],
      memberInfo: {
        name: 'country',
        sql: 'table.country',
        type: 'string_array',
      },
    }) as ConjunctionExpression;
    expect(output).toEqual({
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
    });
  });

  it('Should throw error if values array is empty', () => {
    expect(() =>
      inTransform({
        member: 'country',
        operator: 'in',
        values: [],
        memberInfo: {
          name: 'country',
          sql: 'table.country',
          type: 'string',
        },
      })
    ).toThrow('In filter must have at least one value');
  });
});
