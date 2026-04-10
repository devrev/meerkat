import { CreateColumnRefOptions } from '../base-condition-builder/base-condition-builder';
import { arrayEmptyTransform } from './array-empty';

describe('arrayEmptyTransform', () => {
  describe('isAlias: false (base column refs)', () => {
    const options: CreateColumnRefOptions = {
      isAlias: false,
    };

    it('should return (col IS NULL OR len(col) = 0)', () => {
      const query = {
        member: 'table.column',
      };

      const result = arrayEmptyTransform(query, options);

      expect(result).toEqual({
        class: 'CONJUNCTION',
        type: 'CONJUNCTION_OR',
        alias: '',
        children: [
          {
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
          },
          {
            class: 'COMPARISON',
            type: 'COMPARE_EQUAL',
            alias: '',
            left: {
              class: 'FUNCTION',
              type: 'FUNCTION',
              alias: '',
              function_name: 'len',
              schema: '',
              children: [
                {
                  class: 'COLUMN_REF',
                  type: 'COLUMN_REF',
                  alias: '',
                  column_names: ['table', 'column'],
                },
              ],
              filter: null,
              order_bys: { type: 'ORDER_MODIFIER', orders: [] },
              distinct: false,
              is_operator: false,
              export_state: false,
              catalog: '',
            },
            right: {
              class: 'CONSTANT',
              type: 'VALUE_CONSTANT',
              alias: '',
              value: {
                type: { id: 'INTEGER', type_info: null },
                is_null: false,
                value: 0,
              },
            },
          },
        ],
      });
    });
  });

  describe('isAlias: true (projection alias refs)', () => {
    const options: CreateColumnRefOptions = {
      isAlias: true,
    };

    it('should use alias-style column names', () => {
      const query = {
        member: 'table.column',
      };

      const result = arrayEmptyTransform(query, options);

      // Both the IS NULL and len() children should use alias column names
      const isNullChild = (result as any).children[0];
      const lenChild = (result as any).children[1];

      expect(isNullChild.children[0].column_names).toEqual(['table.column']);
      expect(lenChild.left.children[0].column_names).toEqual(['table.column']);
    });
  });

  describe('type validation', () => {
    const options: CreateColumnRefOptions = { isAlias: false };

    it('should return IS NULL for non-array types', () => {
      const query = {
        member: 'table.column',
        memberInfo: { name: 'column', type: 'string' as const, sql: 'column' },
      };

      const result = arrayEmptyTransform(query, options);

      expect(result).toEqual({
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
      });
    });

    it('should not throw if memberInfo type is string_array', () => {
      const query = {
        member: 'table.column',
        memberInfo: { name: 'column', type: 'string_array' as const, sql: 'column' },
      };

      expect(() => arrayEmptyTransform(query, options)).not.toThrow();
    });

    it('should not throw if memberInfo is not present', () => {
      const query = {
        member: 'table.column',
      };

      expect(() => arrayEmptyTransform(query, options)).not.toThrow();
    });
  });
});
