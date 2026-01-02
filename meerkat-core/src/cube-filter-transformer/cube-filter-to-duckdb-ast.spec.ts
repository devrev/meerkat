import { CreateColumnRefOptions } from './base-condition-builder/base-condition-builder';
import { getBaseAST } from '../utils/base-ast';
import { cubeFilterToDuckdbAST } from './factory';

describe('CubeFilterToDuckDBAST', () => {
  const cubeFilter = [
    {
      and: [
        {
          member: 'name',
          operator: 'equals',
          values: ['1'],
          memberInfo: {
            sql: 'name',
            type: 'number',
          },
        },
        {
          or: [
            {
              member: 'dev_oid',
              operator: 'equals',
              values: ['DEV-0'],
              memberInfo: {
                sql: 'dev_oid',
                type: 'string',
              },
            },
            {
              member: 'dev_oid',
              operator: 'equals',
              values: ['DEV-1'],
              memberInfo: {
                sql: 'dev_oid',
                type: 'string',
              },
            },
          ],
        },
      ],
    },
  ];

  describe('useDotNotation: false', () => {
    const options: CreateColumnRefOptions = {
      isAlias: false,
      useDotNotation: false,
    };

    it('should return a duckdb AST', () => {
      const ast = getBaseAST();

      const output = cubeFilterToDuckdbAST(cubeFilter, ast, options);
      expect(output).toEqual({
        class: 'CONJUNCTION',
        type: 'CONJUNCTION_AND',
        alias: '',
        children: [
          {
            class: 'COMPARISON',
            type: 'COMPARE_EQUAL',
            alias: '',
            left: {
              class: 'COLUMN_REF',
              type: 'COLUMN_REF',
              alias: '',
              column_names: ['name'],
            },
            right: {
              class: 'CONSTANT',
              type: 'VALUE_CONSTANT',
              alias: '',
              value: {
                type: {
                  id: 'DECIMAL',
                  type_info: {
                    type: 'DECIMAL_TYPE_INFO',
                    alias: '',
                    width: 1,
                    scale: 0,
                  },
                },
                is_null: false,
                value: 1,
              },
            },
          },
          {
            class: 'CONJUNCTION',
            type: 'CONJUNCTION_OR',
            alias: '',
            children: [
              {
                class: 'COMPARISON',
                type: 'COMPARE_EQUAL',
                alias: '',
                left: {
                  class: 'COLUMN_REF',
                  type: 'COLUMN_REF',
                  alias: '',
                  column_names: ['dev_oid'],
                },
                right: {
                  class: 'CONSTANT',
                  type: 'VALUE_CONSTANT',
                  alias: '',
                  value: {
                    type: {
                      id: 'VARCHAR',
                      type_info: null,
                    },
                    is_null: false,
                    value: 'DEV-0',
                  },
                },
              },
              {
                class: 'COMPARISON',
                type: 'COMPARE_EQUAL',
                alias: '',
                left: {
                  class: 'COLUMN_REF',
                  type: 'COLUMN_REF',
                  alias: '',
                  column_names: ['dev_oid'],
                },
                right: {
                  class: 'CONSTANT',
                  type: 'VALUE_CONSTANT',
                  alias: '',
                  value: {
                    type: {
                      id: 'VARCHAR',
                      type_info: null,
                    },
                    is_null: false,
                    value: 'DEV-1',
                  },
                },
              },
            ],
          },
        ],
      });
    });

    it('Simple filter without and/or', () => {
      const ast = getBaseAST();

      const output = cubeFilterToDuckdbAST(
        [
          {
            member: 'name',
            operator: 'equals',
            values: ['1'],
            memberInfo: {
              sql: 'name',
              type: 'number',
            },
          },
        ],
        ast,
        options
      );

      expect(output).toEqual({
        class: 'COMPARISON',
        type: 'COMPARE_EQUAL',
        alias: '',
        left: {
          class: 'COLUMN_REF',
          type: 'COLUMN_REF',
          alias: '',
          column_names: ['name'],
        },
        right: {
          class: 'CONSTANT',
          type: 'VALUE_CONSTANT',
          alias: '',
          value: {
            type: {
              id: 'DECIMAL',
              type_info: {
                type: 'DECIMAL_TYPE_INFO',
                alias: '',
                width: 1,
                scale: 0,
              },
            },
            is_null: false,
            value: 1,
          },
        },
      });
    });
  });

  describe('useDotNotation: true', () => {
    const options: CreateColumnRefOptions = {
      isAlias: true,
      useDotNotation: true,
    };

    it('should return a duckdb AST with alias column names', () => {
      const ast = getBaseAST();

      const output = cubeFilterToDuckdbAST(cubeFilter, ast, options);
      expect(output).toEqual({
        class: 'CONJUNCTION',
        type: 'CONJUNCTION_AND',
        alias: '',
        children: [
          {
            class: 'COMPARISON',
            type: 'COMPARE_EQUAL',
            alias: '',
            left: {
              class: 'COLUMN_REF',
              type: 'COLUMN_REF',
              alias: '',
              column_names: ['name'],
            },
            right: {
              class: 'CONSTANT',
              type: 'VALUE_CONSTANT',
              alias: '',
              value: {
                type: {
                  id: 'DECIMAL',
                  type_info: {
                    type: 'DECIMAL_TYPE_INFO',
                    alias: '',
                    width: 1,
                    scale: 0,
                  },
                },
                is_null: false,
                value: 1,
              },
            },
          },
          {
            class: 'CONJUNCTION',
            type: 'CONJUNCTION_OR',
            alias: '',
            children: [
              {
                class: 'COMPARISON',
                type: 'COMPARE_EQUAL',
                alias: '',
                left: {
                  class: 'COLUMN_REF',
                  type: 'COLUMN_REF',
                  alias: '',
                  column_names: ['dev_oid'],
                },
                right: {
                  class: 'CONSTANT',
                  type: 'VALUE_CONSTANT',
                  alias: '',
                  value: {
                    type: {
                      id: 'VARCHAR',
                      type_info: null,
                    },
                    is_null: false,
                    value: 'DEV-0',
                  },
                },
              },
              {
                class: 'COMPARISON',
                type: 'COMPARE_EQUAL',
                alias: '',
                left: {
                  class: 'COLUMN_REF',
                  type: 'COLUMN_REF',
                  alias: '',
                  column_names: ['dev_oid'],
                },
                right: {
                  class: 'CONSTANT',
                  type: 'VALUE_CONSTANT',
                  alias: '',
                  value: {
                    type: {
                      id: 'VARCHAR',
                      type_info: null,
                    },
                    is_null: false,
                    value: 'DEV-1',
                  },
                },
              },
            ],
          },
        ],
      });
    });

    it('Simple filter without and/or', () => {
      const ast = getBaseAST();

      const output = cubeFilterToDuckdbAST(
        [
          {
            member: 'name',
            operator: 'equals',
            values: ['1'],
            memberInfo: {
              sql: 'name',
              type: 'number',
            },
          },
        ],
        ast,
        options
      );

      expect(output).toEqual({
        class: 'COMPARISON',
        type: 'COMPARE_EQUAL',
        alias: '',
        left: {
          class: 'COLUMN_REF',
          type: 'COLUMN_REF',
          alias: '',
          column_names: ['name'],
        },
        right: {
          class: 'CONSTANT',
          type: 'VALUE_CONSTANT',
          alias: '',
          value: {
            type: {
              id: 'DECIMAL',
              type_info: {
                type: 'DECIMAL_TYPE_INFO',
                alias: '',
                width: 1,
                scale: 0,
              },
            },
            is_null: false,
            value: 1,
          },
        },
      });
    });
  });
});
