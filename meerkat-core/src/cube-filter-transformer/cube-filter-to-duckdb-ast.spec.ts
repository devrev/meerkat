import type {
  QueryFiltersWithInfo,
  QueryOperatorsWithInfo,
} from '../cube-to-duckdb/cube-filter-to-duckdb';
import type { SelectNode } from '../types/duckdb-serialization-types/serialization/QueryNode';
import { getBaseAST } from '../utils/base-ast';
import { CreateColumnRefOptions } from './base-condition-builder/base-condition-builder';
import { cubeFilterToDuckdbAST } from './factory';

describe('CubeFilterToDuckDBAST', () => {
  const cubeFilter: QueryFiltersWithInfo = [
    {
      and: [
        {
          member: 'name',
          operator: 'equals',
          values: ['1'],
          memberInfo: {
            name: 'name',
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
                name: 'dev_oid',
                sql: 'dev_oid',
                type: 'string',
              },
            },
            {
              member: 'dev_oid',
              operator: 'equals',
              values: ['DEV-1'],
              memberInfo: {
                name: 'dev_oid',
                sql: 'dev_oid',
                type: 'string',
              },
            },
          ],
        },
      ],
    },
  ];

  describe('root filter arrays', () => {
    const amount: QueryOperatorsWithInfo = {
      member: 'amount',
      operator: 'gt',
      values: ['1'],
      memberInfo: { name: 'amount', sql: 'amount', type: 'number' },
    };
    const cohort = (value: string): QueryOperatorsWithInfo => ({
      member: 'cohort',
      operator: 'equals',
      values: [value],
      memberInfo: { name: 'cohort', sql: 'cohort', type: 'string' },
    });

    it.each([false, true])(
      'conjoins root entries without flattening nested OR (isAlias=%s)',
      (isAlias) => {
        const ast = getBaseAST();
        const filters: QueryFiltersWithInfo = [
          amount,
          { or: [cohort('a'), cohort('b')] },
        ];
        const before = structuredClone(filters);
        const output = cubeFilterToDuckdbAST(filters, ast, { isAlias });
        expect(output).toMatchObject({
          type: 'CONJUNCTION_AND',
          children: [
            { type: 'COMPARE_GREATERTHAN' },
            {
              type: 'CONJUNCTION_OR',
              children: [{ type: 'COMPARE_EQUAL' }, { type: 'COMPARE_EQUAL' }],
            },
          ],
        });
        expect(filters).toEqual(before);
        expect((ast.node as SelectNode).where_clause).toBeNull();
      }
    );

    it('retains an existing WHERE as a separate conjunct without mutating it', () => {
      const ast = getBaseAST();
      const existing = cubeFilterToDuckdbAST(
        [{ or: [cohort('a'), cohort('b')] }],
        ast,
        { isAlias: false }
      );
      if (!existing) throw new Error('Expected an existing WHERE predicate');
      (ast.node as SelectNode).where_clause = existing;
      const before = structuredClone(existing);
      const output = cubeFilterToDuckdbAST([amount], ast, { isAlias: false });
      expect(output).toMatchObject({
        type: 'CONJUNCTION_AND',
        children: [{ type: 'CONJUNCTION_OR' }, { type: 'COMPARE_GREATERTHAN' }],
      });
      expect(existing).toEqual(before);
      expect((ast.node as SelectNode).where_clause).toBe(existing);
    });

    it('ignores empty logical entries while retaining all root predicates', () => {
      const output = cubeFilterToDuckdbAST(
        [{ and: [] }, amount, cohort('a')],
        getBaseAST(),
        { isAlias: false }
      );
      expect(output).toMatchObject({
        type: 'CONJUNCTION_AND',
        children: [{ type: 'COMPARE_GREATERTHAN' }, { type: 'COMPARE_EQUAL' }],
      });
    });

    it('omits root groups whose entries are all empty', () => {
      expect(
        cubeFilterToDuckdbAST([{ and: [] }, { or: [] }], getBaseAST(), {
          isAlias: false,
        })
      ).toBeNull();
    });

    it('omits recursively empty groups without dropping sibling predicates', () => {
      const empty: QueryFiltersWithInfo = [{ and: [{ or: [] }] }];
      expect(
        cubeFilterToDuckdbAST(empty, getBaseAST(), { isAlias: false })
      ).toBeNull();
      expect(
        cubeFilterToDuckdbAST([...empty, amount], getBaseAST(), {
          isAlias: false,
        })
      ).toMatchObject({
        type: 'CONJUNCTION_AND',
        children: [{ type: 'COMPARE_GREATERTHAN' }],
      });
    });

    it('keeps the existing WHERE for an empty filter array', () => {
      const ast = getBaseAST();
      const existing = cubeFilterToDuckdbAST([amount], ast, { isAlias: false });
      if (!existing) throw new Error('Expected an existing WHERE predicate');
      (ast.node as SelectNode).where_clause = existing;
      expect(cubeFilterToDuckdbAST([], ast, { isAlias: false })).toBe(existing);
      expect(
        cubeFilterToDuckdbAST([], getBaseAST(), { isAlias: false })
      ).toBeNull();
    });
  });

  describe('isAlias: false (base column refs)', () => {
    const options: CreateColumnRefOptions = {
      isAlias: false,
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
              name: 'name',
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

  describe('isAlias: true (projection alias refs)', () => {
    const options: CreateColumnRefOptions = {
      isAlias: true,
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
              name: 'name',
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
