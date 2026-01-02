import { Query } from '../types/cube-types/query';
import { TableSchema } from '../types/cube-types/table';
import {
  LimitModifier,
  OrderModifier,
  QueryNodeType,
  ResultModifierType,
} from '../types/duckdb-serialization-types';
import { isSelectNode } from '../types/utils';
import { cubeToDuckdbAST } from './ast-builder';

const defaultOptions = {
  filterType: 'PROJECTION_FILTER' as const,
  config: { useDotNotation: false },
};

describe('cubeToDuckdbAST', () => {
  const mockTableSchema: TableSchema = {
    name: 'test_table',
    sql: 'test_table',
    measures: [
      {
        name: 'measure1',
        sql: 'test_table.measure1',
        type: 'number',
      },
    ],
    dimensions: [
      {
        name: 'dimension1',
        sql: 'test_table.dimension1',
        type: 'string',
      },
    ],
  };

  it('should return null if table schema is null', () => {
    const result = cubeToDuckdbAST(
      {} as Query,
      null as unknown as TableSchema,
      defaultOptions
    );
    expect(result).toBeNull();
  });

  it('should handle basic query', () => {
    const query: Query = {
      measures: ['test_table.measure1'],
      dimensions: ['test_table.dimension1'],
    };

    const result = cubeToDuckdbAST(query, mockTableSchema, defaultOptions);
    expect(result).not.toBeNull();
    expect(result.node.type).toBe(QueryNodeType.SELECT_NODE);
    expect(result.node.group_expressions).toHaveLength(1);
    expect(result.node.group_expressions).toEqual([
      {
        alias: '',
        class: 'COLUMN_REF',
        column_names: ['test_table__dimension1'],
        type: 'COLUMN_REF',
      },
    ]);
  });

  it('should handle filters for dimensions (WHERE clause)', () => {
    const query: Query = {
      measures: ['test_table.measure1'],
      dimensions: ['test_table.dimension1'],
      filters: [
        {
          member: 'test_table.dimension1',
          operator: 'equals',
          values: ['value1'],
        },
      ],
    };

    const result = cubeToDuckdbAST(query, mockTableSchema, defaultOptions);
    expect(result).not.toBeNull();
    expect(result.node.type).toBe(QueryNodeType.SELECT_NODE);
    expect(result.node.where_clause).toEqual({
      alias: '',
      class: 'COMPARISON',
      left: {
        alias: '',
        class: 'COLUMN_REF',
        column_names: ['test_table__dimension1'],
        type: 'COLUMN_REF',
      },
      right: {
        alias: '',
        class: 'CONSTANT',
        type: 'VALUE_CONSTANT',
        value: {
          is_null: false,
          type: {
            id: 'VARCHAR',
            type_info: null,
          },
          value: 'value1',
        },
      },
      type: 'COMPARE_EQUAL',
    });
  });

  it('should handle filters for measures (HAVING clause)', () => {
    const query: Query = {
      measures: ['test_table.measure1'],
      dimensions: ['test_table.dimension1'],
      filters: [
        {
          member: 'test_table.measure1',
          operator: 'gt',
          values: ['100'],
        },
      ],
    };

    const result = cubeToDuckdbAST(query, mockTableSchema, defaultOptions);
    expect(result.node.type).toBe(QueryNodeType.SELECT_NODE);
    expect(result.node.having).toEqual({
      alias: '',
      class: 'COMPARISON',
      left: {
        alias: '',
        class: 'COLUMN_REF',
        column_names: ['test_table__measure1'],
        type: 'COLUMN_REF',
      },
      right: {
        alias: '',
        class: 'CONSTANT',
        type: 'COMPARE_GREATERTHAN',
        type: 'VALUE_CONSTANT',
        value: {
          is_null: false,
          type: {
            id: 'DECIMAL',
            type_info: {
              alias: '',
              scale: 0,
              type: 'DECIMAL_TYPE_INFO',
              width: 3,
            },
          },
          value: 100,
        },
      },
      type: 'COMPARE_GREATERTHAN',
    });
  });

  it('should handle order by clause', () => {
    const query: Query = {
      measures: ['test_table.measure1'],
      dimensions: ['test_table.dimension1'],
      order: {
        'test_table.dimension1': 'asc',
      },
    };

    const result = cubeToDuckdbAST(query, mockTableSchema, defaultOptions);
    expect(result).not.toBeNull();
    expect(result.node.type).toBe(QueryNodeType.SELECT_NODE);
    if (isSelectNode(result.node)) {
      const orderModifier = result.node.modifiers[0] as OrderModifier;
      expect(orderModifier.type).toBe(ResultModifierType.ORDER_MODIFIER);
      expect(orderModifier.orders[0].expression).toEqual({
        alias: '',
        class: 'COLUMN_REF',
        column_names: ['test_table__dimension1'],
        type: 'COLUMN_REF',
      });
    }
  });

  it('should handle limit and offset', () => {
    const query: Query = {
      measures: ['test_table.measure1'],
      dimensions: ['test_table.dimension1'],
      limit: 10,
      offset: 5,
    };

    const result = cubeToDuckdbAST(query, mockTableSchema, defaultOptions);
    expect(result).not.toBeNull();
    expect(result.node.type).toBe(QueryNodeType.SELECT_NODE);
    if (isSelectNode(result.node)) {
      expect(result.node.modifiers).toHaveLength(1);
      const limitModifier = result.node.modifiers[0] as LimitModifier;
      expect(limitModifier).toEqual({
        limit: {
          alias: '',
          class: 'CONSTANT',
          type: 'VALUE_CONSTANT',
          value: {
            is_null: false,
            type: {
              id: 'INTEGER',
              type_info: null,
            },
            value: 10,
          },
        },
        offset: {
          alias: '',
          class: 'CONSTANT',
          type: 'VALUE_CONSTANT',
          value: {
            is_null: false,
            type: {
              id: 'INTEGER',
              type_info: null,
            },
            value: 5,
          },
        },
        type: 'LIMIT_MODIFIER',
      });
    }
  });

  it('should handle complex query', () => {
    const query: Query = {
      measures: ['test_table.measure1'],
      dimensions: ['test_table.dimension1'],
      filters: [
        {
          member: 'test_table.dimension1',
          operator: 'equals',
          values: ['value1'],
        },
        {
          member: 'test_table.measure1',
          operator: 'gt',
          values: ['100'],
        },
      ],
      order: {
        'test_table.dimension1': 'asc',
        'test_table.measure1': 'desc',
      },
      limit: 10,
      offset: 5,
    };

    const result = cubeToDuckdbAST(query, mockTableSchema, defaultOptions);
    expect(result).not.toBeNull();
    expect(result.node.type).toBe(QueryNodeType.SELECT_NODE);
    expect(result.node.group_expressions).toHaveLength(1);
    expect(result.node.group_expressions[0]).toEqual({
      alias: '',
      class: 'COLUMN_REF',
      column_names: ['test_table__dimension1'],
      type: 'COLUMN_REF',
    });
    expect(result.node.where_clause).toEqual({
      alias: '',
      class: 'COMPARISON',
      left: {
        alias: '',
        class: 'COLUMN_REF',
        column_names: ['test_table__dimension1'],
        type: 'COLUMN_REF',
      },
      right: {
        alias: '',
        class: 'CONSTANT',
        type: 'VALUE_CONSTANT',
        value: {
          is_null: false,
          type: {
            id: 'VARCHAR',
            type_info: null,
          },
          value: 'value1',
        },
      },
      type: 'COMPARE_EQUAL',
    });
    expect(result.node.having).toEqual({
      alias: '',
      class: 'COMPARISON',
      left: {
        alias: '',
        class: 'COLUMN_REF',
        column_names: ['test_table__measure1'],
        type: 'COLUMN_REF',
      },
      right: {
        alias: '',
        class: 'CONSTANT',
        type: 'VALUE_CONSTANT',
        value: {
          is_null: false,
          type: {
            id: 'DECIMAL',
            type_info: {
              alias: '',
              scale: 0,
              type: 'DECIMAL_TYPE_INFO',
              width: 3,
            },
          },
          value: 100,
        },
      },
      type: 'COMPARE_GREATERTHAN',
    });
    expect(result.node.modifiers).toHaveLength(2);
    const orderModifier = result.node.modifiers[0] as OrderModifier;
    expect(orderModifier.type).toBe(ResultModifierType.ORDER_MODIFIER);
    expect(orderModifier.orders).toHaveLength(2);
    expect(orderModifier.orders[0].expression).toEqual({
      alias: '',
      class: 'COLUMN_REF',
      column_names: ['test_table__dimension1'],
      type: 'COLUMN_REF',
    });
    expect(orderModifier.orders[1].expression).toEqual({
      alias: '',
      class: 'COLUMN_REF',
      column_names: ['test_table__measure1'],
      type: 'COLUMN_REF',
    });
    const limitModifier = result.node.modifiers[1] as LimitModifier;
    expect(limitModifier).toEqual({
      limit: {
        alias: '',
        class: 'CONSTANT',
        type: 'VALUE_CONSTANT',
        value: {
          is_null: false,
          type: { id: 'INTEGER', type_info: null },
          value: 10,
        },
      },
      offset: {
        alias: '',
        class: 'CONSTANT',
        type: 'VALUE_CONSTANT',
        value: {
          is_null: false,
          type: { id: 'INTEGER', type_info: null },
          value: 5,
        },
      },
      type: 'LIMIT_MODIFIER',
    });
  });

  it('should handle dimensions with multiple dots in their names', () => {
    const complexTableSchema: TableSchema = {
      name: 'test_table',
      sql: 'test_table',
      measures: [
        {
          name: 'measure.with.dots',
          sql: 'test_table.measure.with.dots',
          type: 'number',
        },
      ],
      dimensions: [
        {
          name: 'dimension.with.dots',
          sql: 'test_table.dimension.with.dots',
          type: 'string',
        },
      ],
    };

    const query: Query = {
      measures: ['test_table.measure.with.dots'],
      dimensions: ['test_table.dimension.with.dots'],
    };

    const result = cubeToDuckdbAST(query, complexTableSchema, defaultOptions);
    expect(result.node.type).toBe(QueryNodeType.SELECT_NODE);
    expect(result.node.group_expressions).toHaveLength(1);
    expect(result.node.group_expressions[0]).toEqual({
      alias: '',
      class: 'COLUMN_REF',
      column_names: ['test_table__dimension__with__dots'],
      type: 'COLUMN_REF',
    });
  });

  it('should handle aliases', () => {
    const tableSchema: TableSchema = {
      name: 'test_table',
      sql: 'test_table',
      measures: [
        {
          name: 'measure',
          sql: 'test_table.measure',
          type: 'number',
          alias: 'measure_with_alias',
        },
      ],
      dimensions: [
        {
          name: 'dimension',
          sql: 'test_table.dimension',
          type: 'string',
          alias: 'dimension_with_alias',
        },
      ],
    };

    const query: Query = {
      measures: ['test_table.measure'],
      dimensions: ['test_table.dimension'],
    };

    const result = cubeToDuckdbAST(query, tableSchema, defaultOptions);
    expect(result.node.type).toBe(QueryNodeType.SELECT_NODE);
    expect(result.node.group_expressions).toHaveLength(1);
    expect(result.node.group_expressions[0]).toEqual({
      alias: '',
      class: 'COLUMN_REF',
      column_names: ['dimension_with_alias'],
      type: 'COLUMN_REF',
    });
  });
});
