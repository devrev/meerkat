import { Query, TableSchema } from '../types/cube-types';
import { getUsedTableSchema } from './get-used-table-schema';

describe('getUsedTableSchema', () => {
  const sampleTableSchema: TableSchema[] = [
    {
      name: 'table1',
      sql: 'SELECT * FROM table1',
      measures: [
        {
          name: 'measure1',
          sql: 'SUM(value1)',
          type: 'string',
        },
        {
          name: 'measure2',
          sql: 'COUNT(*)',
          type: 'string',
        },
      ],
      dimensions: [
        {
          name: 'dimension1',
          sql: 'dim1',
          type: 'string',
        },
        {
          name: 'dimension2',
          sql: 'dim2',
          type: 'string',
        },
      ],
      joins: [],
    },
    {
      name: 'table2',
      sql: 'SELECT * FROM table2',
      measures: [
        {
          name: 'measure3',
          sql: 'COUNT(*)',
          type: 'string',
        },
      ],
      dimensions: [
        {
          name: 'dimension3',
          sql: 'dim3',
          type: 'string',
        },
      ],
      joins: [],
    },
    {
      name: 'table3',
      sql: 'SELECT * FROM table3',
      measures: [
        {
          name: 'measure4',
          sql: 'SUM(value2)',
          type: 'string',
        },
      ],
      dimensions: [
        {
          name: 'dimension4',
          sql: 'dim4',
          type: 'string',
        },
      ],
      joins: [],
    },
  ];

  it('should return all tables when no filters are present', () => {
    const query: Query = {
      filters: [],
      measures: [],
    };
    const result = getUsedTableSchema(sampleTableSchema, query);
    expect(result).toEqual(sampleTableSchema);
  });

  it('should filter tables based on simple filter', () => {
    const query: Query = {
      filters: [
        {
          member: 'table1.measure1',
          operator: 'gt',
          values: ['100'],
        },
      ],
      measures: [],
    };
    const result = getUsedTableSchema(sampleTableSchema, query);
    expect(result).toHaveLength(1);
    expect(result[0].name).toBe('table1');
  });

  it('should filter tables based on AND conditions', () => {
    const query: Query = {
      filters: [
        {
          and: [
            {
              member: 'table1.measure1',
              operator: 'gt',
              values: ['100'],
            },
            {
              member: 'table2.dimension3',
              operator: 'equals',
              values: ['value'],
            },
          ],
        },
      ],
      measures: [],
    };
    const result = getUsedTableSchema(sampleTableSchema, query);
    expect(result).toHaveLength(2);
    expect(result.map((schema) => schema.name)).toContain('table1');
    expect(result.map((schema) => schema.name)).toContain('table2');
  });

  it('should filter tables based on OR conditions', () => {
    const query: Query = {
      filters: [
        {
          or: [
            {
              member: 'table1.measure1',
              operator: 'gt',
              values: ['100'],
            },
            {
              member: 'table3.dimension4',
              operator: 'equals',
              values: ['value'],
            },
          ],
        },
      ],
      measures: [],
    };
    const result = getUsedTableSchema(sampleTableSchema, query);
    expect(result).toHaveLength(2);
    expect(result.map((schema) => schema.name)).toContain('table1');
    expect(result.map((schema) => schema.name)).toContain('table3');
  });

  it('should filter tables based on measures', () => {
    const query: Query = {
      filters: [],
      measures: ['table1.measure1', 'table2.measure3'],
    };
    const result = getUsedTableSchema(sampleTableSchema, query);
    expect(result).toHaveLength(2);
    expect(result.map((schema) => schema.name)).toContain('table1');
    expect(result.map((schema) => schema.name)).toContain('table2');
  });

  it('should filter tables based on dimensions', () => {
    const query: Query = {
      filters: [],
      dimensions: ['table3.dimension4', 'table2.dimension3'],
      measures: [],
    };
    const result = getUsedTableSchema(sampleTableSchema, query);
    expect(result).toHaveLength(2);
    expect(result.map((schema) => schema.name)).toContain('table3');
    expect(result.map((schema) => schema.name)).toContain('table2');
  });

  it('should filter tables based on order', () => {
    const query: Query = {
      filters: [],
      order: {
        'table1.measure1': 'desc',
      },
      measures: [],
    };
    const result = getUsedTableSchema(sampleTableSchema, query);
    expect(result).toHaveLength(1);
    expect(result[0].name).toBe('table1');
  });

  it('should filter tables based on join paths', () => {
    const query: Query = {
      filters: [],
      joinPaths: [
        [
          {
            left: 'table1',
            right: 'table2',
            on: 'id',
          },
        ],
      ],
      measures: [],
    };
    const result = getUsedTableSchema(sampleTableSchema, query);
    expect(result).toHaveLength(2);
    expect(result.map((schema) => schema.name)).toContain('table1');
    expect(result.map((schema) => schema.name)).toContain('table2');
  });

  it('should handle complex query with multiple conditions', () => {
    const query: Query = {
      filters: [
        {
          and: [
            {
              member: 'table1.measure1',
              operator: 'gt',
              values: ['100'],
            },
            {
              or: [
                {
                  member: 'table2.dimension3',
                  operator: 'equals',
                  values: ['value1'],
                },
                {
                  member: 'table3.dimension4',
                  operator: 'equals',
                  values: ['value2'],
                },
              ],
            },
          ],
        },
      ],
      measures: ['table1.measure2'],
      dimensions: ['table2.dimension3'],
      order: {
        'table3.measure4': 'asc',
      },
      joinPaths: [
        [
          {
            left: 'table1',
            right: 'table2',
            on: 'id',
          },
        ],
      ],
    };
    const result = getUsedTableSchema(sampleTableSchema, query);
    expect(result).toHaveLength(3);
    expect(result.map((schema) => schema.name)).toContain('table1');
    expect(result.map((schema) => schema.name)).toContain('table2');
    expect(result.map((schema) => schema.name)).toContain('table3');
  });
});
