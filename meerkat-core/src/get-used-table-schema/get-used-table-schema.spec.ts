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
      ],
      dimensions: [
        {
          name: 'dimension1',
          sql: 'dim1',
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
          name: 'measure2',
          sql: 'COUNT(*)',
          type: 'string',
        },
      ],
      dimensions: [
        {
          name: 'dimension2',
          sql: 'dim2',
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
          name: 'measure3',
          sql: 'AVG(value3)',
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
  ];

  it('should filter tables based on simple filter', () => {
    const query: Query = {
      filters: [
        {
          member: 'table1.measure1',
          operator: 'equals',
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
              operator: 'equals',
              values: ['100'],
            },
            {
              member: 'table2.dimension2',
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
    expect(result.map((schema) => schema.name).sort()).toEqual([
      'table1',
      'table2',
    ]);
  });

  it('should filter tables based on OR conditions', () => {
    const query: Query = {
      filters: [
        {
          or: [
            {
              member: 'table1.measure1',
              operator: 'equals',
              values: ['100'],
            },
            {
              member: 'table3.dimension3',
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
    expect(result.map((schema) => schema.name).sort()).toEqual([
      'table1',
      'table3',
    ]);
  });

  it('should handle nested AND-OR conditions', () => {
    const query: Query = {
      filters: [
        {
          and: [
            {
              member: 'table1.measure1',
              operator: 'equals',
              values: ['100'],
            },
            {
              or: [
                {
                  member: 'table2.dimension2',
                  operator: 'equals',
                  values: ['value1'],
                },
                {
                  member: 'table3.dimension3',
                  operator: 'equals',
                  values: ['value2'],
                },
              ],
            },
          ],
        },
      ],
      measures: [],
    };
    const result = getUsedTableSchema(sampleTableSchema, query);
    expect(result).toHaveLength(3);
    expect(result.map((schema) => schema.name).sort()).toEqual([
      'table1',
      'table2',
      'table3',
    ]);
  });

  it('should handle multiple top-level filters', () => {
    const query: Query = {
      filters: [
        {
          member: 'table1.measure1',
          operator: 'equals',
          values: ['100'],
        },
        {
          member: 'table2.measure2',
          operator: 'equals',
          values: ['200'],
        },
      ],
      measures: [],
    };
    const result = getUsedTableSchema(sampleTableSchema, query);
    expect(result).toHaveLength(2);
    expect(result.map((schema) => schema.name).sort()).toEqual([
      'table1',
      'table2',
    ]);
  });

  it('should handle complex nested filters', () => {
    const query: Query = {
      filters: [
        {
          and: [
            {
              or: [
                {
                  member: 'table1.measure1',
                  operator: 'equals',
                  values: ['100'],
                },
                {
                  member: 'table2.measure2',
                  operator: 'equals',
                  values: ['200'],
                },
              ],
            },
            {
              member: 'table3.measure3',
              operator: 'equals',
              values: ['300'],
            },
          ],
        },
      ],
      measures: [],
    };
    const result = getUsedTableSchema(sampleTableSchema, query);
    expect(result).toHaveLength(3);
    expect(result.map((schema) => schema.name).sort()).toEqual([
      'table1',
      'table2',
      'table3',
    ]);
  });
});
