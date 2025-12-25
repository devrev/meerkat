import { isArrayTypeMember } from '../utils/is-array-member-type';
import {
  generateResolutionJoinPaths,
  generateResolutionSchemas,
  generateResolvedDimensions,
  generateRowNumberSql,
} from './generators';
import {
  createBaseTableSchema,
  createWrapperTableSchema,
  getArrayTypeResolutionColumnConfigs,
  withArrayFlattenModifier,
} from './resolution';
import { BASE_DATA_SOURCE_NAME, ResolutionConfig } from './types';

describe('Create base table schema', () => {
  it('dimensions and measures are converted to dimensions', () => {
    const sql =
      'SELECT COUNT(*), column1, column2 FROM base_table GROUP BY column1, column2';
    const tableSchemas = [
      {
        name: 'base_table',
        sql: '<base_table_sql>',
        measures: [
          {
            name: 'count',
            sql: 'COUNT(*)',
            type: 'number' as const,
          },
        ],
        dimensions: [
          {
            name: 'column1',
            sql: 'base_table.column1',
            type: 'string' as const,
          },
          {
            name: 'column2',
            sql: 'base_table.column2',
            type: 'string' as const,
          },
        ],
      },
    ];
    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
    };
    const measures = ['base_table.count'];
    const dimensions = ['base_table.column1', 'base_table.column2'];

    const baseTableSchema = createBaseTableSchema(
      sql,
      tableSchemas,
      resolutionConfig,
      measures,
      dimensions
    );

    expect(baseTableSchema).toEqual({
      name: '__base_query',
      sql: 'SELECT COUNT(*), column1, column2 FROM base_table GROUP BY column1, column2',
      measures: [],
      dimensions: [
        {
          name: 'base_table__count',
          sql: '__base_query.base_table__count',
          type: 'number',
          alias: 'base_table__count',
        },
        {
          name: 'base_table__column1',
          sql: '__base_query.base_table__column1',
          type: 'string',
          alias: 'base_table__column1',
        },
        {
          name: 'base_table__column2',
          sql: '__base_query.base_table__column2',
          type: 'string',
          alias: 'base_table__column2',
        },
      ],
      joins: [],
    });
  });

  it('create join config', () => {
    const sql = 'SELECT * FROM base_table';
    const tableSchemas = [
      {
        name: 'base_table',
        sql: '<base_table_sql>',
        measures: [],
        dimensions: [
          {
            name: 'column1',
            sql: 'base_table.column1',
            type: 'string' as const,
          },
          {
            name: 'column2',
            sql: 'base_table.column2',
            type: 'string' as const,
          },
        ],
      },
    ];
    const resolutionConfig = {
      columnConfigs: [
        {
          name: 'base_table.column1',
          source: 'resolution_table',
          type: 'string' as const,
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
        {
          name: 'base_table.column2',
          source: 'resolution_table',
          type: 'string' as const,
          joinColumn: 'id',
          resolutionColumns: ['display_name'],
        },
      ],
      tableSchemas: [],
    };
    const dimensions = ['base_table.column1', 'base_table.column2'];

    const baseTableSchema = createBaseTableSchema(
      sql,
      tableSchemas,
      resolutionConfig,
      [],
      dimensions
    );

    expect(baseTableSchema).toEqual({
      name: '__base_query',
      sql: 'SELECT * FROM base_table',
      measures: [],
      dimensions: [
        {
          name: 'base_table__column1',
          sql: '__base_query.base_table__column1',
          type: 'string',
          alias: 'base_table__column1',
        },
        {
          name: 'base_table__column2',
          sql: '__base_query.base_table__column2',
          type: 'string',
          alias: 'base_table__column2',
        },
      ],
      joins: [
        {
          sql: '__base_query.base_table__column1 = base_table__column1.id',
        },
        {
          sql: '__base_query.base_table__column2 = base_table__column2.id',
        },
      ],
    });
  });

  it('dimension not found', () => {
    const sql = 'SELECT * FROM base_table';
    const tableSchemas = [
      {
        name: 'base_table',
        sql: '<base_table_sql>',
        measures: [],
        dimensions: [
          {
            name: 'column1',
            sql: 'base_table.column1',
            type: 'string' as const,
          },
        ],
      },
    ];
    const resolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
    };
    const dimensions = ['base_table.column1', 'base_table.column2']; // column2 does not exist

    expect(() => {
      createBaseTableSchema(
        sql,
        tableSchemas,
        resolutionConfig,
        [],
        dimensions
      );
    }).toThrow('Not found: base_table.column2');
  });

  it('handle aliases', () => {
    const sql = 'SELECT * FROM base_table';
    const tableSchemas = [
      {
        name: 'base_table',
        sql: '<base_table_sql>',
        measures: [],
        dimensions: [
          {
            name: 'column1',
            sql: 'base_table.column1',
            type: 'string' as const,
            alias: 'Column 1',
          },
          {
            name: 'column2',
            sql: 'base_table.column2',
            type: 'string' as const,
            alias: 'Column 2',
          },
        ],
      },
    ];
    const resolutionConfig = {
      columnConfigs: [
        {
          name: 'base_table.column1',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
        {
          name: 'base_table.column2',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_name'],
        },
      ],
      tableSchemas: [],
    };
    const dimensions = ['base_table.column1', 'base_table.column2'];

    const baseTableSchema = createBaseTableSchema(
      sql,
      tableSchemas,
      resolutionConfig,
      [],
      dimensions
    );

    expect(baseTableSchema).toEqual({
      name: '__base_query',
      sql: 'SELECT * FROM base_table',
      measures: [],
      dimensions: [
        {
          name: 'base_table__column1',
          sql: '__base_query."Column 1"',
          type: 'string',
          alias: 'Column 1',
        },
        {
          name: 'base_table__column2',
          sql: '__base_query."Column 2"',
          type: 'string',
          alias: 'Column 2',
        },
      ],
      joins: [
        {
          sql: '__base_query."Column 1" = base_table__column1.id',
        },
        {
          sql: '__base_query."Column 2" = base_table__column2.id',
        },
      ],
    });
  });
});

describe('Generate resolution schemas', () => {
  it('multiple columns using same table', () => {
    const baseTableSchemas = [
      {
        name: 'base_table',
        sql: '<base_table_sql>',
        measures: [],
        dimensions: [
          {
            name: 'column1',
            sql: 'base_table.column1',
            type: 'string' as const,
          },
          {
            name: 'column2',
            sql: 'base_table.column2',
            type: 'string' as const,
          },
        ],
      },
    ];

    const resolutionConfig = {
      columnConfigs: [
        {
          name: 'base_table.column1',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
        {
          name: 'base_table.column2',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['id', 'display_name'],
        },
      ],
      tableSchemas: [
        {
          name: 'resolution_table',
          sql: '<resolution_table_sql>',
          measures: [],
          dimensions: [
            {
              name: 'id',
              sql: 'resolution_table.id',
              type: 'string' as const,
            },
            {
              name: 'display_id',
              sql: 'resolution_table.display_id',
              type: 'string' as const,
            },
            {
              name: 'display_name',
              sql: 'resolution_table.display_name',
              type: 'string' as const,
            },
          ],
        },
      ],
    };

    const schemas = generateResolutionSchemas(resolutionConfig);

    expect(schemas).toEqual([
      {
        name: 'base_table__column1',
        sql: '<resolution_table_sql>',
        measures: [],
        dimensions: [
          {
            name: 'base_table__column1__display_id',
            sql: 'base_table__column1.display_id',
            type: 'string',
            alias: 'base_table__column1__display_id',
          },
        ],
      },
      {
        name: 'base_table__column2',
        sql: '<resolution_table_sql>',
        measures: [],
        dimensions: [
          {
            name: 'base_table__column2__id',
            sql: 'base_table__column2.id',
            type: 'string',
            alias: 'base_table__column2__id',
          },
          {
            name: 'base_table__column2__display_name',
            sql: 'base_table__column2.display_name',
            type: 'string',
            alias: 'base_table__column2__display_name',
          },
        ],
      },
    ]);
  });

  it('table does not exist', () => {
    const resolutionConfig = {
      columnConfigs: [
        {
          name: 'base_table.column1',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
        {
          name: 'base_table.column2',
          type: 'string' as const,
          source: 'resolution_table1', // does not exist
          joinColumn: 'id',
          resolutionColumns: ['id', 'display_name'],
        },
      ],
      tableSchemas: [
        {
          name: 'resolution_table',
          sql: '<resolution_table_sql>',
          measures: [],
          dimensions: [
            {
              name: 'id',
              sql: 'resolution_table.id',
              type: 'string' as const,
            },
            {
              name: 'display_id',
              sql: 'resolution_table.display_id',
              type: 'string' as const,
            },
            {
              name: 'display_name',
              sql: 'resolution_table.display_name',
              type: 'string' as const,
            },
          ],
        },
      ],
    };

    expect(() => {
      generateResolutionSchemas(resolutionConfig);
    }).toThrow('Table schema not found for resolution_table1');
  });

  it('resolution column does not exist', () => {
    const resolutionConfig = {
      columnConfigs: [
        {
          name: 'base_table.column1',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
      ],
      tableSchemas: [
        {
          name: 'resolution_table',
          sql: '<resolution_table_sql>',
          measures: [
            {
              name: 'display_id',
              sql: 'resolution_table.display_id',
              type: 'string' as const,
            },
          ],
          dimensions: [
            {
              name: 'id',
              sql: 'resolution_table.id',
              type: 'string' as const,
            },
          ],
        },
      ],
    };

    expect(() => {
      generateResolutionSchemas(resolutionConfig);
    }).toThrow('Dimension not found: display_id');
  });

  it('column does not exist in base query', () => {
    const resolutionConfig = {
      columnConfigs: [
        {
          name: 'base_table.column1',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
      ],
      tableSchemas: [
        {
          name: 'resolution_table',
          sql: '<resolution_table_sql>',
          measures: [],
          dimensions: [
            {
              name: 'id',
              sql: 'resolution_table.id',
              type: 'string' as const,
            },
            {
              name: 'display_id',
              sql: 'resolution_table.display_id',
              type: 'string' as const,
            },
          ],
        },
      ],
    };

    const schemas = generateResolutionSchemas(resolutionConfig);
    expect(schemas).toEqual([
      {
        name: 'base_table__column1',
        sql: '<resolution_table_sql>',
        measures: [],
        dimensions: [
          {
            name: 'base_table__column1__display_id',
            sql: 'base_table__column1.display_id',
            type: 'string',
            alias: 'base_table__column1__display_id',
          },
        ],
      },
    ]);
  });

  it('handle aliases', () => {
    const baseTableSchemas = [
      {
        name: 'base_table',
        sql: '<base_table_sql>',
        measures: [],
        dimensions: [
          {
            name: 'column1',
            sql: 'base_table.column1',
            type: 'string' as const,
            alias: 'Column 1',
          },
        ],
      },
    ];

    const resolutionConfig = {
      columnConfigs: [
        {
          name: 'base_table.column1',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
      ],
      tableSchemas: [
        {
          name: 'resolution_table',
          sql: '<resolution_table_sql>',
          measures: [],
          dimensions: [
            {
              name: 'id',
              sql: 'resolution_table.id',
              type: 'string' as const,
              alias: 'ID',
            },
            {
              name: 'display_id',
              sql: 'resolution_table.display_id',
              type: 'string' as const,
              alias: 'Display ID',
            },
          ],
        },
      ],
    };

    const schemas = generateResolutionSchemas(resolutionConfig);
    expect(schemas).toEqual([
      {
        name: 'base_table__column1',
        sql: '<resolution_table_sql>',
        measures: [],
        dimensions: [
          {
            name: 'base_table__column1__display_id',
            sql: 'base_table__column1.display_id',
            type: 'string',
            alias: 'base_table__column1__display_id',
          },
        ],
      },
    ]);
  });
});

describe('Generate resolved dimensions', () => {
  it('resolves dimensions based on resolution config', () => {
    const query = {
      measures: [],
      dimensions: ['base_table.column1', 'base_table.column2'],
    };
    const resolutionConfig = {
      columnConfigs: [
        {
          name: 'base_table.column1',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
        {
          name: 'base_table.column2',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_name'],
        },
      ],
      tableSchemas: [],
    };

    const resolvedDimensions = generateResolvedDimensions(
      BASE_DATA_SOURCE_NAME,
      query,
      resolutionConfig
    );

    expect(resolvedDimensions).toEqual([
      'base_table__column1.base_table__column1__display_id',
      'base_table__column2.base_table__column2__display_name',
    ]);
  });

  it('unresolved columns', () => {
    const query = {
      measures: ['base_table.count'],
      dimensions: ['base_table.column1'],
    };
    const resolutionConfig = {
      columnConfigs: [
        {
          name: 'base_table.column3',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
      ],
      tableSchemas: [],
    };

    const resolvedDimensions = generateResolvedDimensions(
      BASE_DATA_SOURCE_NAME,
      query,
      resolutionConfig
    );

    expect(resolvedDimensions).toEqual([
      '__base_query.base_table__count',
      '__base_query.base_table__column1',
    ]);
  });

  it('only include projected columns', () => {
    const query = {
      measures: ['base_table.count', 'base_table.total'],
      dimensions: ['base_table.column1', 'base_table.column2'],
    };
    const resolutionConfig = {
      columnConfigs: [
        {
          name: 'base_table.column1',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
        {
          name: 'base_table.column2',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['id', 'display_name'],
        },
      ],
      tableSchemas: [],
    };
    const projections = [
      'base_table.count',
      'base_table.column2',
      'base_table.total',
    ];

    const resolvedDimensions = generateResolvedDimensions(
      BASE_DATA_SOURCE_NAME,
      query,
      resolutionConfig,
      projections
    );

    expect(resolvedDimensions).toEqual([
      '__base_query.base_table__count',
      'base_table__column2.base_table__column2__id',
      'base_table__column2.base_table__column2__display_name',
      '__base_query.base_table__total',
    ]);
  });
});

describe('Generate resolution join paths', () => {
  it('generate join paths for resolution columns', () => {
    const resolutionConfig = {
      columnConfigs: [
        {
          name: 'base_table.column1',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
        {
          name: 'base_table.column2',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_name'],
        },
      ],
      tableSchemas: [],
    };

    const joinPaths = generateResolutionJoinPaths(
      BASE_DATA_SOURCE_NAME,
      resolutionConfig
    );

    expect(joinPaths).toEqual([
      [
        {
          left: '__base_query',
          right: 'base_table__column1',
          on: 'base_table__column1',
        },
      ],
      [
        {
          left: '__base_query',
          right: 'base_table__column2',
          on: 'base_table__column2',
        },
      ],
    ]);
  });

  it('join paths with aliases', () => {
    const baseTableSchemas = [
      {
        name: 'base_table',
        sql: '<base_table_sql>',
        measures: [],
        dimensions: [
          {
            name: 'column1',
            sql: 'base_table.column1',
            type: 'string' as const,
            alias: 'base_table__column1',
          },
          {
            name: 'column2',
            sql: 'base_table.column2',
            type: 'string' as const,
            alias: 'base_table__column2',
          },
        ],
      },
    ];

    const resolutionConfig = {
      columnConfigs: [
        {
          name: 'base_table.column1',
          type: 'string' as const,
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
      ],
      tableSchemas: [],
    };

    const joinPaths = generateResolutionJoinPaths(
      BASE_DATA_SOURCE_NAME,
      resolutionConfig
    );
    expect(joinPaths).toEqual([
      [
        {
          left: '__base_query',
          right: 'base_table__column1',
          on: 'base_table__column1',
        },
      ],
    ]);
  });
});

describe('createWrapperTableSchema', () => {
  it('should create wrapper schema with correct structure', () => {
    const sql = 'SELECT * FROM base_table';
    const baseTableSchema = {
      name: 'original_table',
      sql: 'original sql',
      dimensions: [
        {
          name: 'column1',
          sql: 'original_table.column1',
          type: 'string' as const,
          alias: 'Column 1',
        },
        {
          name: 'column2',
          sql: 'original_table.column2',
          type: 'number' as const,
          alias: 'Column 2',
        },
      ],
      measures: [
        {
          name: 'count',
          sql: 'COUNT(*)',
          type: 'number' as const,
          alias: 'Count',
        },
      ],
      joins: [
        {
          sql: 'some_join_condition',
        },
      ],
    } as any;

    const result = createWrapperTableSchema(sql, baseTableSchema);

    expect(result).toEqual({
      name: '__base_query',
      sql: 'SELECT * FROM base_table',
      dimensions: [
        {
          name: 'column1',
          sql: '__base_query."Column 1"',
          type: 'string',
          alias: 'Column 1',
        },
        {
          name: 'column2',
          sql: '__base_query."Column 2"',
          type: 'number',
          alias: 'Column 2',
        },
      ],
      measures: [
        {
          name: 'count',
          sql: '__base_query."Count"',
          type: 'number',
          alias: 'Count',
        },
      ],
      joins: [
        {
          sql: 'some_join_condition',
        },
      ],
    });
  });

  it('should handle dimensions without aliases', () => {
    const sql = 'SELECT column1 FROM base_table';
    const baseTableSchema = {
      name: 'original_table',
      sql: 'original sql',
      dimensions: [
        {
          name: 'column1',
          sql: 'original_table.column1',
          type: 'string' as const,
        },
      ],
      measures: [],
      joins: [],
    } as any;

    const result = createWrapperTableSchema(sql, baseTableSchema);

    expect(result.dimensions[0].sql).toBe('__base_query."column1"');
  });

  it('should handle empty dimensions and measures', () => {
    const sql = 'SELECT * FROM base_table';
    const baseTableSchema = {
      name: 'original_table',
      sql: 'original sql',
      dimensions: [],
      measures: [],
      joins: [],
    } as any;

    const result = createWrapperTableSchema(sql, baseTableSchema);

    expect(result).toEqual({
      name: '__base_query',
      sql: 'SELECT * FROM base_table',
      dimensions: [],
      measures: [],
      joins: [],
    });
  });
});

describe('getArrayTypeResolutionColumnConfigs', () => {
  it('should filter and return only array type column configs', () => {
    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [
        {
          name: 'table.array_column',
          type: 'string_array' as const,
          source: 'lookup_table',
          joinColumn: 'id',
          resolutionColumns: ['name'],
        },
        {
          name: 'table.scalar_column',
          type: 'string' as const,
          source: 'lookup_table',
          joinColumn: 'id',
          resolutionColumns: ['name'],
        },
        {
          name: 'table.another_array',
          type: 'number_array' as const,
          source: 'lookup_table2',
          joinColumn: 'id',
          resolutionColumns: ['value'],
        },
      ],
      tableSchemas: [],
    };

    const result = getArrayTypeResolutionColumnConfigs(resolutionConfig);

    expect(result).toHaveLength(2);
    expect(result[0].name).toBe('table.array_column');
    expect(result[1].name).toBe('table.another_array');
    expect(result.every((config) => isArrayTypeMember(config.type))).toBe(true);
  });

  it('should return empty array when no array type configs exist', () => {
    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [
        {
          name: 'table.scalar_column1',
          type: 'string' as const,
          source: 'lookup_table',
          joinColumn: 'id',
          resolutionColumns: ['name'],
        },
        {
          name: 'table.scalar_column2',
          type: 'number' as const,
          source: 'lookup_table',
          joinColumn: 'id',
          resolutionColumns: ['name'],
        },
      ],
      tableSchemas: [],
    };

    const result = getArrayTypeResolutionColumnConfigs(resolutionConfig);

    expect(result).toEqual([]);
  });

  it('should return empty array when columnConfigs is empty', () => {
    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [],
      tableSchemas: [],
    };

    const result = getArrayTypeResolutionColumnConfigs(resolutionConfig);

    expect(result).toEqual([]);
  });
});

describe('withArrayFlattenModifier', () => {
  it('should add shouldFlattenArray modifier to array columns', () => {
    const baseTableSchema = {
      name: 'base_table',
      sql: 'SELECT * FROM base_table',
      dimensions: [
        {
          name: 'array_column',
          sql: 'base_table.array_column',
          type: 'string_array' as const,
        },
        {
          name: 'scalar_column',
          sql: 'base_table.scalar_column',
          type: 'string' as const,
        },
      ],
      measures: [],
    } as any;

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [
        {
          name: 'array_column',
          type: 'string_array' as const,
          source: 'lookup_table',
          joinColumn: 'id',
          resolutionColumns: ['name'],
        },
      ],
      tableSchemas: [],
    };

    const result = withArrayFlattenModifier(baseTableSchema, resolutionConfig);

    expect(result.dimensions[0].modifier).toEqual({
      shouldFlattenArray: true,
    });
    expect(result.dimensions[1].modifier).toBeUndefined();
    // Verify immutability
    expect(baseTableSchema.dimensions[0].modifier).toBeUndefined();
  });

  it('should handle multiple array columns', () => {
    const baseTableSchema = {
      name: 'base_table',
      sql: 'SELECT * FROM base_table',
      dimensions: [
        {
          name: 'array_column1',
          sql: 'base_table.array_column1',
          type: 'string_array' as const,
        },
        {
          name: 'array_column2',
          sql: 'base_table.array_column2',
          type: 'string_array' as const,
        },
        {
          name: 'scalar_column',
          sql: 'base_table.scalar_column',
          type: 'string' as const,
        },
      ],
      measures: [],
    } as any;

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [
        {
          name: 'array_column1',
          type: 'string_array' as const,
          source: 'lookup_table1',
          joinColumn: 'id',
          resolutionColumns: ['name'],
        },
        {
          name: 'array_column2',
          type: 'number_array' as const,
          source: 'lookup_table2',
          joinColumn: 'id',
          resolutionColumns: ['value'],
        },
      ],
      tableSchemas: [],
    };

    const result = withArrayFlattenModifier(baseTableSchema, resolutionConfig);

    expect(result.dimensions[0].modifier).toEqual({
      shouldFlattenArray: true,
    });
    expect(result.dimensions[1].modifier).toEqual({
      shouldFlattenArray: true,
    });
    expect(result.dimensions[2].modifier).toBeUndefined();
    // Verify immutability
    expect(baseTableSchema.dimensions[0].modifier).toBeUndefined();
  });

  it('should not modify dimensions when no array columns in config', () => {
    const baseTableSchema = {
      name: 'base_table',
      sql: 'SELECT * FROM base_table',
      dimensions: [
        {
          name: 'column1',
          sql: 'base_table.column1',
          type: 'string',
        },
        {
          name: 'column2',
          sql: 'base_table.column2',
          type: 'number',
        },
      ],
      measures: [],
    } as any;

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [
        {
          name: 'column1',
          type: 'string' as const,
          source: 'lookup_table',
          joinColumn: 'id',
          resolutionColumns: ['name'],
        },
      ],
      tableSchemas: [],
    };

    const result = withArrayFlattenModifier(baseTableSchema, resolutionConfig);

    expect(result.dimensions[0].modifier).toBeUndefined();
    expect(result.dimensions[1].modifier).toBeUndefined();
  });

  it('should handle empty dimensions array', () => {
    const baseTableSchema = {
      name: 'base_table',
      sql: 'SELECT * FROM base_table',
      dimensions: [],
      measures: [],
    } as any;

    const resolutionConfig: ResolutionConfig = {
      columnConfigs: [
        {
          name: 'array_column',
          type: 'string_array' as const,
          source: 'lookup_table',
          joinColumn: 'id',
          resolutionColumns: ['name'],
        },
      ],
      tableSchemas: [],
    };

    // Should not throw error
    expect(() => {
      withArrayFlattenModifier(baseTableSchema, resolutionConfig);
    }).not.toThrow();

    const result = withArrayFlattenModifier(baseTableSchema, resolutionConfig);
    expect(result.dimensions).toEqual([]);
  });
});

describe('generateRowNumberSql', () => {
  it('should generate row_number with ORDER BY for single column', () => {
    const query = {
      order: { 'table.id': 'asc' },
    };
    const dimensions = [
      {
        name: 'table__id',
        alias: 'ID',
      },
      {
        name: 'table__name',
        alias: 'Name',
      },
    ];

    const result = generateRowNumberSql(
      query,
      dimensions,
      BASE_DATA_SOURCE_NAME
    );

    expect(result).toBe('row_number() OVER (ORDER BY __base_query."ID" ASC)');
  });

  it('should generate row_number with ORDER BY for multiple columns', () => {
    const query = {
      order: { 'table.id': 'asc', 'table.name': 'desc' },
    };
    const dimensions = [
      {
        name: 'table__id',
        alias: 'ID',
      },
      {
        name: 'table__name',
        alias: 'Name',
      },
    ];

    const result = generateRowNumberSql(
      query,
      dimensions,
      BASE_DATA_SOURCE_NAME
    );

    expect(result).toBe(
      'row_number() OVER (ORDER BY __base_query."ID" ASC, __base_query."Name" DESC)'
    );
  });

  it('should generate row_number without ORDER BY when query has no order', () => {
    const query = {};
    const dimensions = [
      {
        name: 'table__id',
        alias: 'ID',
      },
    ];

    const result = generateRowNumberSql(
      query,
      dimensions,
      BASE_DATA_SOURCE_NAME
    );

    expect(result).toBe('row_number() OVER ()');
  });

  it('should generate row_number without ORDER BY when order is empty', () => {
    const query = {
      order: {},
    };
    const dimensions = [
      {
        name: 'table__id',
        alias: 'ID',
      },
    ];

    const result = generateRowNumberSql(
      query,
      dimensions,
      BASE_DATA_SOURCE_NAME
    );

    expect(result).toBe('row_number() OVER ()');
  });

  it('should use dimension name when alias is not present', () => {
    const query = {
      order: { 'table.id': 'asc' },
    };
    const dimensions = [
      {
        name: 'table__id',
      },
    ];

    const result = generateRowNumberSql(
      query,
      dimensions,
      BASE_DATA_SOURCE_NAME
    );

    expect(result).toBe(
      'row_number() OVER (ORDER BY __base_query."table__id" ASC)'
    );
  });

  it('should handle dimension not found by using safe member name', () => {
    const query = {
      order: { 'table.unknown_column': 'desc' },
    };
    const dimensions = [
      {
        name: 'table__id',
        alias: 'ID',
      },
    ];

    const result = generateRowNumberSql(
      query,
      dimensions,
      BASE_DATA_SOURCE_NAME
    );

    expect(result).toBe(
      'row_number() OVER (ORDER BY __base_query."table__unknown_column" DESC)'
    );
  });

  it('should handle mixed case order directions', () => {
    const query = {
      order: { 'table.id': 'asc', 'table.name': 'desc' },
    };
    const dimensions = [
      {
        name: 'table__id',
        alias: 'ID',
      },
      {
        name: 'table__name',
        alias: 'Name',
      },
    ];

    const result = generateRowNumberSql(
      query,
      dimensions,
      BASE_DATA_SOURCE_NAME
    );

    expect(result).toBe(
      'row_number() OVER (ORDER BY __base_query."ID" ASC, __base_query."Name" DESC)'
    );
  });

  it('should use custom base table name', () => {
    const query = {
      order: { 'table.id': 'asc' },
    };
    const dimensions = [
      {
        name: 'table__id',
        alias: 'ID',
      },
    ];
    const customBaseTableName = 'custom_table';

    const result = generateRowNumberSql(query, dimensions, customBaseTableName);

    expect(result).toBe('row_number() OVER (ORDER BY custom_table."ID" ASC)');
  });
});
