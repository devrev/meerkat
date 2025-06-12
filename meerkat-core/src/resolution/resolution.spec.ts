import {
  createBaseTableSchema,
  generateResolutionSchemas,
  generateResolvedDimensions,
} from './resolution';

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
            type: 'number',
          },
        ],
        dimensions: [
          {
            name: 'column1',
            sql: 'base_table.column1',
            type: 'string',
          },
          {
            name: 'column2',
            sql: 'base_table.column2',
            type: 'string',
          },
        ],
      },
    ];
    const resolutionConfig = {
      columnConfigs: [],
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
          name: 'base_table__column1',
          sql: '__base_query.base_table__column1',
          type: 'string',
        },
        {
          name: 'base_table__column2',
          sql: '__base_query.base_table__column2',
          type: 'string',
        },
        {
          name: 'base_table__count',
          sql: '__base_query.base_table__count',
          type: 'number',
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
            type: 'string',
          },
          {
            name: 'column2',
            sql: 'base_table.column2',
            type: 'string',
          },
        ],
      },
    ];
    const resolutionConfig = {
      columnConfigs: [
        {
          name: 'base_table.column1',
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
        {
          name: 'base_table.column2',
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_name'],
        },
      ],
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
        },
        {
          name: 'base_table__column2',
          sql: '__base_query.base_table__column2',
          type: 'string',
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
            type: 'string',
          },
        ],
      },
    ];
    const resolutionConfig = {
      columnConfigs: [],
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
    }).toThrow('Dimension not found: base_table.column2');
  });
});

describe('Generate resolution schemas', () => {
  it('multiple columns using same table', () => {
    const resolutionConfig = {
      columnConfigs: [
        {
          name: 'base_table.column1',
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
        {
          name: 'base_table.column2',
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
              type: 'string',
            },
            {
              name: 'display_id',
              sql: 'resolution_table.display_id',
              type: 'string',
            },
            {
              name: 'display_name',
              sql: 'resolution_table.display_name',
              type: 'string',
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
            name: 'display_id',
            sql: 'base_table__column1.display_id',
            type: 'string',
          },
        ],
      },
      {
        name: 'base_table__column2',
        sql: '<resolution_table_sql>',
        measures: [],
        dimensions: [
          {
            name: 'id',
            sql: 'base_table__column2.id',
            type: 'string',
          },
          {
            name: 'display_name',
            sql: 'base_table__column2.display_name',
            type: 'string',
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
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
        {
          name: 'base_table.column2',
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
              type: 'string',
            },
            {
              name: 'display_id',
              sql: 'resolution_table.display_id',
              type: 'string',
            },
            {
              name: 'display_name',
              sql: 'resolution_table.display_name',
              type: 'string',
            },
          ],
        },
      ],
    };

    expect(() => {
      generateResolutionSchemas(resolutionConfig);
    }).toThrow('Table schema not found for resolution_table1');
  });

  it('dimension does not exist', () => {
    const resolutionConfig = {
      columnConfigs: [
        {
          name: 'base_table.column1',
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
              type: 'string',
            },
          ],
          dimensions: [
            {
              name: 'id',
              sql: 'resolution_table.id',
              type: 'string',
            },
          ],
        },
      ],
    };

    expect(() => {
      generateResolutionSchemas(resolutionConfig);
    }).toThrow('Dimension not found: display_id');
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
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
        {
          name: 'base_table.column2',
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_name'],
        },
      ],
    };

    const resolvedDimensions = generateResolvedDimensions(
      query,
      resolutionConfig
    );

    expect(resolvedDimensions).toEqual([
      'base_table__column1.display_id',
      'base_table__column2.display_name',
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
          source: 'resolution_table',
          joinColumn: 'id',
          resolutionColumns: ['display_id'],
        },
      ],
    };

    const resolvedDimensions = generateResolvedDimensions(
      query,
      resolutionConfig
    );

    expect(resolvedDimensions).toEqual([
      '__base_query.base_table__count',
      '__base_query.base_table__column1',
    ]);
  });
});
