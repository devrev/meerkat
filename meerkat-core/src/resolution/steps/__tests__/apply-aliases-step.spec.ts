import { TableSchema } from '../../../types/cube-types';
import { ResolutionConfig } from '../../types';
import { applyAliases, ApplyAliasesParams } from '../apply-aliases-step';

const defaultConfig = { useDotNotation: false };

describe('apply-aliases-step', () => {
  describe('applyAliases', () => {
    const createMockTableSchema = (
      name: string,
      dimensions: { name: string; alias?: string; sql?: string }[] = [],
      measures: { name: string; alias?: string; sql?: string }[] = []
    ): TableSchema => ({
      name,
      sql: `SELECT * FROM ${name}`,
      dimensions: dimensions.map((d) => ({
        name: d.name,
        sql: d.sql || `${name}.${d.name}`,
        type: 'string',
        alias: d.alias,
      })),
      measures: measures.map((m) => ({
        name: m.name,
        sql: m.sql || `SUM(${name}.${m.name})`,
        type: 'number',
        alias: m.alias,
      })),
    });

    const mockCubeQueryToSQL = jest
      .fn()
      .mockResolvedValue('SELECT * FROM test');

    beforeEach(() => {
      mockCubeQueryToSQL.mockClear();
    });

    it('should apply aliases from original schema to aggregated schema', async () => {
      const aggregatedTableSchema = createMockTableSchema('aggregated', [
        { name: 'orders__customer_id', alias: 'orders__customer_id' },
      ]);
      const originalTableSchemas = [
        createMockTableSchema('orders', [
          { name: 'customer_id', alias: 'Customer ID' },
        ]),
      ];
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const params: ApplyAliasesParams = {
        aggregatedTableSchema,
        originalTableSchemas,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultConfig,
      };

      await applyAliases(params);

      expect(mockCubeQueryToSQL).toHaveBeenCalledTimes(1);
      const calledSchema = mockCubeQueryToSQL.mock.calls[0][0]
        .tableSchemas[0] as TableSchema;
      expect(calledSchema.dimensions[0].alias).toBe('Customer ID');
    });

    it('should use original alias for single resolution column', async () => {
      const aggregatedTableSchema = createMockTableSchema('aggregated', [
        {
          name: 'orders__customer_id__display_name',
          alias: 'orders__customer_id__display_name',
        },
      ]);
      const originalTableSchemas = [
        createMockTableSchema('orders', [
          { name: 'customer_id', alias: 'Customer ID' },
        ]),
      ];
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders__customer_id',
            type: 'string',
            source: 'customers',
            joinColumn: 'id',
            resolutionColumns: ['display_name'],
          },
        ],
        tableSchemas: [
          createMockTableSchema('customers', [
            { name: 'display_name', alias: 'Display Name' },
          ]),
        ],
      };

      const params: ApplyAliasesParams = {
        aggregatedTableSchema,
        originalTableSchemas,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultConfig,
      };

      await applyAliases(params);

      expect(mockCubeQueryToSQL).toHaveBeenCalledTimes(1);
      const calledSchema = mockCubeQueryToSQL.mock.calls[0][0]
        .tableSchemas[0] as TableSchema;
      expect(calledSchema.dimensions[0].alias).toBe('Customer ID');
    });

    it('should create compound alias for multiple resolution columns', async () => {
      const aggregatedTableSchema = createMockTableSchema('aggregated', [
        {
          name: 'orders__owner_id__first_name',
          alias: 'orders__owner_id__first_name',
        },
        {
          name: 'orders__owner_id__last_name',
          alias: 'orders__owner_id__last_name',
        },
      ]);
      const originalTableSchemas = [
        createMockTableSchema('orders', [{ name: 'owner_id', alias: 'Owner' }]),
      ];
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders__owner_id',
            type: 'string',
            source: 'users',
            joinColumn: 'id',
            resolutionColumns: ['first_name', 'last_name'],
          },
        ],
        tableSchemas: [
          createMockTableSchema('users', [
            { name: 'first_name', alias: 'First Name' },
            { name: 'last_name', alias: 'Last Name' },
          ]),
        ],
      };

      const params: ApplyAliasesParams = {
        aggregatedTableSchema,
        originalTableSchemas,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultConfig,
      };

      await applyAliases(params);

      expect(mockCubeQueryToSQL).toHaveBeenCalledTimes(1);
      const calledSchema = mockCubeQueryToSQL.mock.calls[0][0]
        .tableSchemas[0] as TableSchema;
      expect(calledSchema.dimensions[0].alias).toBe('Owner - First Name');
      expect(calledSchema.dimensions[1].alias).toBe('Owner - Last Name');
    });

    it('should handle nested dot notation with underscores', async () => {
      const aggregatedTableSchema = createMockTableSchema('aggregated', [
        {
          name: 'orders__customer__nested_id',
          alias: 'orders__customer__nested_id',
        },
      ]);
      const originalTableSchemas = [
        createMockTableSchema('orders', [
          { name: 'customer__nested_id', alias: 'Nested Customer' },
        ]),
      ];
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const params: ApplyAliasesParams = {
        aggregatedTableSchema,
        originalTableSchemas,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultConfig,
      };

      await applyAliases(params);

      expect(mockCubeQueryToSQL).toHaveBeenCalledTimes(1);
    });

    it('should handle measures from original schema', async () => {
      const aggregatedTableSchema = createMockTableSchema(
        'aggregated',
        [],
        [{ name: 'orders__total', alias: 'orders__total' }]
      );
      const originalTableSchemas = [
        createMockTableSchema(
          'orders',
          [],
          [{ name: 'total', alias: 'Total Revenue' }]
        ),
      ];
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const params: ApplyAliasesParams = {
        aggregatedTableSchema,
        originalTableSchemas,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultConfig,
      };

      await applyAliases(params);

      expect(mockCubeQueryToSQL).toHaveBeenCalledTimes(1);
      const calledSchema = mockCubeQueryToSQL.mock.calls[0][0]
        .tableSchemas[0] as TableSchema;
      // Measures are converted to dimensions in the final schema
      expect(calledSchema.dimensions[0].alias).toBe('Total Revenue');
    });

    describe('error handling', () => {
      it('should throw error when source table schema not found', async () => {
        const aggregatedTableSchema = createMockTableSchema('aggregated', [
          {
            name: 'orders__owner_id__first_name',
            alias: 'orders__owner_id__first_name',
          },
        ]);
        const originalTableSchemas = [
          createMockTableSchema('orders', [
            { name: 'owner_id', alias: 'Owner' },
          ]),
        ];
        const resolutionConfig: ResolutionConfig = {
          columnConfigs: [
            {
              name: 'orders__owner_id',
              type: 'string',
              source: 'nonexistent',
              joinColumn: 'id',
              resolutionColumns: ['first_name', 'last_name'],
            },
          ],
          tableSchemas: [], // Missing source table schema
        };

        const params: ApplyAliasesParams = {
          aggregatedTableSchema,
          originalTableSchemas,
          resolutionConfig,
          cubeQueryToSQL: mockCubeQueryToSQL,
          config: defaultConfig,
        };

        await expect(applyAliases(params)).rejects.toThrow(
          'Source table schema not found for nonexistent'
        );
      });

      it('should throw error when source field alias not found', async () => {
        const aggregatedTableSchema = createMockTableSchema('aggregated', [
          {
            name: 'orders__owner_id__first_name',
            alias: 'orders__owner_id__first_name',
          },
        ]);
        const originalTableSchemas = [
          createMockTableSchema('orders', [
            { name: 'owner_id', alias: 'Owner' },
          ]),
        ];
        const resolutionConfig: ResolutionConfig = {
          columnConfigs: [
            {
              name: 'orders__owner_id',
              type: 'string',
              source: 'users',
              joinColumn: 'id',
              resolutionColumns: ['first_name', 'last_name'],
            },
          ],
          tableSchemas: [
            createMockTableSchema('users', [
              // Missing alias for first_name
              { name: 'first_name' },
              { name: 'last_name', alias: 'Last Name' },
            ]),
          ],
        };

        const params: ApplyAliasesParams = {
          aggregatedTableSchema,
          originalTableSchemas,
          resolutionConfig,
          cubeQueryToSQL: mockCubeQueryToSQL,
          config: defaultConfig,
        };

        await expect(applyAliases(params)).rejects.toThrow(
          'Source field alias not found for first_name'
        );
      });
    });

    it('should skip members without aliases', async () => {
      const aggregatedTableSchema = createMockTableSchema('aggregated', [
        { name: 'orders__customer_id', alias: 'orders__customer_id' },
      ]);
      const originalTableSchemas = [
        createMockTableSchema('orders', [
          { name: 'customer_id' }, // No alias
        ]),
      ];
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const params: ApplyAliasesParams = {
        aggregatedTableSchema,
        originalTableSchemas,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultConfig,
      };

      await applyAliases(params);

      expect(mockCubeQueryToSQL).toHaveBeenCalledTimes(1);
      const calledSchema = mockCubeQueryToSQL.mock.calls[0][0]
        .tableSchemas[0] as TableSchema;
      // Should keep original alias from aggregated schema
      expect(calledSchema.dimensions[0].alias).toBe('orders__customer_id');
    });

    it('should pass context params to cubeQueryToSQL', async () => {
      const aggregatedTableSchema = createMockTableSchema('aggregated', [
        { name: 'orders__customer_id', alias: 'orders__customer_id' },
      ]);
      const originalTableSchemas = [
        createMockTableSchema('orders', [
          { name: 'customer_id', alias: 'Customer ID' },
        ]),
      ];
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };
      const contextParams = { userId: '123', orgId: 'abc' };

      const params: ApplyAliasesParams = {
        aggregatedTableSchema,
        originalTableSchemas,
        resolutionConfig,
        contextParams,
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultConfig,
      };

      await applyAliases(params);

      expect(mockCubeQueryToSQL).toHaveBeenCalledTimes(1);
      expect(mockCubeQueryToSQL.mock.calls[0][0].contextParams).toEqual({
        userId: '123',
        orgId: 'abc',
      });
    });

    it('should merge dimensions and measures into dimensions array', async () => {
      const aggregatedTableSchema = createMockTableSchema(
        'aggregated',
        [{ name: 'orders__status', alias: 'orders__status' }],
        [{ name: 'orders__total', alias: 'orders__total' }]
      );
      const originalTableSchemas = [
        createMockTableSchema(
          'orders',
          [{ name: 'status', alias: 'Status' }],
          [{ name: 'total', alias: 'Total' }]
        ),
      ];
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const params: ApplyAliasesParams = {
        aggregatedTableSchema,
        originalTableSchemas,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultConfig,
      };

      await applyAliases(params);

      const calledSchema = mockCubeQueryToSQL.mock.calls[0][0]
        .tableSchemas[0] as TableSchema;
      expect(calledSchema.dimensions).toHaveLength(2);
      expect(calledSchema.dimensions[0].alias).toBe('Status');
      expect(calledSchema.dimensions[1].alias).toBe('Total');
      expect(calledSchema.measures).toEqual([]);
    });

    it('should generate query with all dimensions namespaced', async () => {
      const aggregatedTableSchema = createMockTableSchema('aggregated', [
        { name: 'orders__customer_id', alias: 'orders__customer_id' },
        { name: 'orders__status', alias: 'orders__status' },
      ]);
      const originalTableSchemas = [
        createMockTableSchema('orders', [
          { name: 'customer_id', alias: 'Customer ID' },
          { name: 'status', alias: 'Status' },
        ]),
      ];
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const params: ApplyAliasesParams = {
        aggregatedTableSchema,
        originalTableSchemas,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultConfig,
      };

      await applyAliases(params);

      const calledQuery = mockCubeQueryToSQL.mock.calls[0][0].query;
      expect(calledQuery.measures).toEqual([]);
      expect(calledQuery.dimensions).toEqual([
        'aggregated.orders__customer_id',
        'aggregated.orders__status',
      ]);
    });

    it('should handle multiple original table schemas', async () => {
      const aggregatedTableSchema = createMockTableSchema('aggregated', [
        { name: 'orders__customer_id', alias: 'orders__customer_id' },
        { name: 'products__name', alias: 'products__name' },
      ]);
      const originalTableSchemas = [
        createMockTableSchema('orders', [
          { name: 'customer_id', alias: 'Customer ID' },
        ]),
        createMockTableSchema('products', [
          { name: 'name', alias: 'Product Name' },
        ]),
      ];
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const params: ApplyAliasesParams = {
        aggregatedTableSchema,
        originalTableSchemas,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultConfig,
      };

      await applyAliases(params);

      const calledSchema = mockCubeQueryToSQL.mock.calls[0][0]
        .tableSchemas[0] as TableSchema;
      expect(calledSchema.dimensions[0].alias).toBe('Customer ID');
      expect(calledSchema.dimensions[1].alias).toBe('Product Name');
    });

    it('should return SQL from cubeQueryToSQL', async () => {
      mockCubeQueryToSQL.mockResolvedValueOnce(
        'SELECT customer_id AS "Customer ID" FROM orders'
      );

      const aggregatedTableSchema = createMockTableSchema('aggregated', [
        { name: 'orders__customer_id', alias: 'orders__customer_id' },
      ]);
      const originalTableSchemas = [
        createMockTableSchema('orders', [
          { name: 'customer_id', alias: 'Customer ID' },
        ]),
      ];
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const params: ApplyAliasesParams = {
        aggregatedTableSchema,
        originalTableSchemas,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultConfig,
      };

      const result = await applyAliases(params);

      expect(result).toBe('SELECT customer_id AS "Customer ID" FROM orders');
    });

    it('should handle three resolution columns', async () => {
      const aggregatedTableSchema = createMockTableSchema('aggregated', [
        {
          name: 'orders__owner_id__first_name',
          alias: 'orders__owner_id__first_name',
        },
        {
          name: 'orders__owner_id__last_name',
          alias: 'orders__owner_id__last_name',
        },
        {
          name: 'orders__owner_id__email',
          alias: 'orders__owner_id__email',
        },
      ]);
      const originalTableSchemas = [
        createMockTableSchema('orders', [{ name: 'owner_id', alias: 'Owner' }]),
      ];
      const resolutionConfig: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders__owner_id',
            type: 'string',
            source: 'users',
            joinColumn: 'id',
            resolutionColumns: ['first_name', 'last_name', 'email'],
          },
        ],
        tableSchemas: [
          createMockTableSchema('users', [
            { name: 'first_name', alias: 'First Name' },
            { name: 'last_name', alias: 'Last Name' },
            { name: 'email', alias: 'Email Address' },
          ]),
        ],
      };

      const params: ApplyAliasesParams = {
        aggregatedTableSchema,
        originalTableSchemas,
        resolutionConfig,
        cubeQueryToSQL: mockCubeQueryToSQL,
        config: defaultConfig,
      };

      await applyAliases(params);

      const calledSchema = mockCubeQueryToSQL.mock.calls[0][0]
        .tableSchemas[0] as TableSchema;
      expect(calledSchema.dimensions[0].alias).toBe('Owner - First Name');
      expect(calledSchema.dimensions[1].alias).toBe('Owner - Last Name');
      expect(calledSchema.dimensions[2].alias).toBe('Owner - Email Address');
    });
  });
});
