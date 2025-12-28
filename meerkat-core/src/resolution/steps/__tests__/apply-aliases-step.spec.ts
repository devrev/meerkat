import { TableSchema } from '../../../types/cube-types';
import { ResolutionConfig } from '../../types';
import { applyAliases, ApplyAliasesParams } from '../apply-aliases-step';

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

    const mockCubeQueryToSQL = jest.fn().mockResolvedValue('SELECT * FROM test');

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
      };

      await applyAliases(params);

      expect(mockCubeQueryToSQL).toHaveBeenCalled();
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
      };

      await applyAliases(params);

      expect(mockCubeQueryToSQL).toHaveBeenCalled();
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
      };

      await applyAliases(params);

      expect(mockCubeQueryToSQL).toHaveBeenCalled();
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
      };

      await applyAliases(params);

      expect(mockCubeQueryToSQL).toHaveBeenCalled();
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
      };

      await applyAliases(params);

      expect(mockCubeQueryToSQL).toHaveBeenCalled();
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
          createMockTableSchema('orders', [{ name: 'owner_id', alias: 'Owner' }]),
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
      };

      await applyAliases(params);

      expect(mockCubeQueryToSQL).toHaveBeenCalled();
      const calledSchema = mockCubeQueryToSQL.mock.calls[0][0]
        .tableSchemas[0] as TableSchema;
      // Should keep original alias from aggregated schema
      expect(calledSchema.dimensions[0].alias).toBe('orders__customer_id');
    });
  });
});
