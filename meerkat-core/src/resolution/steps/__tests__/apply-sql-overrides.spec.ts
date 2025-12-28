import { TableSchema } from '../../../types/cube-types';
import { ResolutionConfig } from '../../types';
import { applySqlOverrides } from '../apply-sql-overrides';

describe('apply-sql-overrides', () => {
  describe('applySqlOverrides', () => {
    const createBaseSchema = (
      dimensions: { name: string; sql: string; type: string }[] = [],
      measures: { name: string; sql: string; type: string }[] = []
    ): TableSchema => ({
      name: '__base_query',
      sql: 'SELECT * FROM base',
      dimensions,
      measures,
    });

    it('should return base schema when no override configs', () => {
      const baseSchema = createBaseSchema([
        { name: 'issues__priority', sql: 'priority', type: 'number' },
      ]);
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = applySqlOverrides(baseSchema, config);

      expect(result).toBe(baseSchema);
    });

    it('should return base schema when override configs is empty array', () => {
      const baseSchema = createBaseSchema([
        { name: 'issues__priority', sql: 'priority', type: 'number' },
      ]);
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
        sqlOverrideConfigs: [],
      };

      const result = applySqlOverrides(baseSchema, config);

      expect(result).toBe(baseSchema);
    });

    it('should override dimension SQL with converted field names', () => {
      const baseSchema = createBaseSchema([
        { name: 'issues__priority', sql: 'priority', type: 'number' },
      ]);
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
        sqlOverrideConfigs: [
          {
            fieldName: 'issues.priority',
            overrideSql:
              "CASE WHEN issues.priority = 1 THEN 'P0' WHEN issues.priority = 2 THEN 'P1' END",
            type: 'string',
          },
        ],
      };

      const result = applySqlOverrides(baseSchema, config);

      expect(result.dimensions[0].sql).toBe(
        "CASE WHEN issues__priority = 1 THEN 'P0' WHEN issues__priority = 2 THEN 'P1' END"
      );
      expect(result.dimensions[0].type).toBe('string');
    });

    it('should override measure SQL with converted field names', () => {
      const baseSchema = createBaseSchema(
        [],
        [{ name: 'orders__total', sql: 'SUM(total)', type: 'number' }]
      );
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
        sqlOverrideConfigs: [
          {
            fieldName: 'orders.total',
            overrideSql: 'ROUND(orders.total, 2)',
            type: 'number',
          },
        ],
      };

      const result = applySqlOverrides(baseSchema, config);

      expect(result.measures[0].sql).toBe('ROUND(orders__total, 2)');
    });

    it('should handle multiple override configs', () => {
      const baseSchema = createBaseSchema(
        [{ name: 'issues__priority', sql: 'priority', type: 'number' }],
        [{ name: 'orders__total', sql: 'SUM(total)', type: 'number' }]
      );
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
        sqlOverrideConfigs: [
          {
            fieldName: 'issues.priority',
            overrideSql: 'issues.priority * 10',
            type: 'number',
          },
          {
            fieldName: 'orders.total',
            overrideSql: 'orders.total + 100',
            type: 'number',
          },
        ],
      };

      const result = applySqlOverrides(baseSchema, config);

      expect(result.dimensions[0].sql).toBe('issues__priority * 10');
      expect(result.measures[0].sql).toBe('orders__total + 100');
    });

    it('should replace multiple occurrences of field name', () => {
      const baseSchema = createBaseSchema([
        { name: 'issues__priority', sql: 'priority', type: 'number' },
      ]);
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
        sqlOverrideConfigs: [
          {
            fieldName: 'issues.priority',
            overrideSql: 'issues.priority + issues.priority + issues.priority',
            type: 'number',
          },
        ],
      };

      const result = applySqlOverrides(baseSchema, config);

      expect(result.dimensions[0].sql).toBe(
        'issues__priority + issues__priority + issues__priority'
      );
    });

    it('should handle nested dot notation in field names', () => {
      const baseSchema = createBaseSchema([
        {
          name: 'issues__nested__priority',
          sql: 'priority',
          type: 'number',
        },
      ]);
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
        sqlOverrideConfigs: [
          {
            fieldName: 'issues.nested.priority',
            overrideSql: 'issues.nested.priority * 2',
            type: 'number',
          },
        ],
      };

      const result = applySqlOverrides(baseSchema, config);

      expect(result.dimensions[0].sql).toBe('issues__nested__priority * 2');
    });

    it('should not modify original schema', () => {
      const baseSchema = createBaseSchema([
        { name: 'issues__priority', sql: 'priority', type: 'number' },
      ]);
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
        sqlOverrideConfigs: [
          {
            fieldName: 'issues.priority',
            overrideSql: 'issues.priority * 10',
            type: 'string',
          },
        ],
      };

      applySqlOverrides(baseSchema, config);

      expect(baseSchema.dimensions[0].sql).toBe('priority');
      expect(baseSchema.dimensions[0].type).toBe('number');
    });

    describe('error handling', () => {
      it('should throw error when override SQL does not reference field name', () => {
        const baseSchema = createBaseSchema([
          { name: 'issues__priority', sql: 'priority', type: 'number' },
        ]);
        const config: ResolutionConfig = {
          columnConfigs: [],
          tableSchemas: [],
          sqlOverrideConfigs: [
            {
              fieldName: 'issues.priority',
              overrideSql: 'some_unrelated_sql',
              type: 'string',
            },
          ],
        };

        expect(() => applySqlOverrides(baseSchema, config)).toThrow(
          "SQL override for field 'issues.priority' must reference the field in the SQL"
        );
      });
    });

    it('should skip override when dimension/measure not found in schema', () => {
      const baseSchema = createBaseSchema([
        { name: 'issues__priority', sql: 'priority', type: 'number' },
      ]);
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
        sqlOverrideConfigs: [
          {
            fieldName: 'issues.nonexistent',
            overrideSql: 'issues.nonexistent * 10',
            type: 'number',
          },
        ],
      };

      const result = applySqlOverrides(baseSchema, config);

      // Original schema should be unchanged (field not found)
      expect(result.dimensions[0].sql).toBe('priority');
    });
  });
});
