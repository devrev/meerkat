import { Member } from '../types/cube-types/query';
import { TableSchema } from '../types/cube-types/table';
import {
  applyProjectionToSQLQuery,
  cubeMeasureToSQLSelectString,
  getAllColumnUsedInMeasures,
} from './cube-measure-transformer';

const defaultConfig = { useDotNotation: false };
const dotNotationConfig = { useDotNotation: true };

describe('cubeMeasureToSQLSelectString', () => {
  let tableSchema: TableSchema, tableSchemaWithAliases: TableSchema;
  const cube = 'cube_test';

  beforeEach(() => {
    tableSchema = {
      name: 'test',
      sql: cube,
      measures: [
        { name: 'measure1', sql: 'COUNT(*)', type: 'number' },
        { name: 'measure2', sql: 'SUM(total)', type: 'number' },
      ],
      dimensions: [
        { name: 'dimension1', sql: 'dimension1', type: 'number' },
        {
          name: 'dimension2',
          sql: `DATE_TRUNC('month', order_date)`,
          type: 'number',
        },
      ],
    };

    tableSchemaWithAliases = {
      name: 'test_with_aliases',
      sql: cube,
      measures: [
        {
          name: 'measure1',
          sql: 'COUNT(*)',
          type: 'number',
          alias: 'alias_measure1',
        },
        {
          name: 'measure2',
          sql: 'SUM(total)',
          type: 'number',
          alias: 'alias_measure2',
        },
      ],
      dimensions: [],
    };
  });

  describe('useDotNotation: false', () => {
    it('should construct a SQL select string with COUNT(*) when provided with correct measure', () => {
      const measures: Member[] = ['temp.measure1'];
      const result = cubeMeasureToSQLSelectString(
        measures,
        tableSchema,
        defaultConfig
      );
      expect(result).toBe(`SELECT COUNT(*) AS temp__measure1 `);
    });

    it('should construct a SQL select string with SUM(total) when provided with correct measure', () => {
      const measures: Member[] = ['temp.measure2'];
      const result = cubeMeasureToSQLSelectString(
        measures,
        tableSchema,
        defaultConfig
      );
      expect(result).toBe(`SELECT SUM(total) AS temp__measure2 `);
    });

    it('should substitute "*" for all columns in the cube', () => {
      const measures: Member[] = ['*'];
      const result = cubeMeasureToSQLSelectString(
        measures,
        tableSchema,
        defaultConfig
      );
      expect(result).toBe(`SELECT test.*`);
    });

    it('should use alias for measures when provided', () => {
      const measures: Member[] = ['temp.measure1', 'temp.measure2'];
      const result = cubeMeasureToSQLSelectString(
        measures,
        tableSchemaWithAliases,
        defaultConfig
      );
      expect(result).toBe(
        `SELECT COUNT(*) AS "alias_measure1" ,  SUM(total) AS "alias_measure2" `
      );
    });

    it('should replace the select portion of a SQL string using replaceSelectWithCubeMeasure 1', () => {
      const measures: Member[] = ['temp.measure1', 'temp.measure2'];
      const sqlToReplace = 'SELECT * FROM my_table';
      const result = applyProjectionToSQLQuery(
        [],
        measures,
        tableSchema,
        sqlToReplace,
        defaultConfig
      );
      expect(result).toBe(
        `SELECT COUNT(*) AS temp__measure1 ,  SUM(total) AS temp__measure2  FROM my_table`
      );
    });

    it('should replace the select portion of a SQL string using replaceSelectWithCubeMeasure 2', () => {
      const measures: Member[] = ['temp.measure1', 'temp.measure2'];
      const sqlToReplace = 'SELECT * FROM (SELECT * FROM TABLE_1)';
      const result = applyProjectionToSQLQuery(
        [],
        measures,
        tableSchema,
        sqlToReplace,
        defaultConfig
      );
      expect(result).toBe(
        `SELECT COUNT(*) AS temp__measure1 ,  SUM(total) AS temp__measure2  FROM (SELECT * FROM TABLE_1)`
      );
    });

    it('should replace the select portion of a SQL string using replaceSelectWithCubeMeasure & dimension', () => {
      const measures: Member[] = ['temp.measure1', 'temp.measure2'];
      const dimensions: Member[] = ['temp.dimension1', 'temp.dimension2'];
      const sqlToReplace = 'SELECT * FROM (SELECT * FROM TABLE_1)';
      const result = applyProjectionToSQLQuery(
        dimensions,
        measures,
        tableSchema,
        sqlToReplace,
        defaultConfig
      );
      expect(result).toBe(
        `SELECT COUNT(*) AS temp__measure1 ,  SUM(total) AS temp__measure2 ,   temp__dimension1,  temp__dimension2 FROM (SELECT * FROM TABLE_1)`
      );
    });

    it('should use aliases when provided', () => {
      const measures: Member[] = ['temp.measure1', 'temp.measure2'];
      const sqlToReplace = 'SELECT * FROM my_table';
      const result = applyProjectionToSQLQuery(
        [],
        measures,
        tableSchemaWithAliases,
        sqlToReplace,
        defaultConfig
      );
      expect(result).toBe(
        `SELECT COUNT(*) AS "alias_measure1" ,  SUM(total) AS "alias_measure2"  FROM my_table`
      );
    });
  });

  describe('useDotNotation: true', () => {
    it('should construct a SQL select string with COUNT(*) when provided with correct measure', () => {
      const measures: Member[] = ['temp.measure1'];
      const result = cubeMeasureToSQLSelectString(
        measures,
        tableSchema,
        dotNotationConfig
      );
      expect(result).toBe(`SELECT COUNT(*) AS "temp.measure1" `);
    });

    it('should construct a SQL select string with SUM(total) when provided with correct measure', () => {
      const measures: Member[] = ['temp.measure2'];
      const result = cubeMeasureToSQLSelectString(
        measures,
        tableSchema,
        dotNotationConfig
      );
      expect(result).toBe(`SELECT SUM(total) AS "temp.measure2" `);
    });

    it('should substitute "*" for all columns in the cube', () => {
      const measures: Member[] = ['*'];
      const result = cubeMeasureToSQLSelectString(
        measures,
        tableSchema,
        dotNotationConfig
      );
      expect(result).toBe(`SELECT test.*`);
    });

    it('should use alias for measures when provided', () => {
      const measures: Member[] = ['temp.measure1', 'temp.measure2'];
      const result = cubeMeasureToSQLSelectString(
        measures,
        tableSchemaWithAliases,
        dotNotationConfig
      );
      expect(result).toBe(
        `SELECT COUNT(*) AS "alias_measure1" ,  SUM(total) AS "alias_measure2" `
      );
    });

    it('should replace the select portion of a SQL string using replaceSelectWithCubeMeasure 1', () => {
      const measures: Member[] = ['temp.measure1', 'temp.measure2'];
      const sqlToReplace = 'SELECT * FROM my_table';
      const result = applyProjectionToSQLQuery(
        [],
        measures,
        tableSchema,
        sqlToReplace,
        dotNotationConfig
      );
      expect(result).toBe(
        `SELECT COUNT(*) AS "temp.measure1" ,  SUM(total) AS "temp.measure2"  FROM my_table`
      );
    });

    it('should replace the select portion of a SQL string using replaceSelectWithCubeMeasure 2', () => {
      const measures: Member[] = ['temp.measure1', 'temp.measure2'];
      const sqlToReplace = 'SELECT * FROM (SELECT * FROM TABLE_1)';
      const result = applyProjectionToSQLQuery(
        [],
        measures,
        tableSchema,
        sqlToReplace,
        dotNotationConfig
      );
      expect(result).toBe(
        `SELECT COUNT(*) AS "temp.measure1" ,  SUM(total) AS "temp.measure2"  FROM (SELECT * FROM TABLE_1)`
      );
    });

    it('should replace the select portion of a SQL string using replaceSelectWithCubeMeasure & dimension', () => {
      const measures: Member[] = ['temp.measure1', 'temp.measure2'];
      const dimensions: Member[] = ['temp.dimension1', 'temp.dimension2'];
      const sqlToReplace = 'SELECT * FROM (SELECT * FROM TABLE_1)';
      const result = applyProjectionToSQLQuery(
        dimensions,
        measures,
        tableSchema,
        sqlToReplace,
        dotNotationConfig
      );
      expect(result).toBe(
        `SELECT COUNT(*) AS "temp.measure1" ,  SUM(total) AS "temp.measure2" ,   "temp.dimension1",  "temp.dimension2" FROM (SELECT * FROM TABLE_1)`
      );
    });

    it('should use aliases when provided', () => {
      const measures: Member[] = ['temp.measure1', 'temp.measure2'];
      const sqlToReplace = 'SELECT * FROM my_table';
      const result = applyProjectionToSQLQuery(
        [],
        measures,
        tableSchemaWithAliases,
        sqlToReplace,
        dotNotationConfig
      );
      expect(result).toBe(
        `SELECT COUNT(*) AS "alias_measure1" ,  SUM(total) AS "alias_measure2"  FROM my_table`
      );
    });
  });
});

describe('getAllColumnUsedInMeasures', () => {
  it('should return all columns used in measures', () => {
    const tableSchema: TableSchema = {
      name: 'test',
      sql: 'test',
      measures: [
        { name: 'measure1', sql: 'COUNT(*)', type: 'number' },
        { name: 'measure2', sql: 'SUM(test.total)', type: 'number' },
      ],
      dimensions: [
        { name: 'dimension1', sql: 'dimension1', type: 'number' },
        {
          name: 'dimension2',
          sql: `DATE_TRUNC('month', order_date)`,
          type: 'number',
        },
      ],
    };
    const result = getAllColumnUsedInMeasures(
      tableSchema.measures,
      tableSchema
    );
    expect(result).toEqual(['test.total']);
  });

  it('should return all columns used in measures with no measures', () => {
    const tableSchema: TableSchema = {
      name: 'test',
      sql: 'test',
      measures: [
        { name: 'measure1', sql: 'COUNT(*)', type: 'number' },
        {
          name: 'measure2',
          sql: 'SUM(test.total)/AVG(test.amount)',
          type: 'number',
        },
      ],
      dimensions: [
        { name: 'dimension1', sql: 'dimension1', type: 'number' },
        {
          name: 'dimension2',
          sql: `DATE_TRUNC('month', order_date)`,
          type: 'number',
        },
      ],
    };
    const result = getAllColumnUsedInMeasures(
      tableSchema.measures,
      tableSchema
    );
    expect(result).toEqual(['test.total', 'test.amount']);
  });

  it('should return all columns with complex case', () => {
    const tableSchema: TableSchema = {
      name: 'test',
      sql: 'test',
      measures: [
        { name: 'measure1', sql: 'COUNT(*)', type: 'number' },
        {
          name: 'measure2',
          sql: `CASE
          WHEN COUNT(DISTINCT CASE WHEN test.sla_stage = 'breached' THEN test.id END) + COUNT(DISTINCT CASE WHEN test.sla_stage = 'completed' AND (ARRAY_LENGTH(test.next_resp_time_arr) > 0 OR ARRAY_LENGTH(test.first_resp_time_arr) > 0 OR ARRAY_LENGTH(test.resolution_time_arr) > 0) AND (test.total_second_resp_breaches_ever = 0 OR test.total_second_resp_breaches_ever IS NULL) AND (test.total_first_resp_breaches_ever = 0 OR test.total_first_resp_breaches_ever IS NULL) AND (test.total_resolution_breaches_ever = 0 OR test.total_resolution_breaches_ever IS NULL) THEN test.id END) > 0
              THEN 100 - (COUNT(DISTINCT CASE WHEN test.sla_stage = 'breached' THEN test.id END) * 100.0 /(COUNT(DISTINCT CASE WHEN test.sla_stage = 'breached' THEN test.id END) + COUNT(DISTINCT CASE WHEN test.sla_stage = 'completed' AND (ARRAY_LENGTH(test.next_resp_time_arr) > 0 OR ARRAY_LENGTH(test.first_resp_time_arr) > 0 OR ARRAY_LENGTH(test.resolution_time_arr) > 0) AND (test.total_second_resp_breaches_ever = 0 OR test.total_second_resp_breaches_ever IS NULL) AND (test.total_first_resp_breaches_ever = 0 OR test.total_first_resp_breaches_ever IS NULL) AND (test.total_resolution_breaches_ever = 0 OR test.total_resolution_breaches_ever IS NULL) THEN test.id END)))
          ELSE NULL
      END`,
          type: 'number',
        },
      ],
      dimensions: [
        { name: 'dimension1', sql: 'dimension1', type: 'number' },
        {
          name: 'dimension2',
          sql: `DATE_TRUNC('month', order_date)`,
          type: 'number',
        },
      ],
    };
    const result = getAllColumnUsedInMeasures(
      tableSchema.measures,
      tableSchema
    );
    expect(result).toEqual([
      'test.sla_stage',
      'test.id',
      'test.next_resp_time_arr',
      'test.first_resp_time_arr',
      'test.resolution_time_arr',
      'test.total_second_resp_breaches_ever',
      'test.total_first_resp_breaches_ever',
      'test.total_resolution_breaches_ever',
    ]);
  });

  describe('regex behavior - should not match quoted aliases', () => {
    it('should NOT match already-quoted dot notation aliases', () => {
      const tableSchema: TableSchema = {
        name: 'orders',
        sql: 'orders',
        measures: [
          {
            name: 'total',
            sql: 'SUM("orders.amount")',
            type: 'number',
          },
        ],
        dimensions: [],
      };
      const result = getAllColumnUsedInMeasures(
        tableSchema.measures,
        tableSchema
      );
      // Should NOT match "orders.amount" because it's quoted
      expect(result).toEqual([]);
    });

    it('should match unquoted table.column references', () => {
      const tableSchema: TableSchema = {
        name: 'orders',
        sql: 'orders',
        measures: [
          {
            name: 'total',
            sql: 'SUM(orders.amount)',
            type: 'number',
          },
        ],
        dimensions: [],
      };
      const result = getAllColumnUsedInMeasures(
        tableSchema.measures,
        tableSchema
      );
      // Should match orders.amount because it's NOT quoted
      expect(result).toEqual(['orders.amount']);
    });

    it('should match unquoted but not quoted in mixed SQL', () => {
      const tableSchema: TableSchema = {
        name: 'orders',
        sql: 'orders',
        measures: [
          {
            name: 'total',
            sql: 'SUM(orders.amount) + AVG("orders.discount")',
            type: 'number',
          },
        ],
        dimensions: [],
      };
      const result = getAllColumnUsedInMeasures(
        tableSchema.measures,
        tableSchema
      );
      // Should only match orders.amount (unquoted), not orders.discount (quoted)
      expect(result).toEqual(['orders.amount']);
    });

    it('should handle multiple unquoted columns', () => {
      const tableSchema: TableSchema = {
        name: 'orders',
        sql: 'orders',
        measures: [
          {
            name: 'ratio',
            sql: 'orders.amount / orders.quantity',
            type: 'number',
          },
        ],
        dimensions: [],
      };
      const result = getAllColumnUsedInMeasures(
        tableSchema.measures,
        tableSchema
      );
      expect(result).toEqual(['orders.amount', 'orders.quantity']);
    });

    it('should not match columns from different tables', () => {
      const tableSchema: TableSchema = {
        name: 'orders',
        sql: 'orders',
        measures: [
          {
            name: 'total',
            sql: 'SUM(orders.amount) + SUM(customers.balance)',
            type: 'number',
          },
        ],
        dimensions: [],
      };
      const result = getAllColumnUsedInMeasures(
        tableSchema.measures,
        tableSchema
      );
      // Should only match orders.amount, not customers.balance
      expect(result).toEqual(['orders.amount']);
    });
  });
});
