import { Member } from '../types/cube-types/query';
import { TableSchema } from '../types/cube-types/table';
import {
  applyProjectionToSQLQuery,
  cubeMeasureToSQLSelectString,
  getAllColumnUsedInMeasures,
} from './cube-measure-transformer';

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

  it('should construct a SQL select string with COUNT(*) when provided with correct measure', () => {
    const measures: Member[] = ['temp.measure1'];
    const result = cubeMeasureToSQLSelectString(measures, tableSchema);
    expect(result).toBe(`SELECT COUNT(*) AS temp__measure1 `);
  });

  it('should construct a SQL select string with SUM(total) when provided with correct measure', () => {
    const measures: Member[] = ['temp.measure2'];
    const result = cubeMeasureToSQLSelectString(measures, tableSchema);
    expect(result).toBe(`SELECT SUM(total) AS temp__measure2 `);
  });

  it('should substitute "*" for all columns in the cube', () => {
    const measures: Member[] = ['*'];
    const result = cubeMeasureToSQLSelectString(measures, tableSchema);
    expect(result).toBe(`SELECT test.*`);
  });

  it('should use alias for measures when provided', () => {
    const measures: Member[] = ['temp.measure1', 'temp.measure2'];
    const result = cubeMeasureToSQLSelectString(
      measures,
      tableSchemaWithAliases
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
      sqlToReplace
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
      sqlToReplace
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
      sqlToReplace
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
      sqlToReplace
    );
    expect(result).toBe(
      `SELECT COUNT(*) AS "alias_measure1" ,  SUM(total) AS "alias_measure2"  FROM my_table`
    );
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
});
