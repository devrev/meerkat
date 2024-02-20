import { TableSchema } from '../types/cube-types';
import {
  getDimensionProjection,
  getFilterMeasureProjection,
  getProjectionClause,
} from './get-projection-clause';

const TABLE_SCHEMA: TableSchema = {
  dimensions: [
    { name: 'a', sql: 'others', type: 'number' },
    { name: 'c', sql: 'any', type: 'string' },
  ],
  measures: [
    { name: 'x', sql: 'x', type: 'number' },
    { name: 'y', sql: 'y', type: 'number' },
    { name: 'z', sql: 'z', type: 'number' },
    { name: 'total_rows', sql: 'count(test.id)', type: 'number' },
  ],
  name: 'test',
  sql: 'SELECT * from test',
  // Define your table schema here
};
describe('get-projection-clause', () => {
  describe('getDimensionProjection', () => {
    it('should return the member projection when the key exists in the table schema', () => {
      const key = 'test.a';

      const result = getDimensionProjection({ key, tableSchema: TABLE_SCHEMA });
      expect(result).toEqual({
        aliasKey: 'test__a',
        foundMember: { name: 'a', sql: 'others', type: 'number' },
        sql: 'others AS test__a',
      });
    });

    it("should return the object with undefined values when the key doesn't exist in the table schema", () => {
      const key = 'test.a';
      const tableSchema: TableSchema = {
        ...TABLE_SCHEMA,
        dimensions: [{ name: 'b', sql: 'others', type: 'number' }],
      };

      const result = getDimensionProjection({ key, tableSchema });
      expect(result).toEqual({
        aliasKey: undefined,
        foundMember: undefined,
        sql: undefined,
      });
    });
  });

  describe('getFilterMeasureProjection', () => {
    it('should return the member projection when the key exists in the table schema', () => {
      const key = 'test.x';
      const result = getFilterMeasureProjection({
        key,
        tableSchema: TABLE_SCHEMA,
        measures: ['test.a'],
      });
      expect(result).toEqual({
        aliasKey: 'test__x',
        foundMember: { name: 'x', sql: 'x', type: 'number' },
        sql: 'test.x AS test__x',
      });
    });

    it('should not create alias when item in measure list', () => {
      const key = 'test.x';
      const result = getFilterMeasureProjection({
        key,
        tableSchema: TABLE_SCHEMA,
        measures: ['test.x'],
      });
      expect(result).toEqual({
        aliasKey: undefined,
        foundMember: undefined,
        sql: undefined,
      });
    });

    it("should return the object with undefined values when the key doesn't exist in the table schema", () => {
      const key = 'test.a';
      const tableSchema: TableSchema = {
        ...TABLE_SCHEMA,
        measures: [{ name: 'b', sql: 'others', type: 'number' }],
      };

      const result = getFilterMeasureProjection({
        key,
        tableSchema,
        measures: ['test.b'],
      });
      expect(result).toEqual({
        aliasKey: undefined,
        foundMember: undefined,
        sql: undefined,
      });
    });
  });

  describe('getProjectionClause', () => {
    it('should return the projection clause when the members are present in the table schema', () => {
      const members = ['test.a', 'test.c'];
      const aliasedColumnSet = new Set<string>();
      const result = getProjectionClause(
        [],
        members,
        TABLE_SCHEMA,
        aliasedColumnSet
      );
      expect(result).toEqual('others AS test__a, any AS test__c');
    });
    it('should skip aliased items present in already seen', () => {
      const members = ['test.a', 'test.c'];
      const aliasedColumnSet = new Set<string>(['test.c']);
      const result = getProjectionClause(
        [],
        members,
        TABLE_SCHEMA,
        aliasedColumnSet
      );
      expect(result).toEqual('others AS test__a');
    });

    it('should project columns used inside the measure string', () => {
      const members = ['test.a', 'test.c'];
      const aliasedColumnSet = new Set<string>(['test.c']);
      const result = getProjectionClause(
        ['test.total_rows'],
        members,
        TABLE_SCHEMA,
        aliasedColumnSet
      );
      expect(result).toEqual('others AS test__a, test.id AS test__id');
    });
  });
});
