import { TableSchema } from '../../types/cube-types/table';
import {
  getDimensionProjection,
  getFilterMeasureProjection,
} from '../get-aliased-columns-from-filters';

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

describe('get-aliased-columns-from-filters', () => {
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

  describe('getDimensionProjection', () => {
    it('should return the member projection when the key exists in the table schema', () => {
      const key = 'test.a';

      const result = getDimensionProjection({
        key,
        tableSchema: TABLE_SCHEMA,
        modifiers: [],
      });
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

      const result = getDimensionProjection({
        key,
        tableSchema,
        modifiers: [],
      });
      expect(result).toEqual({
        aliasKey: undefined,
        foundMember: undefined,
        sql: undefined,
      });
    });

    it('should use aliases', () => {
      const key = 'test.a';

      const result = getDimensionProjection({
        key,
        tableSchema: TABLE_SCHEMA,
        modifiers: [],
        aliases: { 'test.a': 'test a' },
      });
      expect(result).toEqual({
        aliasKey: '"test a"',
        foundMember: { name: 'a', sql: 'others', type: 'number' },
        sql: 'others AS "test a"',
      });
    });
  });
});
