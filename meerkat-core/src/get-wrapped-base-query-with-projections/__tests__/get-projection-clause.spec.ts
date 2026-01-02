import { TableSchema } from '../../types/cube-types/table';
import { getProjectionClause } from '../get-projection-clause';

const defaultConfig = { useDotNotation: false };
const dotNotationConfig = { useDotNotation: true };

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
};

describe('get-projection-clause', () => {
  describe('getProjectionClause (useDotNotation: false)', () => {
    it('should return the projection clause when the members are present in the table schema', () => {
      const members = ['test.a', 'test.c'];
      const aliasedColumnSet = new Set<string>();
      const result = getProjectionClause(
        {
          dimensions: members,
          measures: [],
        },
        TABLE_SCHEMA,
        aliasedColumnSet,
        defaultConfig
      );
      expect(result).toEqual('others AS test__a, any AS test__c');
    });

    it('should skip aliased items present in already seen', () => {
      const members = ['test.a', 'test.c'];
      const aliasedColumnSet = new Set<string>(['test__c']);
      const result = getProjectionClause(
        {
          dimensions: members,
          measures: [],
        },
        TABLE_SCHEMA,
        aliasedColumnSet,
        defaultConfig
      );
      expect(result).toEqual('others AS test__a');
    });

    it('should project columns used inside the measure string', () => {
      const members = ['test.a', 'test.c'];
      const aliasedColumnSet = new Set<string>(['test__c']);
      const result = getProjectionClause(
        {
          measures: ['test.total_rows'],
          dimensions: members,
        },
        TABLE_SCHEMA,
        aliasedColumnSet,
        defaultConfig
      );
      expect(result).toEqual('others AS test__a, test.id AS test__id');
    });

    it('should apply aliases', () => {
      const members = ['test.a', 'test.c'];
      const aliasedColumnSet = new Set<string>();
      const tableSchema: TableSchema = {
        ...TABLE_SCHEMA,
        dimensions: [
          { name: 'a', sql: 'others', type: 'number', alias: 'test a' },
          { name: 'c', sql: 'any', type: 'string', alias: 'test c' },
        ],
      };
      const result = getProjectionClause(
        {
          dimensions: members,
          measures: [],
        },
        tableSchema,
        aliasedColumnSet,
        defaultConfig
      );
      expect(result).toEqual('others AS "test a", any AS "test c"');
    });
  });

  describe('getProjectionClause (useDotNotation: true)', () => {
    it('should return the projection clause when the members are present in the table schema', () => {
      const members = ['test.a', 'test.c'];
      const aliasedColumnSet = new Set<string>();
      const result = getProjectionClause(
        {
          dimensions: members,
          measures: [],
        },
        TABLE_SCHEMA,
        aliasedColumnSet,
        dotNotationConfig
      );
      expect(result).toEqual('others AS "test.a", any AS "test.c"');
    });

    it('should skip aliased items present in already seen', () => {
      const members = ['test.a', 'test.c'];
      const aliasedColumnSet = new Set<string>(['"test.c"']);
      const result = getProjectionClause(
        {
          dimensions: members,
          measures: [],
        },
        TABLE_SCHEMA,
        aliasedColumnSet,
        dotNotationConfig
      );
      expect(result).toEqual('others AS "test.a"');
    });

    it('should project columns used inside the measure string', () => {
      const members = ['test.a', 'test.c'];
      const aliasedColumnSet = new Set<string>(['"test.c"']);
      const result = getProjectionClause(
        {
          measures: ['test.total_rows'],
          dimensions: members,
        },
        TABLE_SCHEMA,
        aliasedColumnSet,
        dotNotationConfig
      );
      expect(result).toEqual('others AS "test.a", test.id AS "test.id"');
    });

    it('should apply aliases', () => {
      const members = ['test.a', 'test.c'];
      const aliasedColumnSet = new Set<string>();
      const tableSchema: TableSchema = {
        ...TABLE_SCHEMA,
        dimensions: [
          { name: 'a', sql: 'others', type: 'number', alias: 'test a' },
          { name: 'c', sql: 'any', type: 'string', alias: 'test c' },
        ],
      };
      const result = getProjectionClause(
        {
          dimensions: members,
          measures: [],
        },
        tableSchema,
        aliasedColumnSet,
        dotNotationConfig
      );
      expect(result).toEqual('others AS "test a", any AS "test c"');
    });
  });
});
