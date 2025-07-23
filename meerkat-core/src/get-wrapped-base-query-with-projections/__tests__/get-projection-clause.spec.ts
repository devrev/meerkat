import { TableSchema } from '../../types/cube-types/table';
import { getProjectionClause } from '../get-projection-clause';
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
  describe('getProjectionClause', () => {
    it('should return the projection clause when the members are present in the table schema', () => {
      const members = ['test.a', 'test.c'];
      const aliasedColumnSet = new Set<string>();
      const result = getProjectionClause(
        {
          dimensions: members,
          measures: [],
        },
        TABLE_SCHEMA,
        aliasedColumnSet
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
        aliasedColumnSet
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
        aliasedColumnSet
      );
      expect(result).toEqual('others AS test__a, test.id AS test__id');
    });
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
      aliasedColumnSet
    );
    expect(result).toEqual('others AS "test a", any AS "test c"');
  });
});
