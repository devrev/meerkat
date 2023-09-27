import { Dimension, Measure, TableSchema } from '@devrev/cube-types';
import { getMemberInfoFromTableSchema } from './key-from-measures-dimension';

describe('getMemberInfoFromTableSchema', () => {
  it('should return memberInfo if memberKey matches with measure key', () => {
    const memberKey = 'test.testKey';
    const measure: Measure = {
      name: 'testKey',
      sql: `table.testKey`,
      type: 'number',
    };
    const tableSchema = {
      name: 'test',
      cube: 'testCube',
      measures: [measure],
      dimensions: [],
    };
    const result = getMemberInfoFromTableSchema(memberKey, tableSchema);
    expect(result).toBe(measure);
  });

  it('should return memberInfo if memberKey matches with dimension key', () => {
    const memberKey = 'test.testKey';
    const dimension: Dimension = {
      name: 'testKey',
      sql: `table.testKey`,
      type: 'number',
    };
    const tableSchema = {
      name: 'test',
      cube: 'testCube',
      measures: [],
      dimensions: [dimension],
    };

    const result = getMemberInfoFromTableSchema(memberKey, tableSchema);
    expect(result).toBe(dimension);
  });

  it('should return memberInfo undefined when no matching key found', () => {
    const memberKey = 'testKey';
    const measure: Measure = {
      name: 'testKey',
      sql: 'no.match',
      type: 'number',
    };
    const dimension: Dimension = {
      name: 'testKey',
      sql: 'no.match',
      type: 'number',
    };
    const tableSchema: TableSchema = {
      name: 'testKey',
      cube: 'testCube',
      measures: [measure],
      dimensions: [dimension],
    };

    const result = getMemberInfoFromTableSchema(memberKey, tableSchema);
    expect(result).toBe(undefined);
  });
});
