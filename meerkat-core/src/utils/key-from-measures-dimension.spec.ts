import { Dimension, Measure, TableSchema } from '@devrev/cube-types';
import { getMemberInfoFromTableSchema } from './key-from-measures-dimension';

describe('getMemberInfoFromTableSchema', () => {
  it('should return memberInfo if memberKey matches with measure key', () => {
    const memberKey = 'testKey';
    const measure: Measure = {
      sql: `table.${memberKey}`,
      type: 'number',
    };
    const tableSchema = {
      cube: 'testCube',
      measures: [measure],
      dimensions: [],
    };

    const result = getMemberInfoFromTableSchema(memberKey, tableSchema);
    expect(result).toBe(measure);
  });

  it('should return memberInfo if memberKey matches with dimension key', () => {
    const memberKey = 'testKey';
    const dimension: Dimension = {
      sql: `table.${memberKey}`,
      type: 'number',
    };
    const tableSchema = {
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
      sql: 'no.match',
      type: 'number',
    };
    const dimension: Dimension = {
      sql: 'no.match',
      type: 'number',
    };
    const tableSchema: TableSchema = {
      cube: 'testCube',
      measures: [measure],
      dimensions: [dimension],
    };

    const result = getMemberInfoFromTableSchema(memberKey, tableSchema);
    expect(result).toBe(undefined);
  });
});
