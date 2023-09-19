import { Dimension, Measure, TableSchema } from '@devrev/cube-types';
import {
  getMemberInfoFromTableSchema,
  tableKeyFromMeasuresDimension,
} from './key-from-measures-dimension';

describe('tableKeyFromMeasuresDimension function test', () => {
  it('should return the correct key from measures', () => {
    const query = {
      measures: ['measure1.value', 'measure2.value'],
      dimensions: ['dimension1.value', 'dimension2.value'],
    };

    const result = tableKeyFromMeasuresDimension(query);
    expect(result).toBe('measure1');
  });

  it('should return the correct key from dimensions if no measure key found', () => {
    const query = {
      measures: ['.nokey'],
      dimensions: ['dimension1.value', 'dimension2.value'],
    };

    const result = tableKeyFromMeasuresDimension(query);
    expect(result).toBe('dimension1');
  });

  it('should return null if no valid keys found', () => {
    const query = {
      measures: ['.nokey'],
      dimensions: ['.nokey'],
    };

    const result = tableKeyFromMeasuresDimension(query);
    expect(result).toBeNull();
  });

  it('should return null if dimensions does not exist', () => {
    const query = {
      measures: ['.nokey'],
    };

    const result = tableKeyFromMeasuresDimension(query);
    expect(result).toBeNull();
  });
});

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
