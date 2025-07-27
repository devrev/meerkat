import { Dimension, Measure } from '../../types/cube-types';
import { CUBE_TYPE_TO_DUCKDB_TYPE } from '../../utils/cube-type-to-duckdb-type';
import { valueBuilder } from './base-condition-builder';

describe('valueBuilder', () => {
  it('should build a value for a string type', () => {
    const memberInfo: Measure | Dimension = {
      name: 'test',
      type: 'string',
      sql: 'test',
    };
    const value = 'test_value';
    const result = valueBuilder(value, memberInfo);
    expect(result).toEqual({
      type: {
        id: CUBE_TYPE_TO_DUCKDB_TYPE.string,
        type_info: null,
      },
      is_null: false,
      value: value,
    });
  });

  it('should build a value for a string type with single quotes', () => {
    const memberInfo: Measure | Dimension = {
      name: 'test',
      type: 'string',
      sql: 'test',
    };

    const value = "I'm a string with a single quote";
    const result = valueBuilder(value, memberInfo);
    expect(result).toEqual({
      type: {
        id: CUBE_TYPE_TO_DUCKDB_TYPE.string,
        type_info: null,
      },
      is_null: false,
      value: "I''m a string with a single quote",
    });
  });

  it('should build a value for a number type', () => {
    const memberInfo: Measure | Dimension = {
      name: 'test',
      type: 'number',
      sql: 'test',
    };
    const value = '123';
    const result = valueBuilder(value, memberInfo);

    expect(result).toEqual({
      type: {
        id: CUBE_TYPE_TO_DUCKDB_TYPE.number,
        type_info: {
          alias: '',
          type: 'DECIMAL_TYPE_INFO',
          width: 3,
          scale: 0,
        },
      },
      is_null: false,
      value: 123,
    });
  });

  it('should build a value for a boolean type', () => {
    const memberInfo: Measure | Dimension = {
      name: 'test',
      type: 'boolean',
      sql: 'test',
    };
    const value = 'true';
    const result = valueBuilder(value, memberInfo);
    expect(result).toEqual({
      type: {
        id: CUBE_TYPE_TO_DUCKDB_TYPE.boolean,
        type_info: null,
      },
      is_null: false,
      value: true,
    });
  });

  it('should build a value for a time type', () => {
    const memberInfo: Measure | Dimension = {
      name: 'test',
      type: 'time',
      sql: 'test',
    };
    const value = '2023-01-01';
    const result = valueBuilder(value, memberInfo);
    expect(result).toEqual({
      type: {
        id: CUBE_TYPE_TO_DUCKDB_TYPE.time,
        type_info: null,
      },
      is_null: false,
      value: value,
    });
  });

  it('should default to string for unknown types', () => {
    const memberInfo: Measure | Dimension = {
      name: 'test',
      type: 'unknown' as any,
      sql: 'test',
    };
    const value = 'test_value';
    const result = valueBuilder(value, memberInfo);
    expect(result).toEqual({
      type: {
        id: CUBE_TYPE_TO_DUCKDB_TYPE.string,
        type_info: null,
      },
      is_null: false,
      value: value,
    });
  });
});
