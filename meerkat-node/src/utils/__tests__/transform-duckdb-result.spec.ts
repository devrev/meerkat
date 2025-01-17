import { DuckDBResult, DuckDBTypeId } from '@duckdb/node-api';
import { transformDuckDBQueryResult } from '../transform-duckdb-result';

describe('transformDuckDBQueryResult', () => {
  it('should transform DuckDB result into structured format', async () => {
    const mockResult: jest.Mocked<DuckDBResult> = {
      columnNames: jest.fn().mockReturnValue(['id', 'name', 'age']),
      columnTypes: jest
        .fn()
        .mockReturnValue([
          DuckDBTypeId.INTEGER,
          DuckDBTypeId.VARCHAR,
          DuckDBTypeId.INTEGER,
        ]),
      getRows: jest.fn().mockResolvedValue([
        [1, 'John Doe', 30],
        [2, 'Jane Smith', 25],
      ]),
    } as any;

    const result = await transformDuckDBQueryResult(mockResult);

    expect(result).toEqual({
      data: [
        { id: 1, name: 'John Doe', age: 30 },
        { id: 2, name: 'Jane Smith', age: 25 },
      ],
      schema: [
        { name: 'id', type: DuckDBTypeId.INTEGER },
        { name: 'name', type: DuckDBTypeId.VARCHAR },
        { name: 'age', type: DuckDBTypeId.INTEGER },
      ],
    });
  });

  it('should handle empty result set', async () => {
    const mockResult: jest.Mocked<DuckDBResult> = {
      columnNames: jest.fn().mockReturnValue(['id', 'name']),
      columnTypes: jest
        .fn()
        .mockReturnValue([DuckDBTypeId.INTEGER, DuckDBTypeId.VARCHAR]),
      getRows: jest.fn().mockResolvedValue([]),
    } as any;

    const result = await transformDuckDBQueryResult(mockResult);

    expect(result).toEqual({
      data: [],
      schema: [
        { name: 'id', type: DuckDBTypeId.INTEGER },
        { name: 'name', type: DuckDBTypeId.VARCHAR },
      ],
    });
  });

  it('should handle various DuckDB data types', async () => {
    const mockResult: jest.Mocked<DuckDBResult> = {
      columnNames: jest.fn().mockReturnValue(['num', 'text', 'bool', 'date']),
      columnTypes: jest
        .fn()
        .mockReturnValue([
          DuckDBTypeId.DOUBLE,
          DuckDBTypeId.VARCHAR,
          DuckDBTypeId.BOOLEAN,
          DuckDBTypeId.DATE,
        ]),
      getRows: jest
        .fn()
        .mockResolvedValue([
          [123.45, 'text value', true, new Date('2024-01-01')],
        ]),
    } as any;

    const result = await transformDuckDBQueryResult(mockResult);

    expect(result).toEqual({
      data: [
        {
          num: 123.45,
          text: 'text value',
          bool: true,
          date: new Date('2024-01-01'),
        },
      ],
      schema: [
        { name: 'num', type: DuckDBTypeId.DOUBLE },
        { name: 'text', type: DuckDBTypeId.VARCHAR },
        { name: 'bool', type: DuckDBTypeId.BOOLEAN },
        { name: 'date', type: DuckDBTypeId.DATE },
      ],
    });
  });
});
