import { convertRowsToRecords } from '../convert-rows-to-records';

describe('convertRowsToRecords', () => {
  it('should convert rows and column names into an array of records', () => {
    const rows = [
      [1, 'John', true],
      [2, 'Jane', false],
    ];
    const columnNames = ['id', 'name', 'active'];

    const result = convertRowsToRecords(rows, columnNames);

    expect(result).toEqual([
      { id: 1, name: 'John', active: true },
      { id: 2, name: 'Jane', active: false },
    ]);
  });

  it('should handle empty rows', () => {
    const rows: any[][] = [];
    const columnNames = ['id', 'name'];

    const result = convertRowsToRecords(rows, columnNames);

    expect(result).toEqual([]);
  });

  it('should handle rows with null values', () => {
    const rows = [
      [1, null, 'test'],
      [2, undefined, null],
    ];
    const columnNames = ['id', 'optional', 'description'];

    const result = convertRowsToRecords(rows, columnNames);

    expect(result).toEqual([
      { id: 1, optional: null, description: 'test' },
      { id: 2, optional: undefined, description: null },
    ]);
  });
});
