import { DuckDBValue } from '@duckdb/node-api';

export const convertRowsToRecords = (
  rows: DuckDBValue[][],
  columnNames: string[]
) => {
  return rows.map((row) => {
    return columnNames.reduce((obj, columnName, index) => {
      obj[columnName] = row[index];
      return obj;
    }, {} as Record<string, DuckDBValue>);
  });
};
