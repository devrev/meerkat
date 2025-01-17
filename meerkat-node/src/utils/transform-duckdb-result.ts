import { DuckDBResult, DuckDBType } from '@duckdb/node-api';
import { convertRowsToRecords } from './convert-rows-to-records';
import { convertRecordDuckDBValueToJSON } from './duckdb-type-convertor';

export interface ColumnMetadata {
  name: string;
  type: DuckDBType;
}

export interface QueryResult {
  data: Record<string, unknown>[];
  schema: ColumnMetadata[];
}

/**
 * Converts raw DuckDB query results into a structured format with named objects
 */
export const transformDuckDBQueryResult = async (
  result: DuckDBResult
): Promise<QueryResult> => {
  const columnNames = result.columnNames();
  const columnTypes = result.columnTypes();

  const columns = columnNames.map((name, index) => ({
    name,
    type: columnTypes[index],
  }));

  const rows = await result.getRows();

  const records = convertRowsToRecords(rows, result.columnNames());

  const data = convertRecordDuckDBValueToJSON(records, columns);

  return { data, schema: columns };
};
