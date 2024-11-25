import { ParsedSerialization } from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';

export const isParseError = (data: ParsedSerialization): boolean => {
  return !!data.error;
};

// Helper function to get available functions from DuckDB based on function type
export const getAvailableFunctions = async (
  connection: AsyncDuckDBConnection,
  function_type: 'scalar' | 'aggregate'
): Promise<string[]> => {
  const result = await connection.query(
    `SELECT distinct function_name FROM duckdb_functions() WHERE function_type = '${function_type}'`
  );

  return result.toArray().map((row) => row.toJSON().function_name);
};
