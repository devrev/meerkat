import { fetchDuckDBFunctions, ParsedSerialization } from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';

export const isParseError = (data: ParsedSerialization): boolean => {
  return !!data.error;
};

// Wraps AsyncDuckDBConnection into GetQueryOutput interface, then delegates to core.
export const getAvailableFunctions = async (
  connection: AsyncDuckDBConnection,
  function_type: 'scalar' | 'aggregate'
): Promise<string[]> => {
  const getQueryOutput = async (query: string) => {
    const result = await connection.query(query);
    return result.toArray().map((row) => row.toJSON());
  };
  const fnSet = await fetchDuckDBFunctions(getQueryOutput, function_type);
  return [...fnSet];
};
