import { validateMeasure } from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { parseQueryToAST } from './query-to-ast';
import { getAvailableFunctions, isParseError } from './utils';

/**
 * Validates the query can be used as a measure by parsing it to an AST and checking its structure
 * @param connection - DuckDB connection instance
 * @param query - The query string to validate
 * @returns Promise<boolean> - Whether the measure is valid
 */
export const validateMeasureQuery = async ({
  connection,
  query,
  validFunctions,
}: {
  connection: AsyncDuckDBConnection;
  query: string;
  validFunctions?: string[];
}): Promise<boolean> => {
  const parsedSerialization = await parseQueryToAST(query, connection);

  if (isParseError(parsedSerialization)) {
    throw new Error(parsedSerialization.error_message ?? 'Unknown error');
  }

  // Only fetch valid functions if not provided
  const availableFunctions =
    validFunctions ?? (await getAvailableFunctions(connection, 'aggregate'));

  return validateMeasure(parsedSerialization, availableFunctions);
};
