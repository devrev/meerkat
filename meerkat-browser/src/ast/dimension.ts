import { validateDimension } from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { parseQueryToAST } from './query-to-ast';
import { getAvailableFunctions, isParseError } from './utils';

/**
 * Validates a dimension query by executing it against DuckDB and checking the result format
 * @param connection - DuckDB connection instance
 * @param query - The query string to validate
 * @returns Promise<boolean> - Whether the dimension is valid
 * @throws Error if query execution or parsing fails
 */
export const validateDimensionQuery = async ({
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
    validFunctions ?? (await getAvailableFunctions(connection, 'scalar'));

  return validateDimension(parsedSerialization, availableFunctions);
};
