import { validateDimension } from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { parseQueryToAST } from './query-to-ast';
import { isParseError } from './utils';

/**
 * Validates a dimension query by executing it against DuckDB and checking the result format
 * @param connection - DuckDB connection instance
 * @param query - The query string to validate
 * @returns Promise<boolean> - Whether the dimension is valid
 * @throws Error if query execution or parsing fails
 */
export const isValidDimensionQuery = async ({
  connection,
  query,
}: {
  connection: AsyncDuckDBConnection;
  query: string;
}): Promise<boolean> => {
  const parsedSerialization = await parseQueryToAST(query, connection);

  if (isParseError(parsedSerialization)) {
    throw new Error(parsedSerialization.error_message ?? 'Unknown error');
  }

  return validateDimension(parsedSerialization, []);
};
