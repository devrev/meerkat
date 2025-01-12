import {
  astSerializerQuery,
  deserializeQuery,
  ParsedSerialization,
} from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';

/**
 * Converts a query to an AST
 * @param query - The query string to convert
 * @param connection - The DuckDB connection instance
 * @returns The AST as a JSON object
 */
export const parseQueryToAST = async (
  query: string,
  connection: AsyncDuckDBConnection
): Promise<ParsedSerialization> => {
  const serializedQuery = astSerializerQuery(query);
  const arrowResult = await connection.query(serializedQuery);

  const parsedOutputQuery = arrowResult.toArray().map((row) => row.toJSON());
  const deserializedQuery = deserializeQuery(parsedOutputQuery);

  return JSON.parse(deserializedQuery);
};
