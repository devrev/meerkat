import {
  astSerializerQuery,
  deserializeQuery,
  DimensionResponse,
  isValidDimension,
  ParsedSerialization,
} from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';

export const executeAndParseQuery = async ({
  connection,
  query,
}: {
  connection: AsyncDuckDBConnection;
  query: string;
}): Promise<ParsedSerialization | null> => {
  const serializedQuery = astSerializerQuery(query);
  const arrowResult = await connection.query(serializedQuery);

  if (!arrowResult) {
    throw new Error('Query execution returned no results');
  }

  const parsedOutputQuery = arrowResult.toArray().map((row) => row.toJSON());

  if (!parsedOutputQuery.length) {
    return null;
  }

  const deserializedQuery = deserializeQuery(parsedOutputQuery);
  return JSON.parse(deserializedQuery);
};

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
}: {
  connection: AsyncDuckDBConnection;
  query: string;
}): Promise<DimensionResponse> => {
  try {
    const data = await executeAndParseQuery({ connection, query });
    if (!data)
      return { isValid: false, error: 'Query execution returned no results' };

    return isValidDimension(data);
  } catch (error) {
    throw new Error(`Dimension validation failed: ${(error as Error).message}`);
  }
};
