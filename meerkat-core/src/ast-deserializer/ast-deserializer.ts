import { SelectStatement } from '../types/duckdb-serialization-types';

export const astDeserializerQuery = (ast: SelectStatement) => {
  return `SELECT json_deserialize_sql('${JSON.stringify({
    statements: [ast],
  })}');`;
};

export const deserializeQuery = (
  queryOutput: {
    [key: string]: string;
  }[]
) => {
  const deserializeObj = queryOutput[0];
  const deserializeKey = Object.keys(deserializeObj)[0];
  const deserializeQuery = deserializeObj[deserializeKey];
  return deserializeQuery;
};

const BATCH_DESERIALIZE_ALIAS_PREFIX = 'meerkat_deser_';

const getBatchDeserializeAlias = (index: number) =>
  `${BATCH_DESERIALIZE_ALIAS_PREFIX}${index}`;

/**
 * Deserialize many ASTs in a single DuckDB round-trip.
 *
 * The naive pipeline issues one `SELECT json_deserialize_sql(...)` per AST,
 * so N filter-param placeholders cost N serial trips to DuckDB purely to
 * turn JSON AST back into SQL text. This packs every AST into one SELECT with
 * one aliased column per statement, so the whole batch costs a single trip.
 *
 * Returns null for an empty input so callers can skip execution entirely.
 */
export const batchAstDeserializerQuery = (
  asts: SelectStatement[]
): string | null => {
  if (asts.length === 0) {
    return null;
  }

  const columns = asts
    .map((ast, index) => {
      const payload = JSON.stringify({ statements: [ast] });
      return `json_deserialize_sql('${payload}') AS ${getBatchDeserializeAlias(
        index
      )}`;
    })
    .join(', ');

  return `SELECT ${columns};`;
};

/**
 * Read the deserialized SQL strings back out of a batch round-trip, in the
 * same order they were supplied to `batchAstDeserializerQuery`.
 */
export const deserializeBatchQuery = (
  queryOutput: {
    [key: string]: string;
  }[],
  count: number
): string[] => {
  const row = queryOutput[0];
  const results: string[] = [];
  for (let index = 0; index < count; index += 1) {
    results.push(row[getBatchDeserializeAlias(index)]);
  }
  return results;
};
