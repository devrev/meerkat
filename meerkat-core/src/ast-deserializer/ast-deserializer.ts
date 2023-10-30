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
