import { SelectStatement } from '../types/duckdb-serialization-types';

export const astDeserializerQuery = (ast: SelectStatement) => {
  return `SELECT json_deserialize_sql('${JSON.stringify({
    statements: [ast],
  })}');`;
};
