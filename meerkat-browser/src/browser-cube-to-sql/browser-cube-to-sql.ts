import { Query, TableSchema } from '@devrev/cube-types';
import { BASE_TABLE_NAME, cubeToDuckdbAST } from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
export const cubeQueryToSQL = async (
  connection: AsyncDuckDBConnection,
  cubeQuery: Query,
  tableSchema: TableSchema
) => {
  const ast = cubeToDuckdbAST(cubeQuery, tableSchema);

  const queryTemp = `SELECT json_deserialize_sql('${JSON.stringify({
    statements: [ast],
  })}');`;

  const arrowResult = await connection.query(queryTemp);
  const parsedOutputQuery = arrowResult.toArray().map((row) => row.toJSON());

  const deserializeObj = parsedOutputQuery[0];
  const deserializeKey = Object.keys(deserializeObj)[0];
  const deserializeQuery = deserializeObj[deserializeKey];

  /**
   * Replace BASE_TABLE_NAME with cube query
   */
  const replaceBaseTableName = deserializeQuery.replace(
    BASE_TABLE_NAME,
    `(${tableSchema.sql})`
  );

  return replaceBaseTableName;
};
