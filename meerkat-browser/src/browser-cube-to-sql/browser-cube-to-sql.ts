import {
  BASE_TABLE_NAME,
  Query,
  TableSchema,
  applyFilterParamsToBaseSQL,
  applyProjectionToSQLQuery,
  astDeserializerQuery,
  cubeToDuckdbAST,
  deserializeQuery,
  getFilterParamsAST,
} from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
export const cubeQueryToSQL = async (
  connection: AsyncDuckDBConnection,
  cubeQuery: Query,
  tableSchema: TableSchema
) => {
  const ast = cubeToDuckdbAST(cubeQuery, tableSchema);
  if (!ast) {
    throw new Error('Could not generate AST');
  }

  const queryTemp = astDeserializerQuery(ast);

  const arrowResult = await connection.query(queryTemp);
  const parsedOutputQuery = arrowResult.toArray().map((row) => row.toJSON());

  const preBaseQuery = deserializeQuery(parsedOutputQuery);
  const filterParamsAST = getFilterParamsAST(cubeQuery, tableSchema);
  const filterParamsSQL = [];

  for (const filterParamAST of filterParamsAST) {
    if (!filterParamAST.ast) {
      continue;
    }

    const queryOutput = await connection.query(
      astDeserializerQuery(filterParamAST.ast)
    );
    const parsedOutputQuery = queryOutput.toArray().map((row) => row.toJSON());

    const sql = deserializeQuery(parsedOutputQuery);

    filterParamsSQL.push({
      memberKey: filterParamAST.memberKey,
      sql: sql,
      matchKey: filterParamAST.matchKey,
    });
  }

  const baseQuery = applyFilterParamsToBaseSQL(
    tableSchema.sql,
    filterParamsSQL
  );

  /**
   * Replace BASE_TABLE_NAME with cube query
   */
  const replaceBaseTableName = preBaseQuery.replace(
    BASE_TABLE_NAME,
    `(${baseQuery}) AS ${tableSchema.name}`
  );

  /**
   * Add measures to the query
   */
  const measures = cubeQuery.measures;
  const dimensions = cubeQuery.dimensions || [];
  const finalQuery = applyProjectionToSQLQuery(
    dimensions,
    measures,
    tableSchema,
    replaceBaseTableName
  );

  return finalQuery;
};
