import {
  BASE_TABLE_NAME,
  ContextParams,
  Query,
  TableSchema,
  applyFilterParamsToBaseSQL,
  applyProjectionToSQLQuery,
  astDeserializerQuery,
  cubeToDuckdbAST,
  deserializeQuery,
  detectApplyContextParamsToBaseSQL,
  getCombinedTableSchema,
  getFilterParamsSQL,
  getFinalBaseSQL
} from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';


const getQueryOutput = async (query: string, connection: AsyncDuckDBConnection) => {
  const queryOutput = await connection.query(query);
  const parsedOutputQuery = queryOutput.toArray().map((row) => row.toJSON());
  return parsedOutputQuery;
}


interface CubeQueryToSQLParams {
  connection: AsyncDuckDBConnection,
  query: Query,
  tableSchemas: TableSchema[],
  contextParams?: ContextParams,
}

export const cubeQueryToSQL = async ({
  connection,
  query,
  tableSchemas,
  contextParams
}: CubeQueryToSQLParams) => {
  const updatedTableSchemas: TableSchema[] = await Promise.all(
    tableSchemas.map(async (schema: TableSchema) => {
      const baseFilterParamsSQL = await getFinalBaseSQL({
        query,
        tableSchema: schema,
        getQueryOutput: (query) => getQueryOutput(query, connection)
      });
      return {
        ...schema,
        sql: baseFilterParamsSQL,
      };
    })
  );

  const updatedTableSchema = await getCombinedTableSchema(
    updatedTableSchemas,
    query
  );

  const ast = cubeToDuckdbAST(query, updatedTableSchema);
  if (!ast) {
    throw new Error('Could not generate AST');
  }

  const queryTemp = astDeserializerQuery(ast);

  const arrowResult = await connection.query(queryTemp);
  const parsedOutputQuery = arrowResult.toArray().map((row) => row.toJSON());

  const preBaseQuery = deserializeQuery(parsedOutputQuery);
  const filterParamsSQL = await getFilterParamsSQL({
    getQueryOutput: (query) => getQueryOutput(query, connection),
    query,
    tableSchema: updatedTableSchema,
    filterType: 'BASE_FILTER',
  });

  const filterParamQuery = applyFilterParamsToBaseSQL(
    updatedTableSchema.sql,
    filterParamsSQL
  );

  /**
   * Replace CONTEXT_PARAMS with context params
   */
  const baseQuery = detectApplyContextParamsToBaseSQL(
    filterParamQuery,
    contextParams || {}
  );

  /**
   * Replace BASE_TABLE_NAME with cube query
   */
  const replaceBaseTableName = preBaseQuery.replace(
    BASE_TABLE_NAME,
    `(${baseQuery}) AS ${updatedTableSchema.name}`
  );

  /**
   * Add measures to the query
   */
  const measures = query.measures;
  const dimensions = query.dimensions || [];
  const finalQuery = applyProjectionToSQLQuery(
    dimensions,
    measures,
    updatedTableSchema,
    replaceBaseTableName
  );

  return finalQuery;
};
