import { AsyncDuckDBConnection } from '@devrev/duckdb-wasm';
import {
  BASE_TABLE_NAME,
  ContextParams,
  FilterType,
  Query,
  TableSchema,
  applyFilterParamsToBaseSQL,
  applyProjectionToSQLQuery,
  astDeserializerQuery,
  cubeToDuckdbAST,
  deserializeQuery,
  detectApplyContextParamsToBaseSQL,
  getCombinedTableSchema,
  getFilterParamsAST,
  getWrappedBaseQueryWithProjections,
} from '@devrev/meerkat-core';

const getFilterParamsSQL = async ({
  cubeQuery,
  tableSchema,
  filterType,
  connection,
}: {
  cubeQuery: Query;
  tableSchema: TableSchema;
  filterType?: FilterType;
  connection: AsyncDuckDBConnection;
}) => {
  const filterParamsAST = getFilterParamsAST(
    cubeQuery,
    tableSchema,
    filterType
  );
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
  return filterParamsSQL;
};

const getFinalBaseSQL = async (
  cubeQuery: Query,
  tableSchema: TableSchema,
  connection: AsyncDuckDBConnection
) => {
  /**
   * Apply transformation to the supplied base query.
   * This involves updating the filter placeholder with the actual filter values.
   */
  const baseFilterParamsSQL = await getFilterParamsSQL({
    cubeQuery: cubeQuery,
    tableSchema,
    filterType: 'BASE_FILTER',
    connection,
  });
  const baseSQL = applyFilterParamsToBaseSQL(
    tableSchema.sql,
    baseFilterParamsSQL
  );
  const baseSQLWithFilterProjection = getWrappedBaseQueryWithProjections({
    baseQuery: baseSQL,
    tableSchema,
    query: cubeQuery,
  });
  return baseSQLWithFilterProjection;
};

export const cubeQueryToSQL = async (
  connection: AsyncDuckDBConnection,
  cubeQuery: Query,
  tableSchemas: TableSchema[],
  contextParams?: ContextParams
) => {
  const updatedTableSchemas: TableSchema[] = await Promise.all(
    tableSchemas.map(async (schema: TableSchema) => {
      const baseFilterParamsSQL = await getFinalBaseSQL(
        cubeQuery,
        schema,
        connection
      );
      return {
        ...schema,
        sql: baseFilterParamsSQL,
      };
    })
  );

  const updatedTableSchema = await getCombinedTableSchema(
    updatedTableSchemas,
    cubeQuery
  );

  const ast = cubeToDuckdbAST(cubeQuery, updatedTableSchema);
  if (!ast) {
    throw new Error('Could not generate AST');
  }

  const queryTemp = astDeserializerQuery(ast);

  const arrowResult = await connection.query(queryTemp);
  const parsedOutputQuery = arrowResult.toArray().map((row) => row.toJSON());

  const preBaseQuery = deserializeQuery(parsedOutputQuery);
  const filterParamsSQL = await getFilterParamsSQL({
    connection,
    cubeQuery,
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
  const measures = cubeQuery.measures;
  const dimensions = cubeQuery.dimensions || [];
  const finalQuery = applyProjectionToSQLQuery(
    dimensions,
    measures,
    updatedTableSchema,
    replaceBaseTableName
  );

  return finalQuery;
};
