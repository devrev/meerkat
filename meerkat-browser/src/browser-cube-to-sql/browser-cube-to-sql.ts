import {
  BASE_TABLE_NAME,
  ContextParams,
  Query,
  QueryOptions,
  TableSchema,
  applyFilterParamsToBaseSQL,
  applyProjectionToSQLQuery,
  applySQLExpressions,
  astDeserializerQuery,
  cubeToDuckdbAST,
  deserializeQuery,
  detectApplyContextParamsToBaseSQL,
  getCombinedTableSchema,
  getFilterParamsSQL,
  getFinalBaseSQL,
} from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';

const getQueryOutput = async (
  query: string,
  connection: AsyncDuckDBConnection
) => {
  const queryOutput = await connection.query(query);
  const parsedOutputQuery = queryOutput.toArray().map((row) => row.toJSON());
  return parsedOutputQuery;
};

export interface CubeQueryToSQLParams {
  connection: AsyncDuckDBConnection;
  query: Query;
  tableSchemas: TableSchema[];
  contextParams?: ContextParams;
  /**
   * Options for controlling output format.
   * When useDotNotation is true, aliases use dot notation (e.g., "orders.customer_id")
   * When useDotNotation is false, aliases use underscore notation (e.g., orders__customer_id)
   */
  options: QueryOptions;
}

export const cubeQueryToSQL = async ({
  connection,
  query,
  tableSchemas,
  contextParams,
  options,
}: CubeQueryToSQLParams) => {
  const updatedTableSchemas: TableSchema[] = await Promise.all(
    tableSchemas.map(async (schema: TableSchema) => {
      const baseFilterParamsSQL = await getFinalBaseSQL({
        query,
        tableSchema: schema,
        getQueryOutput: (query) => getQueryOutput(query, connection),
        config: options,
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

  const ast = cubeToDuckdbAST(query, updatedTableSchema, {
    filterType: 'PROJECTION_FILTER',
    config: options,
  });
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
    filterType: 'PROJECTION_FILTER',
    config: options,
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
  const queryWithProjections = applyProjectionToSQLQuery(
    dimensions,
    measures,
    updatedTableSchema,
    replaceBaseTableName,
    options
  );

  /**
   * Replace SQL expression placeholders with actual SQL
   */
  const finalQuery = applySQLExpressions(queryWithProjections);

  return finalQuery;
};
