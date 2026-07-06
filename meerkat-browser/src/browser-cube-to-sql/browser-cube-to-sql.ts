import {
  BASE_TABLE_NAME,
  ContextParams,
  Query,
  TableSchema,
  applyFilterParamsToBaseSQL,
  applyProjectionToSQLQuery,
  applySQLExpressions,
  astDeserializerQuery,
  buildPreBaseQuerySync,
  canBuildPreBaseQuerySync,
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

/**
 * Produce the outer query skeleton (`preBaseQuery`).
 *
 * For queries without a projection-filter WHERE clause we build the string
 * synchronously in TS, skipping the `cubeToDuckdbAST -> json_deserialize_sql`
 * DuckDB round-trip. In the browser each round-trip is a message hop to the
 * duckdb-wasm Web Worker, so skipping it is more valuable than in node.
 * Otherwise we fall back to the AST round-trip so the WHERE operator grammar
 * stays correct.
 */
const getPreBaseQuery = async (
  query: Query,
  updatedTableSchema: TableSchema,
  connection: AsyncDuckDBConnection
): Promise<string> => {
  if (canBuildPreBaseQuerySync(query)) {
    return buildPreBaseQuerySync(query, updatedTableSchema);
  }

  const ast = cubeToDuckdbAST(query, updatedTableSchema, {
    filterType: 'PROJECTION_FILTER',
  });
  if (!ast) {
    throw new Error('Could not generate AST');
  }
  const arrowResult = await connection.query(astDeserializerQuery(ast));
  const parsedOutputQuery = arrowResult.toArray().map((row) => row.toJSON());
  return deserializeQuery(parsedOutputQuery);
};

export interface CubeQueryToSQLParams {
  connection: AsyncDuckDBConnection;
  query: Query;
  tableSchemas: TableSchema[];
  contextParams?: ContextParams;
}

export const cubeQueryToSQL = async ({
  connection,
  query,
  tableSchemas,
  contextParams,
}: CubeQueryToSQLParams) => {
  const updatedTableSchemas: TableSchema[] = await Promise.all(
    tableSchemas.map(async (schema: TableSchema) => {
      const baseFilterParamsSQL = await getFinalBaseSQL({
        query,
        tableSchema: schema,
        getQueryOutput: (query) => getQueryOutput(query, connection),
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

  // The preBaseQuery build (AST round-trip or AST-free) and the
  // PROJECTION_FILTER param resolution are independent, so run them
  // concurrently.
  const [preBaseQuery, filterParamsSQL] = await Promise.all([
    getPreBaseQuery(query, updatedTableSchema, connection),
    getFilterParamsSQL({
      getQueryOutput: (query) => getQueryOutput(query, connection),
      query,
      tableSchema: updatedTableSchema,
      filterType: 'PROJECTION_FILTER',
    }),
  ]);

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
    updatedTableSchemas,
    replaceBaseTableName
  );

  /**
   * Replace SQL expression placeholders with actual SQL
   */
  const finalQuery = applySQLExpressions(queryWithProjections);

  return finalQuery;
};
