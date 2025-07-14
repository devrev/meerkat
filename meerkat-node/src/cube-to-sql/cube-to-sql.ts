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
  getAliases,
  getCombinedTableSchema,
  getFilterParamsSQL,
  getFinalBaseSQL,
} from '@devrev/meerkat-core';
import { duckdbExec } from '../duckdb-exec';

export interface CubeQueryToSQLParams {
  query: Query;
  tableSchemas: TableSchema[];
  contextParams?: ContextParams;
}

export const cubeQueryToSQL = async ({
  query,
  tableSchemas,
  contextParams,
}: CubeQueryToSQLParams) => {
  const aliases = getAliases(tableSchemas);

  const updatedTableSchemas: TableSchema[] = await Promise.all(
    tableSchemas.map(async (schema: TableSchema) => {
      const baseFilterParamsSQL = await getFinalBaseSQL({
        query,
        tableSchema: schema,
        aliases,
        getQueryOutput: duckdbExec,
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

  const ast = cubeToDuckdbAST(query, updatedTableSchema, aliases);
  if (!ast) {
    throw new Error('Could not generate AST');
  }

  const queryTemp = astDeserializerQuery(ast);

  const queryOutput = (await duckdbExec(queryTemp)) as Record<string, string>[];
  const preBaseQuery = deserializeQuery(queryOutput);

  const filterParamsSQL = await getFilterParamsSQL({
    query,
    tableSchema: updatedTableSchema,
    aliases,
    getQueryOutput: duckdbExec,
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
    replaceBaseTableName,
    aliases
  );

  return finalQuery;
};
