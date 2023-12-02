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
  getFilterParamsAST,
} from '@devrev/meerkat-core';
import { duckdbExec } from '../duckdb-exec';

export const cubeQueryToSQL = async (
  cubeQuery: Query,
  tableSchema: TableSchema,
  contextParams?: ContextParams
) => {
  const ast = cubeToDuckdbAST(cubeQuery, tableSchema);

  if (!ast) {
    throw new Error('Could not generate AST');
  }

  const queryTemp = astDeserializerQuery(ast);

  const queryOutput = await duckdbExec<
    {
      [key: string]: string;
    }[]
  >(queryTemp);

  const preBaseQuery = deserializeQuery(queryOutput);

  const filterParamsAST = getFilterParamsAST(cubeQuery, tableSchema);
  const filterParamsSQL = [];

  for (const filterParamAST of filterParamsAST) {
    if (!filterParamAST.ast) {
      continue;
    }

    const queryOutput = await duckdbExec<
      {
        [key: string]: string;
      }[]
    >(astDeserializerQuery(filterParamAST.ast));

    const sql = deserializeQuery(queryOutput);

    filterParamsSQL.push({
      memberKey: filterParamAST.memberKey,
      sql: sql,
      matchKey: filterParamAST.matchKey,
    });
  }

  const filterParamQuery = applyFilterParamsToBaseSQL(
    tableSchema.sql,
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
