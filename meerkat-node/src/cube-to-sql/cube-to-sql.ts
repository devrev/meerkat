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
  getAliasedColumnsFromFilters,
  getFilterParamsAST,
  getSelectReplacedSql
} from '@devrev/meerkat-core';
import { duckdbExec } from '../duckdb-exec';

const getWrappedBaseQueryWithProjections = ({ baseQuery, tableSchema, query }: { baseQuery: string, tableSchema: TableSchema, query: Query }) => {
  /*
  * Im order to be able to filter on computed metric from a query, we need to project the computed metric in the base query.
  * If theres filters supplied, we can safely return the original base query. Since nothing need to be projected and filtered in this case
  */
  if (!query?.filters?.length) {
    return baseQuery
  }
  // Wrap the query into another 'SELECT * FROM (baseQuery) AS baseTable'' in order to project everything in the base query, and other computed metrics to be able to filter on them
  const newBaseSql = `SELECT * FROM (${baseQuery}) AS ${tableSchema.name}`;
  const aliasedColumns = getAliasedColumnsFromFilters({
    meerkatFilters: query.filters,
    tableSchema, baseSql: 'SELECT *',
    members: [...query.measures, ...(query.dimensions ?? [])],
    aliasedColumnSet: new Set<string>()
  })
  // Append the aliased columns to the base query select statement
  const sqlWithFilterProjects = getSelectReplacedSql(newBaseSql, aliasedColumns)
  return sqlWithFilterProjects
}

const getFilterParamsSQL = async ({ cubeQuery, tableSchema, filterType }: { cubeQuery: Query, tableSchema: TableSchema, filterType?: FilterType }) => {
  const filterParamsAST = getFilterParamsAST(cubeQuery, tableSchema, filterType);
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
  return filterParamsSQL
}

const getFinalBaseSQL = async (cubeQuery: Query, tableSchema: TableSchema) => {
  /**
   * Apply transformation to the supplied base query.
   * This involves updating the filter placeholder with the actual filter values. 
   */
  const baseFilterParamsSQL = await getFilterParamsSQL({ cubeQuery: cubeQuery, tableSchema, filterType: 'BASE_FILTER' })
  const baseSQL = applyFilterParamsToBaseSQL(tableSchema.sql, baseFilterParamsSQL)
  const baseSQLWithFilterProjection = getWrappedBaseQueryWithProjections({ baseQuery: baseSQL, tableSchema, query: cubeQuery })
  return baseSQLWithFilterProjection
}

export const cubeQueryToSQL = async (
  cubeQuery: Query,
  tableSchema: TableSchema,
  contextParams?: ContextParams
) => {
  const baseFilterParamsSQL = await getFinalBaseSQL(cubeQuery, tableSchema)

  const updatedTableSchema: TableSchema = { ...tableSchema, sql: baseFilterParamsSQL }

  const ast = cubeToDuckdbAST(cubeQuery, updatedTableSchema);
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
 
  const filterParamsSQL = await getFilterParamsSQL({ cubeQuery, tableSchema })

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
    replaceBaseTableName,
  );

  return finalQuery;
};
 