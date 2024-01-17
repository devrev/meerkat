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
  getAliasedColumnsFromFilters,
  getFilterParamsAST,
  getReplacedSQL
} from '@devrev/meerkat-core';
import { duckdbExec } from '../duckdb-exec';

const getWrappedBaseQueryWithProjections = ({ baseQuery, tableSchema, query }: { baseQuery: string, tableSchema: TableSchema, query: Query }) => {
  if (!query.filters) {
    return baseQuery
  }
  const newBaseSql = `SELECT * FROM (${baseQuery}) AS ${tableSchema.name}`;
  const aliasedColumns = getAliasedColumnsFromFilters({
    meerkatFilters: query.filters,
    tableSchema, baseSql: 'SELECT *',
    members: [...query.measures, ...(query.dimensions ?? [])]
  })
  const sqlWithFilterProjects = getReplacedSQL(newBaseSql, aliasedColumns)
  return sqlWithFilterProjects
}

export const cubeQueryToSQL = async (
  cubeQuery: Query,
  tableSchema: TableSchema,
  contextParams?: ContextParams
) => {
  const newSql = getWrappedBaseQueryWithProjections({ baseQuery: tableSchema.sql, tableSchema, query: cubeQuery })
  const updatedTableSchema: TableSchema = { ...tableSchema, sql: newSql }
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
  const filterParamsAST = getFilterParamsAST(cubeQuery, updatedTableSchema);
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
