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
import { duckdbExec } from '../duckdb-exec';

const getFilterParamsSQL = async ({
  cubeQuery,
  tableSchema,
  filterType,
}: {
  cubeQuery: Query;
  tableSchema: TableSchema;
  filterType?: FilterType;
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
  return filterParamsSQL;
};

const getFinalBaseSQL = async (cubeQuery: Query, tableSchema: TableSchema) => {
  /**
   * Apply transformation to the supplied base query.
   * This involves updating the filter placeholder with the actual filter values.
   */
  const baseFilterParamsSQL = await getFilterParamsSQL({
    cubeQuery: cubeQuery,
    tableSchema,
    filterType: 'BASE_FILTER',
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
  cubeQuery: Query,
  tableSchemas: TableSchema[],
  contextParams?: ContextParams
) => {
  const updatedTableSchemas: TableSchema[] = await Promise.all(
    tableSchemas.map(async (schema: TableSchema) => {
      const baseFilterParamsSQL = await getFinalBaseSQL(cubeQuery, schema);
      return {
        ...schema,
        sql: baseFilterParamsSQL,
      };
    })
  );

  const updatedTableSchema = await getCombinedTableSchema(updatedTableSchemas);

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

  const filterParamsSQL = await getFilterParamsSQL({
    cubeQuery,
    tableSchema: updatedTableSchema,
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
