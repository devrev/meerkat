import {
  BASE_TABLE_NAME,
  ContextParams,
  Query,
  SqlGenerationOnEvent,
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
  timePhase,
} from '@devrev/meerkat-core';
import { duckdbExec } from '../duckdb-exec';

export interface CubeQueryToSQLParams {
  query: Query;
  tableSchemas: TableSchema[];
  contextParams?: ContextParams;
  /**
   * Optional per-call callback for SQL-generation phase timings. Invoked once
   * per phase plus once for `total`. No-op when omitted.
   */
  onEvent?: SqlGenerationOnEvent;
}

export const cubeQueryToSQL = async ({
  query,
  tableSchemas,
  contextParams,
  onEvent,
}: CubeQueryToSQLParams) => {
  const totalStart = performance.now();

  const updatedTableSchemas = await timePhase(
    'base_sql',
    () =>
      Promise.all(
        tableSchemas.map(async (schema: TableSchema) => {
          const baseFilterParamsSQL = await getFinalBaseSQL({
            query,
            tableSchema: schema,
            getQueryOutput: duckdbExec,
          });
          return {
            ...schema,
            sql: baseFilterParamsSQL,
          };
        })
      ),
    onEvent
  );

  const updatedTableSchema = getCombinedTableSchema(updatedTableSchemas, query);

  const ast = await timePhase(
    'ast_build',
    async () =>
      cubeToDuckdbAST(query, updatedTableSchema, {
        filterType: 'PROJECTION_FILTER',
      }),
    onEvent
  );
  if (!ast) {
    throw new Error('Could not generate AST');
  }

  const preBaseQuery = await timePhase(
    'ast_deserialize_roundtrip',
    async () => {
      const queryTemp = astDeserializerQuery(ast);
      const queryOutput = (await duckdbExec(queryTemp)) as Record<
        string,
        string
      >[];
      return deserializeQuery(queryOutput);
    },
    onEvent
  );

  const filterParamsSQL = await timePhase(
    'filter_params',
    () =>
      getFilterParamsSQL({
        query,
        tableSchema: updatedTableSchema,
        filterType: 'PROJECTION_FILTER',
        getQueryOutput: duckdbExec,
      }),
    onEvent
  );

  const finalQuery = await timePhase(
    'projections',
    async () => {
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
      return applySQLExpressions(queryWithProjections);
    },
    onEvent
  );

  onEvent?.({
    event_name: 'sql_generation_duration',
    phase: 'total',
    duration: performance.now() - totalStart,
  });

  return finalQuery;
};
