import {
  applyAliases,
  applySqlOverrides,
  BASE_DATA_SOURCE_NAME,
  ContextParams,
  getAggregatedSql as coreGetAggregatedSql,
  getResolvedTableSchema as coreGetResolvedTableSchema,
  getUnnestTableSchema as coreGetUnnestTableSchema,
  createBaseTableSchema,
  Dimension,
  generateRowNumberSql,
  memberKeyToSafeKey,
  Query,
  ResolutionConfig,
  ROW_ID_DIMENSION_NAME,
  shouldSkipResolution,
  TableSchema,
  wrapWithRowIdOrderingAndExclusion,
} from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { cubeQueryToSQL } from '../browser-cube-to-sql/browser-cube-to-sql';

export interface CubeQueryToSQLWithResolutionParams {
  connection: AsyncDuckDBConnection;
  query: Query;
  tableSchemas: TableSchema[];
  resolutionConfig: ResolutionConfig;
  columnProjections?: string[];
  contextParams?: ContextParams;
}

export const cubeQueryToSQLWithResolution = async ({
  connection,
  query,
  tableSchemas,
  resolutionConfig,
  columnProjections,
  contextParams,
}: CubeQueryToSQLWithResolutionParams) => {
  // Check if resolution should be skipped
  if (shouldSkipResolution(resolutionConfig, query, columnProjections)) {
    return await cubeQueryToSQL({
      connection,
      query,
      tableSchemas,
      contextParams,
    });
  }

  // Resolution is needed - create alias-free tableSchemas for resolution pipeline
  const tableSchemasWithoutAliases: TableSchema[] = tableSchemas.map(
    (schema) => ({
      ...schema,
      dimensions: schema.dimensions.map((dim) => ({
        ...dim,
        alias: undefined, // Strip alias for resolution pipeline
      })),
      measures: schema.measures.map((measure) => ({
        ...measure,
        alias: undefined, // Strip alias
      })),
    })
  );

  const baseSql = await cubeQueryToSQL({
    connection,
    query,
    tableSchemas: tableSchemasWithoutAliases,
    contextParams,
  });

  if (!columnProjections) {
    columnProjections = [...(query.dimensions || []), ...query.measures];
  }
  // This is to ensure that, only the column projection columns
  // are being resolved and other definitions are ignored.
  resolutionConfig.columnConfigs = resolutionConfig.columnConfigs.filter(
    (config) => {
      return columnProjections?.includes(config.name);
    }
  );

  const baseSchema: TableSchema = createBaseTableSchema(
    baseSql,
    tableSchemasWithoutAliases, // Use alias-free schemas
    resolutionConfig,
    [],
    columnProjections
  );

  // At this point, filters/sorts are baked into baseSql using original values
  // We can now override dimensions/measures in the base schema with custom SQL expressions for display
  const schemaWithOverrides = applySqlOverrides(baseSchema, resolutionConfig);

  // Transform field names in configs to match base table schema format
  resolutionConfig.columnConfigs.forEach((config) => {
    config.name = memberKeyToSafeKey(config.name);
  });

  const rowIdDimension: Dimension = {
    name: ROW_ID_DIMENSION_NAME,
    sql: generateRowNumberSql(
      query,
      schemaWithOverrides.dimensions,
      BASE_DATA_SOURCE_NAME
    ),
    type: 'number',
    alias: ROW_ID_DIMENSION_NAME,
  };
  schemaWithOverrides.dimensions.push(rowIdDimension);
  columnProjections.push(ROW_ID_DIMENSION_NAME);

  // Generate SQL with row_id and unnested arrays
  const unnestTableSchema = await coreGetUnnestTableSchema({
    baseTableSchema: schemaWithOverrides,
    resolutionConfig,
    contextParams,
    cubeQueryToSQL: async (params) => cubeQueryToSQL({ connection, ...params }),
  });

  //  Apply resolution (join with lookup tables)
  const resolvedTableSchema = await coreGetResolvedTableSchema({
    baseTableSchema: unnestTableSchema,
    resolutionConfig,
    contextParams,
    columnProjections,
    cubeQueryToSQL: async (params) => cubeQueryToSQL({ connection, ...params }),
  });

  // Re-aggregate to reverse the unnest
  const aggregatedTableSchema = await coreGetAggregatedSql({
    resolvedTableSchema,
    resolutionConfig,
    contextParams,
    cubeQueryToSQL: async (params) => cubeQueryToSQL({ connection, ...params }),
  });

  // Apply aliases and generate final SQL
  const sqlWithAliases = await applyAliases({
    aggregatedTableSchema,
    originalTableSchemas: tableSchemas,
    resolutionConfig,
    contextParams,
    cubeQueryToSQL: async (params) => cubeQueryToSQL({ connection, ...params }),
  });

  // Wrap with row_id ordering and exclusion
  return wrapWithRowIdOrderingAndExclusion(
    sqlWithAliases,
    ROW_ID_DIMENSION_NAME
  );
};
