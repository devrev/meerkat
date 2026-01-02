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
  QueryOptions,
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
  /**
   * Options for controlling output format.
   * When useDotNotation is true, aliases use dot notation (e.g., "orders.customer_id")
   * When useDotNotation is false, aliases use underscore notation (e.g., orders__customer_id)
   */
  options: QueryOptions;
}

export const cubeQueryToSQLWithResolution = async ({
  connection,
  query,
  tableSchemas,
  resolutionConfig,
  columnProjections,
  contextParams,
  options,
}: CubeQueryToSQLWithResolutionParams) => {
  // Check if resolution should be skipped
  if (shouldSkipResolution(resolutionConfig, query, columnProjections)) {
    return await cubeQueryToSQL({
      connection,
      query,
      tableSchemas,
      contextParams,
      options,
    });
  }

  //
  // Why remove aliases?
  // The resolution pipeline (unnest → join lookups → re-aggregate) operates on field names
  // internally for consistency and simplicity. Using aliases throughout would complicate
  // the logic as we transform schemas through multiple steps.
  //
  // Alias handling strategy:
  // 1. Strip aliases here - work with field names (e.g., tickets__id, tickets__owners)
  // 2. Run entire resolution pipeline with field names
  // 3. At the final step (applyAliases), restore aliases from original schemas
  // 4. Generate final SQL with user-friendly aliases (e.g., "ID", "Owners")
  //
  // Benefits:
  // - Cleaner internal logic (no alias tracking through transformations)
  // - Single source of truth for aliases (original tableSchemas)
  // - Easier to debug (field names are consistent throughout pipeline)
  // - Separation of concerns (resolution logic vs. display formatting)
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
    options,
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
    columnProjections,
    options
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
    cubeQueryToSQL: async (params) =>
      cubeQueryToSQL({ connection, ...params, options }),
  });

  //  Apply resolution (join with lookup tables)
  const resolvedTableSchema = await coreGetResolvedTableSchema({
    baseTableSchema: unnestTableSchema,
    resolutionConfig,
    contextParams,
    columnProjections,
    cubeQueryToSQL: async (params) =>
      cubeQueryToSQL({ connection, ...params, options }),
    config: options,
  });

  // Re-aggregate to reverse the unnest
  const aggregatedTableSchema = await coreGetAggregatedSql({
    resolvedTableSchema,
    resolutionConfig,
    contextParams,
    cubeQueryToSQL: async (params) =>
      cubeQueryToSQL({ connection, ...params, options }),
  });

  // Apply aliases and generate final SQL
  const sqlWithAliases = await applyAliases({
    aggregatedTableSchema,
    originalTableSchemas: tableSchemas,
    resolutionConfig,
    contextParams,
    config: options,
    cubeQueryToSQL: async (params) =>
      cubeQueryToSQL({ connection, ...params, options }),
  });

  // Wrap with row_id ordering and exclusion
  return wrapWithRowIdOrderingAndExclusion(
    sqlWithAliases,
    ROW_ID_DIMENSION_NAME
  );
};
