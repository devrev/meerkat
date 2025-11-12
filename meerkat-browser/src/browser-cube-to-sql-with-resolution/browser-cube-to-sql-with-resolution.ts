import {
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
  const baseSql = await cubeQueryToSQL({
    connection,
    query,
    tableSchemas,
    contextParams,
  });

  // Check if resolution should be skipped
  if (shouldSkipResolution(resolutionConfig, columnProjections)) {
    return baseSql;
  }

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
    tableSchemas,
    resolutionConfig,
    query.measures,
    query.dimensions
  );

  const rowIdDimension: Dimension = {
    name: ROW_ID_DIMENSION_NAME,
    sql: generateRowNumberSql(
      query,
      baseSchema.dimensions,
      BASE_DATA_SOURCE_NAME
    ),
    type: 'number',
    alias: ROW_ID_DIMENSION_NAME,
  };
  baseSchema.dimensions.push(rowIdDimension);
  columnProjections.push(ROW_ID_DIMENSION_NAME);

  // Doing this because we need to use the original name of the column in the base table schema.
  resolutionConfig.columnConfigs.forEach((config) => {
    config.name = memberKeyToSafeKey(config.name);
  });

  // Generate SQL with row_id and unnested arrays
  const unnestTableSchema = await coreGetUnnestTableSchema({
    baseTableSchema: baseSchema,
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
  const aggregatedSql = await coreGetAggregatedSql({
    resolvedTableSchema,
    resolutionConfig,
    contextParams,
    cubeQueryToSQL: async (params) => cubeQueryToSQL({ connection, ...params }),
  });

  return aggregatedSql;
};
