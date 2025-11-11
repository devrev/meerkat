import {
  BASE_DATA_SOURCE_NAME,
  ContextParams,
  createBaseTableSchema,
  Dimension,
  generateRowNumberSql,
  memberKeyToSafeKey,
  Query,
  ResolutionConfig,
  ROW_ID_DIMENSION_NAME,
  TableSchema,
} from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { cubeQueryToSQL } from '../browser-cube-to-sql/browser-cube-to-sql';
import { getAggregatedSql } from './steps/aggregation-step';
import { getResolvedTableSchema } from './steps/resolution-step';
import { getUnnestTableSchema } from './steps/unnest-step';

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

  // We have columnProjections check here to ensure that, we are using the same
  // order in the final query
  if (
    resolutionConfig.columnConfigs.length === 0 &&
    columnProjections?.length === 0
  ) {
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
  return getCubeQueryToSQLWithResolution({
    connection,
    baseSql,
    query,
    tableSchemas,
    resolutionConfig,
    columnProjections,
    contextParams,
  });
};

const getCubeQueryToSQLWithResolution = async ({
  connection,
  baseSql,
  query,
  tableSchemas,
  resolutionConfig,
  columnProjections,
  contextParams,
}: {
  connection: AsyncDuckDBConnection;
  baseSql: string;
  query: Query;
  tableSchemas: TableSchema[];
  resolutionConfig: ResolutionConfig;
  columnProjections: string[];
  contextParams?: ContextParams;
}): Promise<string> => {
  const baseSchema: TableSchema = createBaseTableSchema(
    baseSql,
    tableSchemas,
    resolutionConfig,
    query.measures,
    query.dimensions
  );

  baseSchema.dimensions.push({
    name: ROW_ID_DIMENSION_NAME,
    sql: generateRowNumberSql(
      query,
      baseSchema.dimensions,
      BASE_DATA_SOURCE_NAME
    ),
    type: 'number',
    alias: ROW_ID_DIMENSION_NAME,
  } as Dimension);
  columnProjections.push(ROW_ID_DIMENSION_NAME);

  // Doing this because we need to use the original name of the column in the base table schema.
  resolutionConfig.columnConfigs.forEach((config) => {
    config.name = memberKeyToSafeKey(config.name);
  });

  // Generate SQL with row_id and unnested arrays
  const unnestTableSchema = await getUnnestTableSchema({
    connection,
    baseTableSchema: baseSchema,
    resolutionConfig,
    contextParams,
  });

  //  Apply resolution (join with lookup tables)
  const resolvedTableSchema = await getResolvedTableSchema({
    connection,
    baseTableSchema: unnestTableSchema,
    resolutionConfig,
    contextParams,
    columnProjections,
  });

  // Re-aggregate to reverse the unnest
  const aggregatedSql = await getAggregatedSql({
    connection,
    resolvedTableSchema,
    resolutionConfig,
    contextParams,
  });

  return aggregatedSql;
};
