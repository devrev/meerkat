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
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { getAggregatedSql } from './steps/aggregation-step';
import { getResolvedTableSchema } from './steps/resolution-step';
import { getUnnestTableSchema } from './steps/unnest-step';

export interface CubeQueryToSQLWithResolutionParams {
  query: Query;
  tableSchemas: TableSchema[];
  resolutionConfig: ResolutionConfig;
  columnProjections?: string[];
  contextParams?: ContextParams;
}

export const cubeQueryToSQLWithResolution = async ({
  query,
  tableSchemas,
  resolutionConfig,
  columnProjections,
  contextParams,
}: CubeQueryToSQLWithResolutionParams) => {
  const baseSql = await cubeQueryToSQL({
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
    baseSql,
    query,
    tableSchemas,
    resolutionConfig,
    columnProjections,
    contextParams,
  });
};

const getCubeQueryToSQLWithResolution = async ({
  baseSql,
  query,
  tableSchemas,
  resolutionConfig,
  columnProjections,
  contextParams,
}: {
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
    [],
    columnProjections
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
    baseTableSchema: baseSchema,
    resolutionConfig,
    contextParams,
  });

  //  Apply resolution (join with lookup tables)
  const resolvedTableSchema = await getResolvedTableSchema({
    baseTableSchema: unnestTableSchema,
    resolutionConfig,
    contextParams,
    columnProjections,
  });

  // Re-aggregate to reverse the unnest
  const aggregatedSql = await getAggregatedSql({
    resolvedTableSchema,
    resolutionConfig,
    contextParams,
  });

  return aggregatedSql;
};
