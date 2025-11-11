import {
  BASE_DATA_SOURCE_NAME,
  ContextParams,
  createBaseTableSchema,
  Dimension,
  generateResolutionJoinPaths,
  generateResolutionSchemas,
  generateResolvedDimensions,
  generateRowNumberSql,
  getNamespacedKey,
  memberKeyToSafeKey,
  Query,
  ResolutionConfig,
  ROW_ID_DIMENSION_NAME,
  TableSchema,
  wrapWithRowIdOrderingAndExclusion,
} from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import {
  cubeQueryToSQL,
  CubeQueryToSQLParams,
} from '../browser-cube-to-sql/browser-cube-to-sql';
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

  if (resolutionConfig.columnConfigs.length === 0) {
    return baseSql;
  }

  // Check if any columns to be resolved are array types
  if (resolutionConfig.columnConfigs.some((config) => config.isArrayType)) {
    // This is to ensure that, only the column projection columns
    // are being resolved and other definitions are ignored.
    resolutionConfig.columnConfigs = resolutionConfig.columnConfigs.filter(
      (config) => {
        return columnProjections?.includes(config.name);
      }
    );
    return cubeQueryToSQLWithResolutionWithArray({
      connection,
      baseSql,
      query,
      tableSchemas,
      resolutionConfig,
      columnProjections,
      contextParams,
    });
  } else {
    // Create a table schema for the base query.
    const baseTable: TableSchema = createBaseTableSchema(
      baseSql,
      tableSchemas,
      resolutionConfig,
      query.measures,
      query.dimensions
    );

    // Add row_id dimension to preserve ordering from base SQL
    const rowIdDimension: Dimension = {
      name: ROW_ID_DIMENSION_NAME,
      sql: generateRowNumberSql(
        query,
        baseTable.dimensions,
        BASE_DATA_SOURCE_NAME
      ),
      type: 'number',
      alias: ROW_ID_DIMENSION_NAME,
    };
    baseTable.dimensions.push(rowIdDimension);

    const resolutionSchemas: TableSchema[] = generateResolutionSchemas(
      resolutionConfig,
      tableSchemas
    );

    const resolveParams: CubeQueryToSQLParams = {
      connection,
      query: {
        measures: [],
        dimensions: [
          ...generateResolvedDimensions(
            BASE_DATA_SOURCE_NAME,
            query,
            resolutionConfig,
            columnProjections
          ),
          // Include row_id in dimensions to preserve it through the query
          getNamespacedKey(BASE_DATA_SOURCE_NAME, ROW_ID_DIMENSION_NAME),
        ],
        joinPaths: generateResolutionJoinPaths(
          BASE_DATA_SOURCE_NAME,
          resolutionConfig,
          tableSchemas
        ),
      },
      tableSchemas: [baseTable, ...resolutionSchemas],
    };
    const sql = await cubeQueryToSQL(resolveParams);

    // Order by row_id to maintain base SQL ordering, then exclude it
    return wrapWithRowIdOrderingAndExclusion(sql, ROW_ID_DIMENSION_NAME);
  }
};

export const cubeQueryToSQLWithResolutionWithArray = async ({
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
  columnProjections?: string[];
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
  columnProjections?.push(ROW_ID_DIMENSION_NAME);

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
