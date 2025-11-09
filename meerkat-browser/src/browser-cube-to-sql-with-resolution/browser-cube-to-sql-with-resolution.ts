import {
  BASE_DATA_SOURCE_NAME,
  ContextParams,
  createBaseTableSchema,
  createWrapperTableSchema,
  Dimension,
  generateResolutionJoinPaths,
  generateResolutionSchemas,
  generateResolvedDimensions,
  getArrayTypeResolutionColumnConfigs,
  getNamespacedKey,
  Measure,
  memberKeyToSafeKey,
  Query,
  ResolutionConfig,
  ROW_ID_DIMENSION_NAME,
  TableSchema,
  updateArrayFlattenModifierUsingResolutionConfig,
} from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import {
  cubeQueryToSQL,
  CubeQueryToSQLParams,
} from '../browser-cube-to-sql/browser-cube-to-sql';

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
    // If no resolution is needed, return the base SQL.
    return baseSql;
  }

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

    const resolutionSchemas: TableSchema[] = generateResolutionSchemas(
      resolutionConfig,
      tableSchemas
    );

    const resolveParams: CubeQueryToSQLParams = {
      connection,
      query: {
        measures: [],
        dimensions: generateResolvedDimensions(
          BASE_DATA_SOURCE_NAME,
          query,
          resolutionConfig,
          columnProjections
        ),
        joinPaths: generateResolutionJoinPaths(
          BASE_DATA_SOURCE_NAME,
          resolutionConfig,
          tableSchemas
        ),
      },
      tableSchemas: [baseTable, ...resolutionSchemas],
    };
    const sql = await cubeQueryToSQL(resolveParams);

    return sql;
  }
};

export const cubeQueryToSQLWithResolutionWithArray = async ({
  connection,
  baseSql,
  tableSchemas,
  resolutionConfig,
  columnProjections,
  contextParams,
}: {
  connection: AsyncDuckDBConnection;
  baseSql: string;
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
    sql: 'row_number() OVER ()',
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
/**
 * Apply unnesting
 *
 * This function performs 1 step:
 * 1. Create schema with unnest modifiers for array columns
 * 2. Generate final unnested SQL
 * @returns Table schema with unnest modifiers for array columns
 */
export const getUnnestTableSchema = async ({
  connection,
  baseTableSchema,
  resolutionConfig,
  contextParams,
}: {
  connection: AsyncDuckDBConnection;
  baseTableSchema: TableSchema;
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
}): Promise<TableSchema> => {
  updateArrayFlattenModifierUsingResolutionConfig(
    baseTableSchema,
    resolutionConfig
  );

  const unnestedSql = await cubeQueryToSQL({
    connection,
    query: {
      measures: [],
      dimensions: [
        ...baseTableSchema.dimensions.map((d) =>
          getNamespacedKey(baseTableSchema.name, d.name)
        ),
      ],
    },
    tableSchemas: [baseTableSchema],
    contextParams,
  });

  const unnestedBaseTableSchema: TableSchema = createWrapperTableSchema(
    unnestedSql,
    baseTableSchema
  );

  return unnestedBaseTableSchema;
};

/**
 * Apply resolution (join with lookup tables)
 *
 * This function:
 * 1. Uses the base table schema from Phase 1 (source of truth)
 * 2. Generates resolution schemas for array fields
 * 3. Sets up join paths between unnested data and resolution tables
 * @returns Table schema with resolved values from lookup tables
 */
export const getResolvedTableSchema = async ({
  connection,
  baseTableSchema,
  resolutionConfig,
  contextParams,
  columnProjections,
}: {
  connection: AsyncDuckDBConnection;
  baseTableSchema: TableSchema;
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
  columnProjections?: string[];
}): Promise<TableSchema> => {
  const updatedBaseTableSchema: TableSchema = baseTableSchema;

  // Generate resolution schemas for fields that need resolution
  const resolutionSchemas = generateResolutionSchemas(resolutionConfig, [
    updatedBaseTableSchema,
  ]);

  const joinPaths = generateResolutionJoinPaths(
    updatedBaseTableSchema.name,
    resolutionConfig,
    [updatedBaseTableSchema]
  );

  const tempQuery: Query = {
    measures: [],
    dimensions: baseTableSchema.dimensions.map((d) =>
      getNamespacedKey(updatedBaseTableSchema.name, d.name)
    ),
  };

  const updatedColumnProjections = columnProjections?.map((cp) =>
    memberKeyToSafeKey(cp)
  );
  // Generate resolved dimensions using columnProjections
  const resolvedDimensions = generateResolvedDimensions(
    updatedBaseTableSchema.name,
    tempQuery,
    resolutionConfig,
    updatedColumnProjections
  );

  // Create query and generate SQL
  const resolutionQuery: Query = {
    measures: [],
    dimensions: resolvedDimensions,
    joinPaths,
  };

  const resolvedSql = await cubeQueryToSQL({
    connection,
    query: resolutionQuery,
    tableSchemas: [updatedBaseTableSchema, ...resolutionSchemas],
    contextParams,
  });

  // Use the baseTableSchema which already has all the column info
  const resolvedTableSchema: TableSchema = createWrapperTableSchema(
    resolvedSql,
    updatedBaseTableSchema
  );

  // Create a map of resolution schema dimensions by original column name
  const resolutionDimensionsByColumnName = new Map<string, any[]>();
  resolutionConfig.columnConfigs.forEach((config) => {
    const resSchema = resolutionSchemas.find((rs) =>
      rs.dimensions.some((dim) => dim.name.startsWith(config.name))
    );
    if (resSchema) {
      resolutionDimensionsByColumnName.set(
        config.name,
        resSchema.dimensions.map((dim) => ({
          name: dim.name,
          sql: `${resolvedTableSchema.name}."${dim.alias || dim.name}"`,
          type: dim.type,
          alias: dim.alias,
        }))
      );
    }
  });

  // Maintain the same order as baseTableSchema.dimensions
  // Replace dimensions that need resolution with their resolved counterparts
  resolvedTableSchema.dimensions = baseTableSchema.dimensions.flatMap((dim) => {
    const resolvedDims = resolutionDimensionsByColumnName.get(dim.name);
    if (resolvedDims) {
      // Replace with resolved dimensions
      return resolvedDims;
    } else {
      // Keep the original dimension with correct SQL reference
      return [dim];
    }
  });

  return resolvedTableSchema;
};

/**
 * Re-aggregate to reverse the unnest
 *
 * This function:
 * 1. Groups by row_id
 * 2. Uses MAX for non-array columns (they're duplicated)
 * 3. Uses ARRAY_AGG for resolved array columns
 *
 * @param resolvedTableSchema - Schema from Phase 2 (contains all column info)
 * @param resolutionConfig - Resolution configuration
 * @param contextParams - Optional context parameters
 * @returns Final SQL with arrays containing resolved values
 */
export const getAggregatedSql = async ({
  connection,
  resolvedTableSchema,
  resolutionConfig,
  contextParams,
}: {
  connection: AsyncDuckDBConnection;
  resolvedTableSchema: TableSchema;
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
}): Promise<string> => {
  const aggregationBaseTableSchema: TableSchema = resolvedTableSchema;

  // Identify which columns need ARRAY_AGG vs MAX
  const arrayColumns = getArrayTypeResolutionColumnConfigs(resolutionConfig);
  const baseTableName = aggregationBaseTableSchema.name;

  const isResolvedArrayColumn = (dimName: string) => {
    return arrayColumns.some((arrayCol) => {
      return dimName.includes(`${arrayCol.name}__`);
    });
  };

  // Create aggregation measures with proper aggregation functions
  // Get row_id dimension for GROUP BY
  const rowIdDimension = aggregationBaseTableSchema.dimensions.find(
    (d) => d.name === ROW_ID_DIMENSION_NAME
  );

  // Create measures with MAX or ARRAY_AGG based on column type
  const aggregationMeasures: Measure[] = [];

  aggregationBaseTableSchema.dimensions
    .filter((dim) => dim.name !== rowIdDimension?.name)
    .forEach((dim) => {
      const isArrayColumn = isResolvedArrayColumn(dim.name);

      // The dimension's sql field already has the correct reference (e.g., __resolved_query."__row_id")
      // We just need to wrap it in the aggregation function
      const columnRef =
        dim.sql || `${baseTableName}."${dim.alias || dim.name}"`;

      // Use ARRAY_AGG for resolved array columns, MAX for others
      // Filter out null values for ARRAY_AGG using FILTER clause
      const aggregationFn = isArrayColumn
        ? `COALESCE(ARRAY_AGG(DISTINCT ${columnRef}) FILTER (WHERE ${columnRef} IS NOT NULL), [])`
        : `MAX(${columnRef})`;

      aggregationMeasures.push({
        name: dim.name,
        sql: aggregationFn,
        type: dim.type,
        alias: dim.alias,
      });
    });

  // Update the schema with aggregation measures
  const schemaWithAggregation: TableSchema = {
    ...aggregationBaseTableSchema,
    measures: aggregationMeasures,
    dimensions: rowIdDimension ? [rowIdDimension] : [],
  };

  // Generate the final SQL
  const aggregatedSql = await cubeQueryToSQL({
    connection,
    query: {
      measures: aggregationMeasures.map((m) =>
        getNamespacedKey(baseTableName, m.name)
      ),
      dimensions: rowIdDimension
        ? [getNamespacedKey(baseTableName, rowIdDimension.name)]
        : [],
    },
    tableSchemas: [schemaWithAggregation],
    contextParams,
  });

  const rowIdExcludedSql = `select * exclude(${ROW_ID_DIMENSION_NAME}) from (${aggregatedSql})`;
  return rowIdExcludedSql;
};
