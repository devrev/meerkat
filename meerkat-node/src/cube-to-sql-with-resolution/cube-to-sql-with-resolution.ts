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
import {
  cubeQueryToSQL,
  CubeQueryToSQLParams,
} from '../cube-to-sql/cube-to-sql';

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

  if (resolutionConfig.columnConfigs.length === 0) {
    // If no resolution is needed, return the base SQL.
    return baseSql;
  }

  // Step 2: Check if array-type resolution is needed
  if (resolutionConfig.columnConfigs.some((config) => config.isArrayType)) {
    // Delegate to array handler, passing baseSql instead of query
    return cubeQueryToSQLWithResolutionWithArray({
      baseSql,
      measures: query.measures,
      dimensions: query.dimensions || [],
      tableSchemas,
      resolutionConfig,
      columnProjections,
      contextParams,
    });
  }

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
    [baseTable]
  );

  const resolveParams: CubeQueryToSQLParams = {
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
        [baseTable]
      ),
    },
    tableSchemas: [baseTable, ...resolutionSchemas],
  };
  const sql = await cubeQueryToSQL(resolveParams);

  return sql;
};

export interface CubeQueryToSQLWithResolutionWithArrayParams {
  baseSql: string;
  measures: string[];
  dimensions: string[];
  tableSchemas: TableSchema[];
  resolutionConfig: ResolutionConfig;
  columnProjections?: string[];
  contextParams?: ContextParams;
}

export const cubeQueryToSQLWithResolutionWithArray = async ({
  baseSql,
  measures,
  dimensions,
  tableSchemas,
  resolutionConfig,
  columnProjections,
  contextParams,
}: CubeQueryToSQLWithResolutionWithArrayParams) => {
  // Step 1: Create schema for the base SQL
  const baseSchema: TableSchema = createBaseTableSchema(
    baseSql,
    tableSchemas,
    resolutionConfig,
    measures,
    dimensions
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

  // Phase 1: Generate SQL with row_id and unnested arrays
  const unnestTableSchema = await getUnnestTableSchema({
    baseTableSchema: baseSchema,
    resolutionConfig,
    contextParams,
  });

  // Phase 2: Apply resolution (join with lookup tables)
  const resolvedTableSchema = await getResolvedSql({
    baseTableSchema: unnestTableSchema,
    resolutionConfig,
    contextParams,
    columnProjections,
  });

  // Phase 3: Re-aggregate to reverse the unnest
  const aggregatedSql = await getAggregatedSql({
    resolvedTableSchema,
    resolutionConfig,
    contextParams,
  });

  return aggregatedSql;
};

/**
 * Phase 1: Apply unnesting
 *
 * This function performs 1 step:
 * 1. Create schema with unnest modifiers for array columns
 * 2. Generate final unnested SQL
 * @returns Table schema with unnest modifiers for array columns
 */
export const getUnnestTableSchema = async ({
  baseTableSchema,
  resolutionConfig,
  contextParams,
}: {
  baseTableSchema: TableSchema;
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
}): Promise<TableSchema> => {
  updateArrayFlattenModifierUsingResolutionConfig(
    baseTableSchema,
    resolutionConfig
  );

  const unnestedSql = await cubeQueryToSQL({
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
 * Phase 2: Apply resolution (join with lookup tables)
 *
 * This function:
 * 1. Uses the base table schema from Phase 1 (source of truth)
 * 2. Generates resolution schemas for array fields
 * 3. Sets up join paths between unnested data and resolution tables
 * @returns Table schema with resolved values from lookup tables
 */
export const getResolvedSql = async ({
  baseTableSchema,
  resolutionConfig,
  contextParams,
  columnProjections,
}: {
  baseTableSchema: TableSchema;
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
  columnProjections?: string[];
}): Promise<TableSchema> => {
  const updatedBaseTableSchema: TableSchema = baseTableSchema;

  // Generate resolution schemas for array fields
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
    query: resolutionQuery,
    tableSchemas: [updatedBaseTableSchema, ...resolutionSchemas],
    contextParams,
  });

  // Use the baseTableSchema which already has all the column info
  const baseDimensionNames = new Set(
    baseTableSchema.dimensions
      .filter((dim) => {
        // Exclude columns that need resolution (they'll be replaced by resolved columns)
        return !resolutionConfig.columnConfigs.some(
          (ac) => ac.name === dim.name
        );
      })
      .map((dim) => dim.name)
  );

  const resolvedTableSchema: TableSchema = createWrapperTableSchema(
    resolvedSql,
    updatedBaseTableSchema
  );
  resolvedTableSchema.dimensions = resolvedTableSchema.dimensions.filter(
    (dim) => baseDimensionNames.has(dim.name)
  );
  resolvedTableSchema.dimensions.push(
    ...resolutionSchemas.flatMap((resSchema) =>
      resSchema.dimensions.map((dim) => ({
        name: dim.name,
        sql: `${resolvedTableSchema.name}."${dim.alias || dim.name}"`,
        type: dim.type,
        alias: dim.alias,
      }))
    )
  );
  return resolvedTableSchema;
};

/**
 * Phase 3: Re-aggregate to reverse the unnest
 *
 * This function:
 * 1. Wraps Phase 2 SQL as a new base table
 * 2. Groups by row_id
 * 3. Uses MAX for non-array columns (they're duplicated)
 * 4. Uses ARRAY_AGG for resolved array columns
 *
 * @param resolvedSql - SQL output from Phase 2 (with resolved values)
 * @param resolvedTableSchema - Schema from Phase 2 (contains all column info)
 * @param resolutionConfig - Resolution configuration
 * @param contextParams - Optional context parameters
 * @returns Final SQL with arrays containing resolved values
 */
export const getAggregatedSql = async ({
  resolvedTableSchema,
  resolutionConfig,
  contextParams,
}: {
  resolvedTableSchema: TableSchema;
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
}): Promise<string> => {
  const aggregationBaseTableSchema: TableSchema = resolvedTableSchema;

  // Step 2: Identify which columns need ARRAY_AGG vs MAX
  const arrayColumns = getArrayTypeResolutionColumnConfigs(resolutionConfig);
  const baseTableName = aggregationBaseTableSchema.name;

  const isResolvedArrayColumn = (dimName: string) => {
    return arrayColumns.some((arrayCol) => {
      return dimName.includes(`${arrayCol.name}__`);
    });
  };

  // Step 3: Create aggregation measures with proper aggregation functions
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
      const aggregationFn = isArrayColumn
        ? `ARRAY_AGG(DISTINCT ${columnRef})`
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

  // Step 5: Generate the final SQL
  const aggregatedSql = await cubeQueryToSQL({
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
