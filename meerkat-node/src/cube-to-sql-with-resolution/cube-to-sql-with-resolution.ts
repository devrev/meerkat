import {
  ContextParams,
  createBaseTableSchema,
  Dimension,
  generateResolutionJoinPaths,
  generateResolutionSchemas,
  generateResolvedDimensions,
  Query,
  ResolutionConfig,
  TableSchema,
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
  if (resolutionConfig.columnConfigs.some((config) => config.isArrayType)) {
    query.dimensions?.push(`${tableSchemas[0].name}.row_id`);
    tableSchemas[0].dimensions.push({
      name: 'row_id',
      sql: 'row_number() OVER ()',
      type: 'number' as const,
      alias: '__row_id',
    } as Dimension);
    columnProjections = [
      `${tableSchemas[0].name}.row_id`,
      ...(columnProjections || []),
    ];
    return cubeQueryToSQLWithResolutionWithArray({
      query,
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
    tableSchemas
  );

  const resolveParams: CubeQueryToSQLParams = {
    query: {
      measures: [],
      dimensions: generateResolvedDimensions(
        query,
        resolutionConfig,
        columnProjections
      ),
      joinPaths: generateResolutionJoinPaths(resolutionConfig, tableSchemas),
    },
    tableSchemas: [baseTable, ...resolutionSchemas],
  };
  const sql = await cubeQueryToSQL(resolveParams);

  return sql;
};

/**
 * Helper function to get array-type columns from resolution config
 */
const getArrayTypeColumns = (resolutionConfig: ResolutionConfig) => {
  return resolutionConfig.columnConfigs.filter(
    (config) => config.isArrayType === true
  );
};

export const cubeQueryToSQLWithResolutionWithArray = async ({
  query,
  tableSchemas,
  resolutionConfig,
  columnProjections,
  contextParams,
}: CubeQueryToSQLWithResolutionParams) => {
  // Generate SQL with row_id and unnested arrays
  const { sql: unnestBaseSql, baseTableSchema } = await getUnnestBaseSql({
    query,
    tableSchemas,
    resolutionConfig,
    contextParams,
  });

  // Apply resolution (join with lookup tables)
  const { sql: resolvedSql, resolvedTableSchema } = await getResolvedSql({
    unnestedSql: unnestBaseSql,
    baseTableSchema,
    query,
    tableSchemas,
    resolutionConfig,
    contextParams,
    columnProjections,
  });

  // Phase 3: Re-aggregate to reverse the unnest
  const aggregatedSql = await getAggregatedSql({
    resolvedSql,
    resolvedTableSchema,
    query,
    resolutionConfig,
    contextParams,
  });

  return aggregatedSql;
};

export const getUnnestBaseSql = async ({
  query,
  tableSchemas,
  resolutionConfig,
  contextParams,
}: CubeQueryToSQLWithResolutionParams): Promise<{
  sql: string;
  baseTableSchema: TableSchema;
}> => {
  // Generate base SQL with row_id
  const baseSqlWithRowId = await cubeQueryToSQL({
    query: query,
    tableSchemas: tableSchemas,
    contextParams,
  });

  // This will be used to apply unnesting
  const baseTableName = '__base_query';

  const baseTableSchema: TableSchema = createBaseTableSchema(
    baseSqlWithRowId,
    tableSchemas,
    resolutionConfig,
    query.measures,
    query.dimensions
  );

  // Create query with unnest modifiers applied
  const unnestQuery: Query = {
    measures: [],
    dimensions: [
      ...query.measures.map((m) => `${baseTableName}.${m.replace('.', '__')}`),
      ...(query.dimensions || []).map(
        (d) => `${baseTableName}.${d.replace('.', '__')}`
      ),
    ],
  };

  // Generate the final SQL with unnesting applied
  const unnestedBaseSql = await cubeQueryToSQL({
    query: unnestQuery,
    tableSchemas: [baseTableSchema],
    contextParams,
  });

  return {
    sql: unnestedBaseSql,
    baseTableSchema,
  };
};

/**
 * Phase 2: Apply resolution (join with lookup tables)
 *
 * This function:
 * 1. Uses the base table schema from Phase 1 (source of truth)
 * 2. Generates resolution schemas for array fields
 * 3. Sets up join paths between unnested data and resolution tables
 * 4. Generates SQL with resolved values
 *
 * @param unnestedSql - SQL output from Phase 1 (with row_id and unnested arrays)
 * @param baseTableSchema - Schema from Phase 1 that describes the unnested SQL
 * @returns SQL with row_id, unnested arrays, and resolved values from lookup tables
 */
export const getResolvedSql = async ({
  unnestedSql,
  baseTableSchema,
  query,
  tableSchemas,
  resolutionConfig,
  contextParams,
  columnProjections,
}: {
  unnestedSql: string;
  baseTableSchema: TableSchema;
  query: Query;
  tableSchemas: TableSchema[];
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
  columnProjections?: string[];
}): Promise<{
  sql: string;
  resolvedTableSchema: TableSchema;
}> => {
  // Update the SQL to point to the unnested SQL
  const updatedBaseTableSchema: TableSchema = {
    ...baseTableSchema,
    sql: unnestedSql,
  };

  // Generate resolution schemas for array fields only
  const resolutionSchemas = generateResolutionSchemas(
    resolutionConfig,
    tableSchemas
  );

  // Generate join paths using existing helper
  const joinPaths = generateResolutionJoinPaths(resolutionConfig, tableSchemas);

  // Generate resolved dimensions
  const resolvedDimensions = generateResolvedDimensions(
    query,
    resolutionConfig,
    columnProjections
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

  // Build list of dimension names that should be in output
  const baseDimensionNames = new Set([
    ...query.measures.map((m) => m.replace('.', '__')),
    ...(query.dimensions || [])
      .filter(
        (d) => !resolutionConfig.columnConfigs.some((ac) => ac.name === d)
      )
      .map((d) => d.replace('.', '__')),
  ]);

  const resolvedTableSchema: TableSchema = {
    name: '__resolved_query',
    sql: resolvedSql,
    measures: [],
    dimensions: [
      // Dimensions from base table that were queried (row_id, measures, non-resolved dimensions)
      ...updatedBaseTableSchema.dimensions
        .filter((dim) => baseDimensionNames.has(dim.name))
        .map((dim) => ({
          name: dim.name,
          sql: `__resolved_query."${dim.alias || dim.name}"`,
          type: dim.type,
          alias: dim.alias,
        })),
      // All dimensions from resolution schemas (resolved columns from JOINs)
      ...resolutionSchemas.flatMap((resSchema) =>
        resSchema.dimensions.map((dim) => ({
          name: dim.name,
          sql: `__resolved_query."${dim.alias || dim.name}"`,
          type: dim.type,
          alias: dim.alias,
        }))
      ),
    ],
  };

  return {
    sql: resolvedSql,
    resolvedTableSchema,
  };
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
 * @param query - Original query
 * @param resolutionConfig - Resolution configuration
 * @param contextParams - Optional context parameters
 * @returns Final SQL with arrays containing resolved values
 */
export const getAggregatedSql = async ({
  resolvedSql,
  resolvedTableSchema,
  query,
  resolutionConfig,
  contextParams,
}: {
  resolvedSql: string;
  resolvedTableSchema: TableSchema;
  query: Query;
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
}): Promise<string> => {
  // Step 1: Use the resolved table schema from Phase 2 as source of truth
  // Update the SQL to point to the resolved SQL
  const aggregationBaseTableSchema: TableSchema = {
    ...resolvedTableSchema,
    sql: resolvedSql,
  };

  // Step 2: Identify which columns need ARRAY_AGG vs MAX
  const arrayColumns = getArrayTypeColumns(resolutionConfig);
  const baseTableName = aggregationBaseTableSchema.name;

  // Helper to check if a dimension is a resolved array column
  const isResolvedArrayColumn = (dimName: string) => {
    // Check if the dimension name matches a resolved column pattern
    // e.g., "tickets__owners__display_name"
    return arrayColumns.some((arrayCol) => {
      const arrayColPrefix = arrayCol.name.replace('.', '__');
      return (
        dimName.includes(`${arrayColPrefix}__`) &&
        !dimName.startsWith('__base_query__')
      );
    });
  };

  // Step 3: Create aggregation measures with proper aggregation functions
  // Get row_id dimension for GROUP BY
  const rowIdDimension = aggregationBaseTableSchema.dimensions.find(
    (d) => d.name === 'row_id' || d.name.endsWith('__row_id')
  );

  // Create measures with MAX or ARRAY_AGG based on column type
  const aggregationMeasures: TableSchema['measures'] = [];

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

  // Step 4: Create the aggregation query
  const aggregationQuery: Query = {
    measures: aggregationMeasures.map((m) => `${baseTableName}.${m.name}`),
    dimensions: rowIdDimension
      ? [`${baseTableName}.${rowIdDimension.name}`]
      : [],
  };

  // Step 5: Generate the final SQL
  const aggregatedSql = await cubeQueryToSQL({
    query: aggregationQuery,
    tableSchemas: [schemaWithAggregation],
    contextParams,
  });

  const rowIdExcludedSql = `select * exclude(__row_id) from (${aggregatedSql})`;
  return rowIdExcludedSql;
};
