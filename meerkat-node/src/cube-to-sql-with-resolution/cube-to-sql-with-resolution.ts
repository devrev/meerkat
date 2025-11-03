import {
  BASE_DATA_SOURCE_NAME,
  ContextParams,
  createBaseTableSchema,
  Dimension,
  generateResolutionJoinPaths,
  generateResolutionJoinPathsFromBaseTable,
  generateResolutionSchemas,
  generateResolutionSchemasFromBaseTable,
  generateResolvedDimensions,
  getNamespacedKey,
  memberKeyToSafeKey,
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

  // Step 2: Check if array-type resolution is needed
  if (resolutionConfig.columnConfigs.some((config) => config.isArrayType)) {
    debugger;
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
    tableSchemas
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
        tableSchemas
      ),
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
  // Phase 1: Generate SQL with row_id and unnested arrays
  const { sql: unnestBaseSql, baseTableSchema } = await getUnnestBaseSql({
    baseSql,
    measures,
    dimensions,
    tableSchemas,
    resolutionConfig,
    contextParams,
  });

  columnProjections?.push(`${tableSchemas[0].name}.row_id`);
  // Phase 2: Apply resolution (join with lookup tables)
  const { sql: resolvedSql, resolvedTableSchema } = await getResolvedSql({
    unnestedSql: unnestBaseSql,
    baseTableSchema,
    tableSchemas,
    resolutionConfig,
    contextParams,
    columnProjections,
  });

  // Phase 3: Re-aggregate to reverse the unnest
  const aggregatedSql = await getAggregatedSql({
    resolvedSql,
    resolvedTableSchema,
    resolutionConfig,
    contextParams,
  });

  return aggregatedSql;
};

/**
 * Phase 1: Add row_id and apply unnesting
 *
 * This function performs 3 steps:
 * 1. Wrap base SQL and add row_id dimension
 * 2. Create schema with unnest modifiers for array columns
 * 3. Generate final unnested SQL
 *
 * @param baseSql - Base SQL generated from the original query (no modifications)
 * @param measures - Original measures from the query
 * @param dimensions - Original dimensions from the query
 * @returns SQL with row_id and unnested arrays, the schema, and list of projected columns
 */
export const getUnnestBaseSql = async ({
  baseSql,
  measures,
  dimensions,
  tableSchemas,
  resolutionConfig,
  contextParams,
}: {
  baseSql: string;
  measures: string[];
  dimensions: string[];
  tableSchemas: TableSchema[];
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
}): Promise<{
  sql: string;
  baseTableSchema: TableSchema;
}> => {
  // Step 1: Create schema for the base SQL
  const baseTableSchema: TableSchema = createBaseTableSchema(
    baseSql,
    tableSchemas,
    resolutionConfig,
    measures,
    dimensions
  );

  const arrayColumns = getArrayTypeColumns(resolutionConfig);
  for (const dimension of baseTableSchema.dimensions) {
    const arrayResolvedFields = arrayColumns.map((ac) =>
      memberKeyToSafeKey(ac.name)
    );
    if (
      arrayResolvedFields.some(
        (resolvedField) => resolvedField === dimension.name
      )
    ) {
      dimension.modifier = { shouldFlattenArray: true };
    }
  }
  debugger;

  // Step 2: Add row_id dimension to schema and generate SQL with row_id
  const schemaWithRowId: TableSchema = {
    ...baseTableSchema,
    dimensions: [
      ...baseTableSchema.dimensions,
      {
        name: memberKeyToSafeKey(`${tableSchemas[0].name}.row_id`),
        sql: 'row_number() OVER ()',
        type: 'number',
        alias: '__row_id',
      } as Dimension,
    ],
    joins: baseTableSchema.joins,
  };

  // Query that projects all original columns plus row_id
  const queryWithRowId: Query = {
    measures: [],
    dimensions: [
      ...schemaWithRowId.dimensions.map((d) =>
        getNamespacedKey(schemaWithRowId.name, d.name)
      ),
    ],
  };

  const unnestedSql = await cubeQueryToSQL({
    query: queryWithRowId,
    tableSchemas: [schemaWithRowId],
    contextParams,
  });

  // Build list of projected columns (in original format, not namespaced)
  // This is what was actually projected in the unnested SQL
  // const projectedColumns = [...dimensions, 'row_id', ...measures];

  const unnestedBaseTableSchema: TableSchema = {
    name: '__base_query',
    sql: unnestedSql,
    dimensions: schemaWithRowId.dimensions.map((d) => ({
      name: d.name,
      sql: `__base_query."${d.alias || d.name}"`,
      type: d.type,
      alias: d.alias,
    })),
    measures: [],
    joins: schemaWithRowId.joins,
  };
  for (const join of unnestedBaseTableSchema.joins || []) {
    const leftJoin = join.sql.split('=')[0];
    const namespace = leftJoin.split('.')[0];
    const name = leftJoin.split('.')[1];
    const toFind = `${namespace}.${name}`.trim();
    const dimensionToJoinOn = schemaWithRowId.dimensions.filter(
      (d) => d.sql.trim() === toFind
    );
    if (dimensionToJoinOn.length === 0) {
      throw new Error(`Dimension not found: ${namespace}.${name}`);
    }
    if (dimensionToJoinOn.length > 1) {
      throw new Error(`Multiple dimensions found: ${namespace}.${name}`);
    }
    const rightJoin = join.sql.split('=')[1];
    const rightJoinNamespace = rightJoin.split('.')[0].trim();
    const rightJoinField = rightJoin.split('.')[1].trim();
    // join.sql = join.sql.replace(
    //   leftJoin,
    //   `${unnestedBaseTableSchema.name}.${
    //     dimensionToJoinOn[0].alias || dimensionToJoinOn[0].name
    //   }`
    // );
    // // TODO: Confirm if name also needs "" like this.
    // join.sql = `${unnestedBaseTableSchema.name}.${
    //   dimensionToJoinOn[0].alias
    //     ? `"${dimensionToJoinOn[0].alias}"`
    //     : dimensionToJoinOn[0].name
    // }=${memberKeyToSafeKey(
    //   getNamespacedKey(unnestedBaseTableSchema.name, rightJoinNamespace)
    // )}.${rightJoinField}`;
  }
  resolutionConfig.columnConfigs.forEach((config) => {
    config.name = memberKeyToSafeKey(config.name);
  });
  return {
    sql: unnestedSql,
    baseTableSchema: unnestedBaseTableSchema,
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
  tableSchemas,
  resolutionConfig,
  contextParams,
  columnProjections,
}: {
  unnestedSql: string;
  baseTableSchema: TableSchema;
  tableSchemas: TableSchema[];
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
  columnProjections?: string[];
}): Promise<{
  sql: string;
  resolvedTableSchema: TableSchema;
}> => {
  // Update the SQL to point to the unnested SQL
  const updatedBaseTableSchema: TableSchema = baseTableSchema;

  // for (const columnConfig of resolutionConfig.columnConfigs) {
  //   columnConfig.name = getNamespacedKey(
  //     updatedBaseTableSchema.name,
  //     memberKeyToSafeKey(columnConfig.name)
  //   );
  //   // columnConfig.name = memberKeyToSafeKey(columnConfig.name);
  // }
  // Generate resolution schemas for array fields
  const resolutionSchemas = generateResolutionSchemasFromBaseTable(
    resolutionConfig,
    updatedBaseTableSchema
  );

  // Generate join paths using existing helper
  const joinPaths = generateResolutionJoinPathsFromBaseTable(
    updatedBaseTableSchema.name,
    resolutionConfig,
    updatedBaseTableSchema
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
 * @param resolvedTableSchema - Schema from Phase 2 (contains all column info)
 * @param resolutionConfig - Resolution configuration
 * @param contextParams - Optional context parameters
 * @returns Final SQL with arrays containing resolved values
 */
export const getAggregatedSql = async ({
  resolvedSql,
  resolvedTableSchema,
  resolutionConfig,
  contextParams,
}: {
  resolvedSql: string;
  resolvedTableSchema: TableSchema;
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
      const arrayColPrefix = memberKeyToSafeKey(arrayCol.name);
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
    measures: aggregationMeasures.map((m) =>
      getNamespacedKey(baseTableName, m.name)
    ),
    dimensions: rowIdDimension
      ? [getNamespacedKey(baseTableName, rowIdDimension.name)]
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
