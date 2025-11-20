import {
  ContextParams,
  createWrapperTableSchema,
  generateResolutionJoinPaths,
  generateResolutionSchemas,
  generateResolvedDimensions,
  getColumnReference,
  getNamespacedKey,
  memberKeyToSafeKey,
  Query,
  ResolutionConfig,
  TableSchema,
} from '../../index';

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
  baseTableSchema,
  resolutionConfig,
  columnProjections,
  contextParams,
  cubeQueryToSQL,
}: {
  baseTableSchema: TableSchema;
  resolutionConfig: ResolutionConfig;
  columnProjections: string[];
  contextParams?: ContextParams;
  cubeQueryToSQL: (params: {
    query: Query;
    tableSchemas: TableSchema[];
    contextParams?: ContextParams;
  }) => Promise<string>;
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

  // Create a map of resolution schemas by config name for efficient lookup
  const resolutionSchemaByConfigName = new Map<
    string,
    (typeof resolutionSchemas)[0]
  >();
  resolutionSchemas.forEach((resSchema) => {
    resolutionSchemaByConfigName.set(resSchema.name, resSchema);
  });

  // Build the dimension map using the pre-indexed schemas
  resolutionConfig.columnConfigs.forEach((config) => {
    const resSchema = resolutionSchemaByConfigName.get(config.name);
    if (resSchema) {
      resolutionDimensionsByColumnName.set(
        config.name,
        resSchema.dimensions.map((dim) => ({
          name: dim.name,
          sql: getColumnReference(resolvedTableSchema.name, dim),
          type: dim.type,
          alias: dim.alias,
        }))
      );
    }
  });

  // Maintain the same order as columnProjections
  // Replace dimensions that need resolution with their resolved counterparts
  resolvedTableSchema.dimensions = (updatedColumnProjections || []).flatMap(
    (projectionName) => {
      // Check if this column has resolved dimensions
      const resolvedDims = resolutionDimensionsByColumnName.get(projectionName);
      if (resolvedDims) {
        // Use resolved dimensions
        return resolvedDims;
      }

      // Otherwise, find the original dimension from baseTableSchema
      const originalDim = baseTableSchema.dimensions.find(
        (d) => d.name === projectionName
      );
      if (originalDim) {
        return [originalDim];
      }

      // If not found, throw an error
      throw new Error(
        `Column projection '${projectionName}' not found in base table schema dimensions`
      );
    }
  );

  return resolvedTableSchema;
};
