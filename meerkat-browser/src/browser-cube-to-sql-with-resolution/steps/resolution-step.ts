import {
  ContextParams,
  createWrapperTableSchema,
  generateResolutionJoinPaths,
  generateResolutionSchemas,
  generateResolvedDimensions,
  getNamespacedKey,
  memberKeyToSafeKey,
  Query,
  ResolutionConfig,
  TableSchema,
} from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { cubeQueryToSQL } from '../../browser-cube-to-sql/browser-cube-to-sql';

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

