import {
  ContextParams,
  getArrayTypeResolutionColumnConfigs,
  getNamespacedKey,
  Measure,
  MEERKAT_OUTPUT_DELIMITER,
  Query,
  ResolutionConfig,
  ROW_ID_DIMENSION_NAME,
  TableSchema,
  wrapWithRowIdOrderingAndExclusion,
} from '../../index';

/**
 * Constructs the resolved column name prefix for array resolution.
 * This is used to identify which columns in the resolved schema correspond to array fields.
 *
 * @param columnName - The original column name
 * @returns The prefixed column name used in resolution
 */
const getResolvedArrayColumnPrefix = (columnName: string): string => {
  return `${columnName}${MEERKAT_OUTPUT_DELIMITER}`;
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
  resolvedTableSchema,
  resolutionConfig,
  contextParams,
  cubeQueryToSQL,
}: {
  resolvedTableSchema: TableSchema;
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
  cubeQueryToSQL: (params: {
    query: Query;
    tableSchemas: TableSchema[];
    contextParams?: ContextParams;
  }) => Promise<string>;
}): Promise<string> => {
  const aggregationBaseTableSchema: TableSchema = resolvedTableSchema;

  // Identify which columns need ARRAY_AGG vs MAX
  const arrayColumns = getArrayTypeResolutionColumnConfigs(resolutionConfig);
  const baseTableName = aggregationBaseTableSchema.name;

  const isResolvedArrayColumn = (dimName: string) => {
    return arrayColumns.some((arrayCol) => {
      return dimName.includes(getResolvedArrayColumnPrefix(arrayCol.name));
    });
  };

  // Create aggregation measures with proper aggregation functions
  // Get row_id dimension for GROUP BY
  const rowIdDimension = aggregationBaseTableSchema.dimensions.find(
    (d) => d.name === ROW_ID_DIMENSION_NAME
  );

  if (!rowIdDimension) {
    throw new Error('Row id dimension not found');
  }
  // Create measures with MAX or ARRAY_AGG based on column type
  const aggregationMeasures: Measure[] = [];

  aggregationBaseTableSchema.dimensions
    .filter((dim) => dim.name !== rowIdDimension?.name)
    .forEach((dim) => {
      const isArrayColumn = isResolvedArrayColumn(dim.name);

      // The dimension's sql field already has the correct reference (e.g., __resolved_query."__row_id")
      // We just need to wrap it in the aggregation function
      const columnRef = dim.sql;

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
    dimensions: [rowIdDimension],
  };

  // Generate the final SQL
  const aggregatedSql = await cubeQueryToSQL({
    query: {
      measures: aggregationMeasures.map((m) =>
        getNamespacedKey(baseTableName, m.name)
      ),
      dimensions: [getNamespacedKey(baseTableName, rowIdDimension.name)],
    },
    tableSchemas: [schemaWithAggregation],
    contextParams,
  });

  // Order by row_id to maintain consistent ordering before excluding it
  return wrapWithRowIdOrderingAndExclusion(
    aggregatedSql,
    ROW_ID_DIMENSION_NAME
  );
};
