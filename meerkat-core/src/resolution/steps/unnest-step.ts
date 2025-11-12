import {
  ContextParams,
  createWrapperTableSchema,
  getNamespacedKey,
  Query,
  ResolutionConfig,
  TableSchema,
  withArrayFlattenModifier,
} from '../../index';

/**
 * Apply unnesting
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
  cubeQueryToSQL,
}: {
  baseTableSchema: TableSchema;
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
  cubeQueryToSQL: (params: {
    query: Query;
    tableSchemas: TableSchema[];
    contextParams?: ContextParams;
  }) => Promise<string>;
}): Promise<TableSchema> => {
  const updatedBaseTableSchema = withArrayFlattenModifier(
    baseTableSchema,
    resolutionConfig
  );

  const unnestedSql = await cubeQueryToSQL({
    query: {
      measures: [],
      dimensions: updatedBaseTableSchema.dimensions.map((d) =>
        getNamespacedKey(updatedBaseTableSchema.name, d.name)
      ),
    },
    tableSchemas: [updatedBaseTableSchema],
    contextParams,
  });

  const unnestedBaseTableSchema: TableSchema = createWrapperTableSchema(
    unnestedSql,
    baseTableSchema
  );

  return unnestedBaseTableSchema;
};
