import {
  ContextParams,
  createWrapperTableSchema,
  getNamespacedKey,
  ResolutionConfig,
  TableSchema,
  withArrayFlattenModifier,
} from '@devrev/meerkat-core';
import { cubeQueryToSQL } from '../../cube-to-sql/cube-to-sql';

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
}: {
  baseTableSchema: TableSchema;
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
}): Promise<TableSchema> => {
  const updatedBaseTableSchema = withArrayFlattenModifier(
    baseTableSchema,
    resolutionConfig
  );

  const unnestedSql = await cubeQueryToSQL({
    query: {
      measures: [],
      dimensions: [
        ...updatedBaseTableSchema.dimensions.map((d) =>
          getNamespacedKey(updatedBaseTableSchema.name, d.name)
        ),
      ],
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
