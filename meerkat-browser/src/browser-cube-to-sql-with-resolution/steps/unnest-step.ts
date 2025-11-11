import {
  ContextParams,
  createWrapperTableSchema,
  getNamespacedKey,
  Query,
  ResolutionConfig,
  TableSchema,
  withArrayFlattenModifier,
} from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { cubeQueryToSQL } from '../../browser-cube-to-sql/browser-cube-to-sql';

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
  const updatedBaseTableSchema = withArrayFlattenModifier(
    baseTableSchema,
    resolutionConfig
  );

  const unnestedSql = await cubeQueryToSQL({
    connection,
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

