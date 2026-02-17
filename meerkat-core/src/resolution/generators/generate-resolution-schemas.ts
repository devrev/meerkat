import { getNamespacedKey, memberKeyToSafeKey } from '../../member-formatters';
import { TableSchema } from '../../types/cube-types/table';
import { findInDimensionSchemas } from '../../utils/find-in-table-schema';
import { ResolutionConfig } from '../types';

export const generateResolutionSchemas = (config: ResolutionConfig) => {
  const resolutionSchemas: TableSchema[] = [];
  config.columnConfigs.forEach((colConfig) => {
    const tableSchema = config.tableSchemas.find(
      (ts) => ts.name === colConfig.source
    );
    if (!tableSchema) {
      throw new Error(`Table schema not found for ${colConfig.source}`);
    }

    const baseName = memberKeyToSafeKey(colConfig.name);

    // For each column that needs to be resolved, create a copy of the relevant table schema.
    // We use the name of the column in the base query as the table schema name
    // to avoid conflicts.
    const resolutionSchema: TableSchema = {
      name: baseName,
      sql: tableSchema.sql,
      measures: [],
      dimensions: colConfig.resolutionColumns.map((col) => {
        const dimension = findInDimensionSchemas(
          getNamespacedKey(colConfig.source, col),
          config.tableSchemas
        );
        if (!dimension) {
          throw new Error(`Dimension not found: ${col}`);
        }
        return {
          // Need to create a new name due to limitations with how
          // CubeToSql handles duplicate dimension names between different sources.
          name: memberKeyToSafeKey(getNamespacedKey(colConfig.name, col)),
          sql: `${baseName}.${col}`,
          type: dimension.type,
          alias: memberKeyToSafeKey(getNamespacedKey(colConfig.name, col)),
        };
      }),
    };

    resolutionSchemas.push(resolutionSchema);
  });

  return resolutionSchemas;
};
