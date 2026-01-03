import { constructCompoundAlias } from '../../member-formatters/get-alias';
import { getNamespacedKey } from '../../member-formatters/get-namespaced-key';
import { memberKeyToSafeKey } from '../../member-formatters/member-key-to-safe-key';
import { Query } from '../../types/cube-types/query';
import { ContextParams, TableSchema } from '../../types/cube-types/table';
import { ResolutionConfig } from '../types';

export interface ApplyAliasesParams {
  aggregatedTableSchema: TableSchema;
  originalTableSchemas: TableSchema[];
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
  cubeQueryToSQL: (params: {
    query: Query;
    tableSchemas: TableSchema[];
    contextParams?: ContextParams;
  }) => Promise<string>;
}

/**
 * Restores aliases from the original table schemas to the aggregated schema
 * and generates the final SQL with proper column names.
 *
 * This step:
 * 1. Creates an alias map from the original table schemas
 * 2. Handles resolution column configs to determine proper aliases
 *    - For single resolution columns: uses the original alias
 *    - For multiple resolution columns: creates compound aliases (e.g., "Owners - Display Name")
 * 3. Creates a new schema with restored aliases
 * 4. Generates final SQL with proper aliases
 *
 * @param aggregatedTableSchema - The aggregated table schema from the aggregation step
 * @param originalTableSchemas - The original table schemas with aliases
 * @param resolutionConfig - Resolution configuration
 * @param contextParams - Optional context parameters
 * @param cubeQueryToSQL - Function to generate SQL from query and table schemas
 * @returns Final SQL string with aliases
 */
export const applyAliases = async ({
  aggregatedTableSchema,
  originalTableSchemas,
  resolutionConfig,
  contextParams,
  cubeQueryToSQL,
}: ApplyAliasesParams): Promise<string> => {
  // Restore aliases from original tableSchemas to get nice column names in final output
  // Create a map of schemaName__fieldName -> alias from original schemas
  const aliasMap = new Map<string, string>();

  const columnConfigMap = new Map(
    resolutionConfig.columnConfigs?.map((colConfig) => [
      colConfig.name,
      colConfig,
    ]) || []
  );

  const tableSchemaMap = new Map(
    resolutionConfig.tableSchemas.map((schema) => [schema.name, schema])
  );

  const safeKeyOptions = { useDotNotation: false };

  // Helper function to process dimensions or measures and populate the alias map
  const processMembers = (
    members: Array<{ name: string; alias?: string }>,
    schemaName: string
  ) => {
    members.forEach((member) => {
      if (!member.alias) return;

      const columnName = memberKeyToSafeKey(
        `${schemaName}.${member.name}`,
        safeKeyOptions
      );
      const columnConfig = columnConfigMap.get(columnName);

      // No resolution config - use original alias
      if (!columnConfig) {
        aliasMap.set(columnName, member.alias);
        return;
      }

      const joinedTableName = columnName;

      // Single resolution column - use original alias
      if (columnConfig.resolutionColumns.length === 1) {
        aliasMap.set(
          memberKeyToSafeKey(
            getNamespacedKey(
              joinedTableName,
              columnConfig.resolutionColumns[0]
            ),
            safeKeyOptions
          ),
          member.alias
        );
        return;
      }

      // Multiple resolution columns - create compound aliases
      const sourceTableSchema = tableSchemaMap.get(columnConfig.source);

      if (!sourceTableSchema) {
        throw new Error(
          `Source table schema not found for ${columnConfig.source}`
        );
      }

      for (const resolutionColumn of columnConfig.resolutionColumns) {
        const sourceFieldAlias = sourceTableSchema.dimensions.find(
          (dimension) => dimension.name === resolutionColumn
        )?.alias;

        if (!sourceFieldAlias) {
          throw new Error(
            `Source field alias not found for ${resolutionColumn}`
          );
        }

        aliasMap.set(
          memberKeyToSafeKey(
            getNamespacedKey(joinedTableName, resolutionColumn),
            safeKeyOptions
          ),
          constructCompoundAlias(member.alias, sourceFieldAlias)
        );
      }
    });
  };

  originalTableSchemas.forEach((schema) => {
    processMembers(schema.dimensions, schema.name);
    processMembers(schema.measures, schema.name);
  });

  // Create a new schema with restored aliases
  const schemaWithAliases: TableSchema = {
    ...aggregatedTableSchema,
    dimensions: [
      ...aggregatedTableSchema.dimensions.map((dim) => ({
        ...dim,
        alias: aliasMap.get(dim.alias ?? '') || dim.alias,
      })),
      ...aggregatedTableSchema.measures.map((measure) => ({
        ...measure,
        alias: aliasMap.get(measure.alias ?? '') || measure.alias,
      })),
    ],
    measures: [],
  };

  // Generate final SQL with aliases
  const sqlWithAliases = await cubeQueryToSQL({
    query: {
      dimensions: [
        ...schemaWithAliases.dimensions.map((d) =>
          getNamespacedKey(schemaWithAliases.name, d.name)
        ),
      ],
      measures: [],
    },
    tableSchemas: [schemaWithAliases],
    contextParams,
  });

  return sqlWithAliases;
};
