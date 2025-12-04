import { constructAlias, getNamespacedKey } from '../../member-formatters';
import { TableSchema } from '../../types/cube-types/table';
import {
  BASE_DATA_SOURCE_NAME,
  RESOLUTION_SQL_OVERRIDE_FIELD_PLACEHOLDER,
  ResolutionConfig,
} from '../types';

/**
 * Applies SQL override configurations to a table schema.
 * This function overrides dimensions/measures with custom SQL expressions.
 *
 * This is done AFTER the base SQL is generated, so filters and sorts
 * that rely on original values are already compiled into the SQL.
 *
 * NOTE: The fieldName in sqlOverrideConfigs should already be transformed
 * using memberKeyToSafeKey before calling this function.
 *
 * The overrideSql can use a {{RESOLUTION_SQL_OVERRIDE_FIELD_PLACEHOLDER}} placeholder which will be replaced with
 * the properly formatted column reference using constructAlias().
 * Use the exported RESOLUTION_SQL_OVERRIDE_FIELD_PLACEHOLDER constant for type safety.
 *
 * @param baseSchema - The base table schema to apply overrides to
 * @param resolutionConfig - Resolution config containing SQL overrides
 * @returns A new TableSchema with SQL overrides applied
 *
 * @example
 * ```typescript
 * import { RESOLUTION_SQL_OVERRIDE_FIELD_PLACEHOLDER } from '@devrev/meerkat-core';
 *
 * // For scalar fields:
 * {
 *   fieldName: 'issues__priority',
 *   overrideSql: `CASE WHEN ${RESOLUTION_SQL_OVERRIDE_FIELD_PLACEHOLDER} = 1 THEN 'P0' END`,
 *   type: 'string'
 * }
 * // {{FIELD}} gets replaced using constructAlias() with proper quoting
 *
 * // For array fields:
 * {
 *   fieldName: 'issues__priority_tags',
 *   overrideSql: `list_transform(${RESOLUTION_SQL_OVERRIDE_FIELD_PLACEHOLDER}, x -> CASE WHEN x = 1 THEN 'P0' ... END)`,
 *   type: 'string_array'
 * }
 * ```
 */
export const applySqlOverrides = (
  baseSchema: TableSchema,
  resolutionConfig: ResolutionConfig
): TableSchema => {
  if (
    !resolutionConfig.sqlOverrideConfigs ||
    resolutionConfig.sqlOverrideConfigs.length === 0
  ) {
    return baseSchema;
  }

  // Validate that all SQL overrides contain the {{FIELD}} placeholder
  resolutionConfig.sqlOverrideConfigs.forEach((overrideConfig) => {
    if (
      !overrideConfig.overrideSql.includes(
        RESOLUTION_SQL_OVERRIDE_FIELD_PLACEHOLDER
      )
    ) {
      throw new Error(
        `SQL override for field '${overrideConfig.fieldName}' must contain ${RESOLUTION_SQL_OVERRIDE_FIELD_PLACEHOLDER} placeholder. ` +
          `This placeholder will be replaced with the proper column reference. ` +
          `Current SQL: ${overrideConfig.overrideSql}`
      );
    }
  });

  // Create a new schema with cloned dimensions and measures
  const updatedSchema: TableSchema = {
    ...baseSchema,
    dimensions: [...baseSchema.dimensions],
    measures: [...baseSchema.measures],
  };

  resolutionConfig.sqlOverrideConfigs.forEach((overrideConfig) => {
    // Check dimensions in base schema
    const dimensionIndex = updatedSchema.dimensions.findIndex(
      (dim) => dim.name === overrideConfig.fieldName
    );

    if (dimensionIndex !== -1) {
      const originalDimension = updatedSchema.dimensions[dimensionIndex];

      // Use constructAlias to get the properly formatted column reference from alias
      // Default aliasContext will add quotes when needed (for aliases with spaces, etc.)
      const columnReference = constructAlias({
        name: originalDimension.name,
        alias: originalDimension.alias,
        aliasContext: {},
      });

      // Replace {{FIELD}} placeholder with the column reference
      const finalSql = overrideConfig.overrideSql.replace(
        new RegExp(
          RESOLUTION_SQL_OVERRIDE_FIELD_PLACEHOLDER.replace(/[{}]/g, '\\$&'),
          'g'
        ),
        getNamespacedKey(BASE_DATA_SOURCE_NAME, columnReference)
      );

      updatedSchema.dimensions[dimensionIndex] = {
        ...originalDimension,
        sql: finalSql,
        type: overrideConfig.type,
      };
    }

    // Check measures in base schema
    const measureIndex = updatedSchema.measures.findIndex(
      (measure) => measure.name === overrideConfig.fieldName
    );

    if (measureIndex !== -1) {
      const originalMeasure = updatedSchema.measures[measureIndex];

      // Use constructAlias to get the properly formatted column reference from alias
      // Default aliasContext will add quotes when needed (for aliases with spaces, etc.)
      const columnReference = constructAlias({
        name: originalMeasure.name,
        alias: originalMeasure.alias,
        aliasContext: {},
      });

      // Replace {{FIELD}} placeholder with the column reference
      const finalSql = overrideConfig.overrideSql.replace(
        new RegExp(
          RESOLUTION_SQL_OVERRIDE_FIELD_PLACEHOLDER.replace(/[{}]/g, '\\$&'),
          'g'
        ),
        columnReference
      );

      updatedSchema.measures[measureIndex] = {
        ...originalMeasure,
        sql: finalSql,
        type: overrideConfig.type,
      };
    }
  });

  return updatedSchema;
};
