import { constructAlias } from '../../member-formatters';
import { TableSchema } from '../../types/cube-types/table';
import { ResolutionConfig } from '../types';

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
 * The overrideSql can use a {{FIELD}} placeholder which will be replaced with
 * the properly formatted column reference using constructAlias().
 *
 * @param baseSchema - The base table schema to apply overrides to
 * @param resolutionConfig - Resolution config containing SQL overrides
 * @returns A new TableSchema with SQL overrides applied
 *
 * @example
 * ```typescript
 * // For scalar fields:
 * {
 *   fieldName: 'issues__priority',
 *   overrideSql: `CASE WHEN {{FIELD}} = 1 THEN 'P0' WHEN {{FIELD}} = 2 THEN 'P1' END`,
 *   type: 'string'
 * }
 * // {{FIELD}} gets replaced using constructAlias() with proper quoting
 *
 * // For array fields:
 * {
 *   fieldName: 'issues__priority_tags',
 *   overrideSql: `list_transform({{FIELD}}, x -> CASE WHEN x = 1 THEN 'P0' ... END)`,
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
    if (!overrideConfig.overrideSql.includes('{{FIELD}}')) {
      throw new Error(
        `SQL override for field '${overrideConfig.fieldName}' must contain {{FIELD}} placeholder. ` +
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
        /\{\{FIELD\}\}/g,
        columnReference
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
        /\{\{FIELD\}\}/g,
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
