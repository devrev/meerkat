import { memberKeyToSafeKey } from '../../member-formatters';
import { TableSchema } from '../../types/cube-types/table';
import { ResolutionConfig } from '../types';

/**
 * Applies SQL override configurations to a table schema.
 * This function overrides dimensions/measures with custom SQL expressions.
 *
 * This is done AFTER the base SQL is generated, so filters and sorts
 * that rely on original values are already compiled into the SQL.
 *
 * NOTE: The fieldName in sqlOverrideConfigs should be in natural format (e.g., 'issues.priority').
 * This function will apply memberKeyToSafeKey internally.
 *
 * The overrideSql should reference fields in datasource.fieldname format (same as fieldName),
 * which will be automatically converted to the safe format (datasource__fieldname).
 *
 * @param baseSchema - The base table schema to apply overrides to
 * @param resolutionConfig - Resolution config containing SQL overrides
 * @returns A new TableSchema with SQL overrides applied
 *
 * @example
 * ```typescript
 * // For scalar fields:
 * {
 *   fieldName: 'issues.priority',
 *   overrideSql: `CASE WHEN issues.priority = 1 THEN 'P0' WHEN issues.priority = 2 THEN 'P1' END`,
 *   type: 'string'
 * }
 * // issues.priority gets automatically replaced with issues__priority
 *
 * // For array fields:
 * {
 *   fieldName: 'issues.priority_tags',
 *   overrideSql: `list_transform(issues.priority_tags, x -> CASE WHEN x = 1 THEN 'P0' ... END)`,
 *   type: 'string_array'
 * }
 * // issues.priority_tags gets automatically replaced with issues__priority_tags
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

  // Validate that all SQL overrides reference the field being overridden
  resolutionConfig.sqlOverrideConfigs.forEach((overrideConfig) => {
    // fieldName is expected to be in natural format (e.g., 'issues.priority')
    if (!overrideConfig.overrideSql.includes(overrideConfig.fieldName)) {
      throw new Error(
        `SQL override for field '${overrideConfig.fieldName}' must reference the field in the SQL. ` +
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
    // Convert natural field name to safe key for matching
    // e.g., 'issues.priority' -> 'issues__priority'
    const safeFieldName = memberKeyToSafeKey(overrideConfig.fieldName);

    // Check dimensions in base schema
    const dimensionIndex = updatedSchema.dimensions.findIndex(
      (dim) => dim.name === safeFieldName
    );

    if (dimensionIndex !== -1) {
      const originalDimension = updatedSchema.dimensions[dimensionIndex];

      // Replace datasource.fieldName with datasource__fieldName in the SQL
      // e.g., "issues.priority" -> "issues__priority"
      const fieldNamePattern = overrideConfig.fieldName.replace(/\./g, '\\.'); // Escape dots for regex

      const finalSql = overrideConfig.overrideSql.replace(
        new RegExp(`\\b${fieldNamePattern}\\b`, 'g'), // Word boundary to match exact field names
        safeFieldName
      );

      updatedSchema.dimensions[dimensionIndex] = {
        ...originalDimension,
        sql: finalSql,
        type: overrideConfig.type,
      };
    }

    // Check measures in base schema
    const measureIndex = updatedSchema.measures.findIndex(
      (measure) => measure.name === safeFieldName
    );

    if (measureIndex !== -1) {
      const originalMeasure = updatedSchema.measures[measureIndex];

      const fieldNamePattern = overrideConfig.fieldName.replace(/\./g, '\\.');

      const finalSql = overrideConfig.overrideSql.replace(
        new RegExp(`\\b${fieldNamePattern}\\b`, 'g'),
        safeFieldName
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
