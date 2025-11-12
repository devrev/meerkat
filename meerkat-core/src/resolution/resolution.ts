import {
  constructAlias,
  getNamespacedKey,
  memberKeyToSafeKey,
} from '../member-formatters';
import { Member, Query } from '../types/cube-types/query';
import { Dimension, Measure, TableSchema } from '../types/cube-types/table';
import { isArrayTypeMember } from '../utils/is-array-member-type';
import {
  BASE_DATA_SOURCE_NAME,
  ResolutionColumnConfig,
  ResolutionConfig,
} from './types';

/**
 * Constructs a SQL column reference from a table name and a dimension/measure.
 *
 * @param tableName - The name of the table
 * @param member - The dimension or measure object with name and optional alias
 * @returns Formatted SQL column reference like: tableName."columnName"
 */
export const getColumnReference = (
  tableName: string,
  member: { name: string; alias?: string }
): string => {
  return `${tableName}."${member.alias || member.name}"`;
};

/**
 * Checks if resolution should be skipped based on the resolution configuration and column projections.
 * Resolution is skipped when there are no columns to resolve and no column projections.
 *
 * @param resolutionConfig - The resolution configuration
 * @param columnProjections - Optional array of column projections
 * @returns true if resolution should be skipped, false otherwise
 */
export const shouldSkipResolution = (
  resolutionConfig: ResolutionConfig,
  query: Query,
  columnProjections?: string[]
): boolean => {
  // If no resolution required and no column projections to ensure order in which export is happening
  // and explicit order is not provided, then skip resolution.
  return (
    resolutionConfig.columnConfigs.length === 0 &&
    columnProjections?.length === 0 &&
    !query.order
  );
};

const constructBaseDimension = (name: string, schema: Measure | Dimension) => {
  return {
    name: memberKeyToSafeKey(name),
    sql: `${BASE_DATA_SOURCE_NAME}.${constructAlias({
      name,
      alias: schema.alias,
      aliasContext: { isAstIdentifier: false },
    })}`,
    type: schema.type,
    // Constructs alias to match the name in the base query.
    alias: constructAlias({
      name,
      alias: schema.alias,
      aliasContext: { isTableSchemaAlias: true },
    }),
  };
};

export const createBaseTableSchema = (
  baseSql: string,
  tableSchemas: TableSchema[],
  resolutionConfig: ResolutionConfig,
  measures: Member[],
  dimensions?: Member[]
) => {
  const schemaByName: Record<string, Measure | Dimension> = {};
  tableSchemas.forEach((tableSchema) => {
    tableSchema.dimensions.forEach((dimension) => {
      schemaByName[getNamespacedKey(tableSchema.name, dimension.name)] =
        dimension;
    });
    tableSchema.measures.forEach((measure) => {
      schemaByName[getNamespacedKey(tableSchema.name, measure.name)] = measure;
    });
  });

  return {
    name: BASE_DATA_SOURCE_NAME,
    sql: baseSql,
    measures: [],
    dimensions: [...measures, ...(dimensions || [])].map((member) => {
      const schema = schemaByName[member];
      if (schema) {
        return constructBaseDimension(member, schema);
      } else {
        throw new Error(`Not found: ${member}`);
      }
    }),
    joins: resolutionConfig.columnConfigs.map((config) => ({
      sql: `${BASE_DATA_SOURCE_NAME}.${constructAlias({
        name: config.name,
        alias: schemaByName[config.name]?.alias,
        aliasContext: { isAstIdentifier: false },
      })} = ${memberKeyToSafeKey(config.name)}.${config.joinColumn}`,
    })),
  };
};

export const createWrapperTableSchema = (
  sql: string,
  baseTableSchema: TableSchema
) => {
  return {
    name: BASE_DATA_SOURCE_NAME,
    sql: sql,
    dimensions: baseTableSchema.dimensions.map((d) => ({
      name: d.name,
      sql: getColumnReference(BASE_DATA_SOURCE_NAME, d),
      type: d.type,
      alias: d.alias,
    })),
    measures: baseTableSchema.measures.map((m) => ({
      name: m.name,
      sql: getColumnReference(BASE_DATA_SOURCE_NAME, m),
      type: m.type,
      alias: m.alias,
    })),
    joins: baseTableSchema.joins,
  };
};

export const withArrayFlattenModifier = (
  baseTableSchema: TableSchema,
  resolutionConfig: ResolutionConfig
): TableSchema => {
  const arrayColumns = getArrayTypeResolutionColumnConfigs(resolutionConfig);

  return {
    ...baseTableSchema,
    dimensions: baseTableSchema.dimensions.map((dimension) => {
      const shouldFlatten = arrayColumns.some(
        (ac: ResolutionColumnConfig) => ac.name === dimension.name
      );

      if (shouldFlatten) {
        return {
          ...dimension,
          modifier: { shouldFlattenArray: true },
        };
      }

      return dimension;
    }),
  };
};

export const getArrayTypeResolutionColumnConfigs = (
  resolutionConfig: ResolutionConfig
) => {
  return resolutionConfig.columnConfigs.filter((config) =>
    isArrayTypeMember(config.type)
  );
};

/**
 * Wraps SQL to order by row_id and then exclude it from results.
 * This maintains the ordering from the base query while removing the internal row_id column.
 *
 * @param sql - The SQL query that includes a __row_id column
 * @param rowIdColumnName - The name of the row_id column (defaults to '__row_id')
 * @returns SQL query ordered by row_id with the row_id column excluded
 */
export const wrapWithRowIdOrderingAndExclusion = (
  sql: string,
  rowIdColumnName: string
): string => {
  return `select * exclude(${rowIdColumnName}) from (${sql}) order by ${rowIdColumnName}`;
};
