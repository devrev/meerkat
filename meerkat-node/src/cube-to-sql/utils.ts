import {
  Member,
  Query,
  splitIntoDataSourceAndFields,
  TableSchema,
} from '@devrev/meerkat-core';

import { BASE_DATA_SOURCE_NAME, ResolutionConfig } from './types';

export const resolveDimension = (dim: string, tableSchemas: TableSchema[]) => {
  const [tableName, columnName] = splitIntoDataSourceAndFields(dim);
  const tableSchema = tableSchemas.find((ts) => ts.name === tableName);
  if (!tableSchema) {
    throw new Error(`Table schema not found for ${tableName}`);
  }
  const dimension = tableSchema.dimensions.find((d) => d.name === columnName);
  if (!dimension) {
    throw new Error(`Dimension not found: ${dim}`);
  }

  return {
    name: `${generateName(dim)}`,
    sql: `${BASE_DATA_SOURCE_NAME}.${generateName(dim)}`,
    type: dimension.type,
  };
};

export const resolveMeasure = (
  measure: string,
  tableSchemas: TableSchema[]
) => {
  const [tableName, columnName] = splitIntoDataSourceAndFields(measure);
  const tableSchema = tableSchemas.find((ts) => ts.name === tableName);
  if (!tableSchema) {
    throw new Error(`Table schema not found for ${tableName}`);
  }
  const measureSchema = tableSchema.measures.find((m) => m.name === columnName);
  if (!measureSchema) {
    throw new Error(`Measure not found: ${measure}`);
  }

  return {
    name: `${generateName(measure)}`,
    sql: `${BASE_DATA_SOURCE_NAME}.${generateName(measure)}`,
    type: measureSchema.type,
  };
};

export const generateResolutionSchemas = (config: ResolutionConfig) => {
  const resolutionSchemas: TableSchema[] = [];
  config.columnConfigs.forEach((colConfig) => {
    const { source, resolutionColumns } = colConfig;
    const tableSchema = config.tableSchemas.find((ts) => ts.name === source);
    if (!tableSchema) {
      throw new Error(`Table schema not found for ${source}`);
    }

    const baseName = generateName(colConfig.name);

    // For each column that needs to be resolved, create a copy of the relevant table schema.
    // We use the name of the column in the base query as the table schema name
    // to avoid conflicts.
    const resolutionSchema: TableSchema = {
      name: baseName,
      sql: tableSchema.sql,
      measures: [],
      dimensions: resolutionColumns.map((col) => {
        const dimension = tableSchema.dimensions.find((d) => d.name === col);
        if (!dimension) {
          throw new Error(`Dimension not found: ${col}`);
        }
        return {
          name: col,
          sql: `${baseName}.${col}`,
          type: dimension.type,
        };
      }),
    };

    resolutionSchemas.push(resolutionSchema);
  });

  return resolutionSchemas;
};

export const generateResolvedDimensions = (
  query: Query,
  config: ResolutionConfig
): Member[] => {
  const resolvedDimensions: Member[] = [
    ...query.measures,
    ...(query.dimensions || []),
  ].flatMap((dimension) => {
    const resolution = config.columnConfigs.find((c) => c.name === dimension);

    if (!resolution) {
      return [`${BASE_DATA_SOURCE_NAME}.${generateName(dimension)}`];
    } else {
      return resolution.resolutionColumns.map(
        (col) => `${generateName(dimension)}.${col}`
      );
    }
  });
  return resolvedDimensions;
};

// Generates a valid column name from a generic reference
// by replacing '.' with '__'.
export const generateName = (columnName: string) =>
  columnName.replace('.', '__');
