import { memberKeyToSafeKey } from '../member-formatters';
import { splitIntoDataSourceAndFields } from '../member-formatters/split-into-data-source-and-fields';
import { JoinPath, Member, Query } from '../types/cube-types/query';
import { TableSchema } from '../types/cube-types/table';
import { BASE_DATA_SOURCE_NAME, ResolutionConfig } from './types';

const resolveDimension = (dim: string, tableSchemas: TableSchema[]) => {
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
    name: `${memberKeyToSafeKey(dim)}`,
    sql: `${BASE_DATA_SOURCE_NAME}.${memberKeyToSafeKey(dim)}`,
    type: dimension.type,
  };
};

const resolveMeasure = (measure: string, tableSchemas: TableSchema[]) => {
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
    name: `${memberKeyToSafeKey(measure)}`,
    sql: `${BASE_DATA_SOURCE_NAME}.${memberKeyToSafeKey(measure)}`,
    type: measureSchema.type,
  };
};

export const createBaseTableSchema = (
  baseSql: string,
  tableSchemas: TableSchema[],
  resolutionConfig: ResolutionConfig,
  measures: Member[],
  dimensions?: Member[]
) => ({
  name: BASE_DATA_SOURCE_NAME,
  sql: baseSql,
  measures: [],
  dimensions: [
    ...(dimensions || []).map((dim) => resolveDimension(dim, tableSchemas)),
    ...(measures || []).map((meas) => resolveMeasure(meas, tableSchemas)),
  ],
  joins: resolutionConfig.columnConfigs.map((config) => ({
    sql: `${BASE_DATA_SOURCE_NAME}.${memberKeyToSafeKey(
      config.name
    )} = ${memberKeyToSafeKey(config.name)}.${config.joinColumn}`,
  })),
});

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
      return [`${BASE_DATA_SOURCE_NAME}.${memberKeyToSafeKey(dimension)}`];
    } else {
      return resolution.resolutionColumns.map(
        (col) => `${memberKeyToSafeKey(dimension)}.${col}`
      );
    }
  });
  return resolvedDimensions;
};

export const generateResolutionJoinPaths = (
  resolutionConfig: ResolutionConfig
): JoinPath[] => {
  return resolutionConfig.columnConfigs.map((config) => [
    {
      left: BASE_DATA_SOURCE_NAME,
      right: memberKeyToSafeKey(config.name),
      on: memberKeyToSafeKey(config.name),
    },
  ]);
};
