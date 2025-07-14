import { memberKeyToSafeKey } from '../member-formatters';
import { constructAlias } from '../member-formatters/get-alias';
import { JoinPath, Member, Query } from '../types/cube-types/query';
import { Dimension, Measure, TableSchema } from '../types/cube-types/table';
import { BASE_DATA_SOURCE_NAME, ResolutionConfig } from './types';

export const constructDimensionsNameMap = (tableSchema: TableSchema[]) => {
  const columnNameMap: Record<string, Dimension> = {};
  tableSchema.forEach((table) => {
    table.dimensions.forEach((dimension) => {
      columnNameMap[`${table.name}.${dimension.name}`] = dimension;
    });
  });
  return columnNameMap;
};

export const constructMeasuresNameMap = (tableSchema: TableSchema[]) => {
  const columnNameMap: Record<string, Measure> = {};
  tableSchema.forEach((table) => {
    table.measures.forEach((measure) => {
      columnNameMap[`${table.name}.${measure.name}`] = measure;
    });
  });
  return columnNameMap;
};

const constructBaseDimension = (name: string, schema: Measure | Dimension) => {
  return {
    name: `${memberKeyToSafeKey(name)}`,
    sql: `${BASE_DATA_SOURCE_NAME}.${constructAlias(name, schema.alias, true)}`,
    type: schema.type,
    // Constructs alias to match the name in the base query.
    alias: constructAlias(name, schema.alias),
  };
};

export const createBaseTableSchema = (
  baseSql: string,
  columnsByName: Record<string, Measure | Dimension>,
  resolutionConfig: ResolutionConfig,
  measures: Member[],
  dimensions?: Member[]
) => ({
  name: BASE_DATA_SOURCE_NAME,
  sql: baseSql,
  measures: [],
  dimensions: [...measures, ...(dimensions || [])].map((member) => {
    const schema = columnsByName[member];
    if (schema) {
      return constructBaseDimension(member, schema);
    } else {
      throw new Error(`Not found: ${member}`);
    }
  }),
  joins: resolutionConfig.columnConfigs.map((config) => ({
    sql: `${BASE_DATA_SOURCE_NAME}.${constructAlias(
      config.name,
      columnsByName[config.name]?.alias,
      true
    )} = ${memberKeyToSafeKey(config.name)}.${config.joinColumn}`,
  })),
});

export const generateResolutionSchemas = (
  config: ResolutionConfig,
  baseColumnsByName: Record<string, Measure | Dimension>
) => {
  const resolutionColumnsByName = constructDimensionsNameMap(
    config.tableSchemas
  );

  const resolutionSchemas: TableSchema[] = [];
  config.columnConfigs.forEach((colConfig) => {
    const tableSchema = config.tableSchemas.find(
      (ts) => ts.name === colConfig.source
    );
    if (!tableSchema) {
      throw new Error(`Table schema not found for ${colConfig.source}`);
    }

    const baseName = memberKeyToSafeKey(colConfig.name);
    const baseAlias = constructAlias(
      colConfig.name,
      baseColumnsByName[colConfig.name]?.alias
    );

    // For each column that needs to be resolved, create a copy of the relevant table schema.
    // We use the name of the column in the base query as the table schema name
    // to avoid conflicts.
    const resolutionSchema: TableSchema = {
      name: baseName,
      sql: tableSchema.sql,
      measures: [],
      dimensions: colConfig.resolutionColumns.map((col) => {
        const dimension = resolutionColumnsByName[`${colConfig.source}.${col}`];
        if (!dimension) {
          throw new Error(`Dimension not found: ${col}`);
        }
        return {
          name: col,
          sql: `${baseName}.${col}`,
          type: dimension.type,
          alias: `${baseAlias} - ${constructAlias(col, dimension.alias)}`,
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
