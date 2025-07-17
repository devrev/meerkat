import { getNamespacedKey, memberKeyToSafeKey } from '../member-formatters';
import { constructAlias } from '../member-formatters/get-alias';
import { JoinPath, Member, Query } from '../types/cube-types/query';
import { Dimension, Measure, TableSchema } from '../types/cube-types/table';
import {
  findInDimensionSchemas,
  findInSchemas,
} from '../utils/find-in-table-schema';
import { BASE_DATA_SOURCE_NAME, ResolutionConfig } from './types';

const constructBaseDimension = (name: string, schema: Measure | Dimension) => {
  return {
    name: memberKeyToSafeKey(name),
    sql: `${BASE_DATA_SOURCE_NAME}.${constructAlias(name, schema.alias, true)}`,
    type: schema.type,
    // Constructs alias to match the name in the base query.
    alias: constructAlias(name, schema.alias),
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
  dimensions: [...measures, ...(dimensions || [])].map((member) => {
    const schema = findInSchemas(member, tableSchemas);
    if (schema) {
      return constructBaseDimension(member, schema);
    } else {
      throw new Error(`Not found: ${member}`);
    }
  }),
  joins: resolutionConfig.columnConfigs.map((config) => ({
    sql: `${BASE_DATA_SOURCE_NAME}.${constructAlias(
      config.name,
      findInSchemas(config.name, tableSchemas)?.alias,
      true
    )} = ${memberKeyToSafeKey(config.name)}.${config.joinColumn}`,
  })),
});

export const generateResolutionSchemas = (
  config: ResolutionConfig,
  baseTableSchemas: TableSchema[]
) => {
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
      findInSchemas(colConfig.name, baseTableSchemas)?.alias
    );

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
    const columnConfig = config.columnConfigs.find((c) => c.name === dimension);

    if (!columnConfig) {
      return [
        getNamespacedKey(BASE_DATA_SOURCE_NAME, memberKeyToSafeKey(dimension)),
      ];
    } else {
      return columnConfig.resolutionColumns.map((col) =>
        getNamespacedKey(
          memberKeyToSafeKey(dimension),
          memberKeyToSafeKey(getNamespacedKey(columnConfig.name, col))
        )
      );
    }
  });
  return resolvedDimensions;
};

export const generateResolutionJoinPaths = (
  resolutionConfig: ResolutionConfig,
  baseTableSchemas: TableSchema[]
): JoinPath[] => {
  return resolutionConfig.columnConfigs.map((config) => [
    {
      left: BASE_DATA_SOURCE_NAME,
      right: memberKeyToSafeKey(config.name),
      on: constructAlias(
        config.name,
        findInSchemas(config.name, baseTableSchemas)?.alias,
        true
      ),
    },
  ]);
};
