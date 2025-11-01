import {
  constructAlias,
  getNamespacedKey,
  memberKeyToSafeKey,
} from '../member-formatters';
import { JoinPath, Member, Query } from '../types/cube-types/query';
import { Dimension, Measure, TableSchema } from '../types/cube-types/table';
import {
  findInDimensionSchemas,
  findInSchemas,
} from '../utils/find-in-table-schema';
import {
  BASE_DATA_SOURCE_NAME,
  ResolutionColumnConfig,
  ResolutionConfig,
} from './types';

const constructBaseDimension = (
  name: string,
  schema: Measure | Dimension,
  resolutionColumnConfigs: ResolutionColumnConfig[]
) => {
  const dimension: Dimension = {
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
  // const shouldFlattenField = resolutionColumnConfigs.some(
  //   (config) => config.name == name && config.isArrayType
  // );
  // if (shouldFlattenField) {
  //   dimension.modifier = {
  //     shouldFlattenArray: true,
  //   };
  // }
  return dimension;
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
        return constructBaseDimension(
          member,
          schema,
          resolutionConfig.columnConfigs
        );
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
    const baseAlias = constructAlias({
      name: colConfig.name,
      alias: findInSchemas(colConfig.name, baseTableSchemas)?.alias,
      aliasContext: { isTableSchemaAlias: true },
    });

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
          alias: `${baseAlias} - ${constructAlias({
            name: col,
            alias: dimension.alias,
            aliasContext: { isTableSchemaAlias: true },
          })}`,
        };
      }),
    };

    resolutionSchemas.push(resolutionSchema);
  });

  return resolutionSchemas;
};

export const generateResolvedDimensions = (
  baseDataSourceName: string,
  query: Query,
  config: ResolutionConfig,
  columnProjections?: string[]
): Member[] => {
  // If column projections are provided, use those.
  // Otherwise, use all measures and dimensions from the original query.
  const aggregatedDimensions = columnProjections
    ? columnProjections
    : [...query.measures, ...(query.dimensions || [])];

  const resolvedDimensions: Member[] = aggregatedDimensions.flatMap(
    (dimension) => {
      const columnConfig = config.columnConfigs.find(
        (c) => c.name === dimension
      );

      if (!columnConfig) {
        // TODO: See if this can be optimized
        // In a first level join right now, we are just passing the dimension without namespacing it.
        // But in a resolution level, we are already adding __unnest_query as the base name and using in
        // other places like join paths as it depends on it to work.
        // For generating join paths, we expect the table name to be exactly the name of memberToSafeKey(namespace, joinDimensionName)
        // Hence, at second level, we are namespacing it everywhere, and we need this condition.
        if (dimension.includes('.')) {
          return [dimension];
        } else {
          return [
            getNamespacedKey(baseDataSourceName, memberKeyToSafeKey(dimension)),
          ];
        }
        return [
          getNamespacedKey(baseDataSourceName, memberKeyToSafeKey(dimension)),
        ];
      } else {
        return columnConfig.resolutionColumns.map((col) =>
          getNamespacedKey(
            memberKeyToSafeKey(dimension),
            memberKeyToSafeKey(getNamespacedKey(columnConfig.name, col))
          )
        );
      }
    }
  );
  return resolvedDimensions;
};

export const generateResolutionJoinPaths = (
  baseDataSourceName: string,
  resolutionConfig: ResolutionConfig,
  baseTableSchemas: TableSchema[]
): JoinPath[] => {
  return resolutionConfig.columnConfigs.map((config) => [
    {
      left: baseDataSourceName,
      right: memberKeyToSafeKey(config.name),
      on: constructAlias({
        name: config.name,
        alias: findInSchemas(config.name, baseTableSchemas)?.alias,
        aliasContext: { isAstIdentifier: false },
      }),
    },
  ]);
};
