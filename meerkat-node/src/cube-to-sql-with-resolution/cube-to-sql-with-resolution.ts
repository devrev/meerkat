import {
  applySqlOverrides,
  BASE_DATA_SOURCE_NAME,
  ContextParams,
  getAggregatedSql as coreGetAggregatedSql,
  getResolvedTableSchema as coreGetResolvedTableSchema,
  getUnnestTableSchema as coreGetUnnestTableSchema,
  createBaseTableSchema,
  Dimension,
  generateRowNumberSql,
  getNamespacedKey,
  memberKeyToSafeKey,
  Query,
  ResolutionConfig,
  ROW_ID_DIMENSION_NAME,
  shouldSkipResolution,
  TableSchema,
  wrapWithRowIdOrderingAndExclusion,
} from '@devrev/meerkat-core';
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';

export interface CubeQueryToSQLWithResolutionParams {
  query: Query;
  tableSchemas: TableSchema[];
  resolutionConfig: ResolutionConfig;
  columnProjections?: string[];
  contextParams?: ContextParams;
}

export const cubeQueryToSQLWithResolution = async ({
  query,
  tableSchemas,
  resolutionConfig,
  columnProjections,
  contextParams,
}: CubeQueryToSQLWithResolutionParams) => {
  // Check if resolution should be skipped
  if (shouldSkipResolution(resolutionConfig, query, columnProjections)) {
    return await cubeQueryToSQL({
      query,
      tableSchemas,
      contextParams,
    });
  }

  // Resolution is needed - create alias-free tableSchemas for resolution pipeline
  const tableSchemasWithoutAliases: TableSchema[] = tableSchemas.map(
    (schema) => ({
      ...schema,
      dimensions: schema.dimensions.map((dim) => ({
        ...dim,
        alias: undefined, // Strip alias for resolution pipeline
      })),
      measures: schema.measures.map((measure) => ({
        ...measure,
        alias: undefined, // Strip alias
      })),
    })
  );

  const baseSql = await cubeQueryToSQL({
    query,
    tableSchemas: tableSchemasWithoutAliases,
    contextParams,
  });

  if (!columnProjections) {
    columnProjections = [...(query.dimensions || []), ...query.measures];
  }
  // This is to ensure that, only the column projection columns
  // are being resolved and other definitions are ignored.
  resolutionConfig.columnConfigs = resolutionConfig.columnConfigs.filter(
    (config) => {
      return columnProjections?.includes(config.name);
    }
  );

  const baseSchema: TableSchema = createBaseTableSchema(
    baseSql,
    tableSchemasWithoutAliases, // Use alias-free schemas
    resolutionConfig,
    [],
    columnProjections
  );

  // Transform field names in configs to match base table schema format
  // This needs to be done for both columnConfigs and sqlOverrideConfigs
  resolutionConfig.columnConfigs.forEach((config) => {
    config.name = memberKeyToSafeKey(config.name);
  });

  if (resolutionConfig.sqlOverrideConfigs) {
    resolutionConfig.sqlOverrideConfigs.forEach((config) => {
      config.fieldName = memberKeyToSafeKey(config.fieldName);
    });
  }

  // Apply SQL overrides to the base schema (with alias-free schema)
  // At this point, filters/sorts are baked into baseSql using original values
  // We can now override dimensions/measures in the base schema with custom SQL expressions for display
  const schemaWithOverrides = applySqlOverrides(baseSchema, resolutionConfig);

  const rowIdDimension: Dimension = {
    name: ROW_ID_DIMENSION_NAME,
    sql: generateRowNumberSql(
      query,
      schemaWithOverrides.dimensions,
      BASE_DATA_SOURCE_NAME
    ),
    type: 'number',
    alias: ROW_ID_DIMENSION_NAME,
  };
  schemaWithOverrides.dimensions.push(rowIdDimension);
  columnProjections.push(ROW_ID_DIMENSION_NAME);

  // Generate SQL with row_id and unnested arrays
  const unnestTableSchema = await coreGetUnnestTableSchema({
    baseTableSchema: schemaWithOverrides,
    resolutionConfig,
    contextParams,
    cubeQueryToSQL: async (params) => cubeQueryToSQL(params),
  });

  //  Apply resolution (join with lookup tables)
  const resolvedTableSchema = await coreGetResolvedTableSchema({
    baseTableSchema: unnestTableSchema,
    resolutionConfig,
    contextParams,
    columnProjections,
    cubeQueryToSQL: async (params) => cubeQueryToSQL(params),
  });

  // Re-aggregate to reverse the unnest
  const aggregatedTableSchema = await coreGetAggregatedSql({
    resolvedTableSchema,
    resolutionConfig,
    contextParams,
    cubeQueryToSQL: async (params) => cubeQueryToSQL(params),
  });

  // Restore aliases from original tableSchemas to get nice column names in final output
  // Create a map of datasource__fieldName -> alias from original schemas
  const aliasMap = new Map<string, string>();
  tableSchemas.forEach((schema) => {
    schema.dimensions.forEach((dim) => {
      if (dim.alias) {
        const safeKey = memberKeyToSafeKey(`${schema.name}.${dim.name}`);
        const columnConfig = resolutionConfig.columnConfigs?.find(
          (config) => config.name === safeKey
        );
        if (columnConfig) {
          if (columnConfig.resolutionColumns.length == 1) {
            aliasMap.set(
              `${safeKey}__${columnConfig.resolutionColumns[0]}`,
              dim.alias
            );
          } else {
            const sourceTableDataSource = resolutionConfig.tableSchemas.find(
              (tableSchema) => tableSchema.name === columnConfig.source
            );
            if (!sourceTableDataSource) {
              throw new Error(
                `Source table data source not found for ${columnConfig.source}`
              );
            }
            for (const resolutionColumn of columnConfig.resolutionColumns) {
              const sourceFieldAlias = sourceTableDataSource.dimensions.find(
                (dimension) => dimension.name === resolutionColumn
              )?.alias;
              if (!sourceFieldAlias) {
                throw new Error(
                  `Source field alias not found for ${resolutionColumn}`
                );
              }
              aliasMap.set(
                `${safeKey}__${resolutionColumn}`,
                `${dim.alias} - ${sourceFieldAlias}`
              );
            }
          }
        } else {
          aliasMap.set(safeKey, dim.alias);
        }
      }
    });
    schema.measures.forEach((measure) => {
      if (measure.alias) {
        const safeKey = memberKeyToSafeKey(`${schema.name}.${measure.name}`);
        const columnConfig = resolutionConfig.columnConfigs?.find(
          (config) => config.name === safeKey
        );
        if (columnConfig) {
          if (columnConfig.resolutionColumns.length == 1) {
            aliasMap.set(
              `${safeKey}__${columnConfig.resolutionColumns[0]}`,
              measure.alias
            );
          } else {
            const sourceTableDataSource = resolutionConfig.tableSchemas.find(
              (tableSchema) => tableSchema.name === columnConfig.source
            );
            if (!sourceTableDataSource) {
              throw new Error(
                `Source table data source not found for ${columnConfig.source}`
              );
            }
            for (const resolutionColumn of columnConfig.resolutionColumns) {
              const sourceFieldAlias = sourceTableDataSource.dimensions.find(
                (dimension) => dimension.name === resolutionColumn
              )?.alias;
              if (!sourceFieldAlias) {
                throw new Error(
                  `Source field alias not found for ${resolutionColumn}`
                );
              }
              aliasMap.set(
                `${safeKey}__${resolutionColumn}`,
                `${measure.alias} - ${sourceFieldAlias}`
              );
            }
          }
        } else {
          aliasMap.set(safeKey, measure.alias);
        }
      }
    });
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
  const finalSql = await cubeQueryToSQL({
    query: {
      dimensions: [
        ...schemaWithAliases.dimensions.map((d) =>
          getNamespacedKey(schemaWithAliases.name, d.name)
        ),
        ...schemaWithAliases.measures.map((m) =>
          getNamespacedKey(schemaWithAliases.name, m.name)
        ),
      ],
      measures: [],
    },
    tableSchemas: [schemaWithAliases],
    contextParams,
  });

  // Wrap with row_id ordering and exclusion
  const wrapWithRowIdOrderingAndExclusionSql =
    wrapWithRowIdOrderingAndExclusion(finalSql, ROW_ID_DIMENSION_NAME);
  return wrapWithRowIdOrderingAndExclusionSql;
};
